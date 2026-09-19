package com.example.graphQL.cats.runtime

import cats.effect.{IO, IOLocal, Ref}
import com.example.graphQL.cats.api.graphql.{HiringGraphQLServices, TestGraphQLSupport}
import com.example.graphQL.cats.api.http.{Admission, HiringApiRoutes}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, Job, User, UserRole}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.service.{ActorContext, DatabaseProbe, Diagnostics, HealthService, HiringReadService, LogEvent, LogField, ProbeResult, ServiceFixtures, TraceContext}
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.job.JobService
import io.circe.Json
import munit.CatsEffectSuite
import org.http4s.{Method, Request, Status}
import org.http4s.circe.*
import org.http4s.syntax.literals.*

final class TracePropagationSpec extends CatsEffectSuite {
  private type DiagnosticRecord = (LogEvent, Option[String], Map[LogField, String])

  test("a served jobs resolver span is a child of its http request span") {
    for {
      records <- Ref.of[IO, Vector[DiagnosticRecord]](Vector.empty)
      traceLocal <- IOLocal[Option[TraceContext]](None)
      usersRef <- Ref.of[IO, Map[UserId, User]](Map(
        ServiceFixtures.candidateId -> ServiceFixtures.candidate,
        ServiceFixtures.recruiterId -> ServiceFixtures.recruiter
      ))
      jobsRef <- Ref.of[IO, Map[JobId, Job]](Map(ServiceFixtures.jobId -> ServiceFixtures.openJob))
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](Map.empty)
      eventsRef <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      createErrorRef <- Ref.of[IO, Option[com.example.graphQL.cats.service.RepositoryError]](None)
      admission <- Admission.create(1)
      users = ServiceFixtures.InMemoryUsers(usersRef)
      jobs = ServiceFixtures.InMemoryJobs(jobsRef)
      applications = ServiceFixtures.InMemoryApplications(applicationsRef, eventsRef, createErrorRef)
      diagnostics = capture(records)
      services = HiringGraphQLServices(
        TracedHiringServices.readModel(HiringReadService[IO](users, jobs, applications), diagnostics, traceLocal),
        TracedHiringServices.jobs(JobService[IO](users, jobs), diagnostics, traceLocal),
        TracedHiringServices.applications(ApplicationService[IO](users, jobs, applications), diagnostics, traceLocal),
        TestGraphQLSupport.cursorCodec,
        TestGraphQLSupport.accountService,
        traceLocal = Some(traceLocal)
      )
      probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
      captured <- TestGraphQLSupport.dependencies(
        hiring = services,
        authenticate = _ => IO.pure(Right(Some(ActorContext(ServiceFixtures.candidateId, UserRole.Candidate))))
      ).use { dependencies =>
        val http = new HiringApiRoutes(new HealthService(probe, diagnostics), diagnostics, admission, dependencies).app
        http(Request[IO](Method.POST, uri"/graphql").withEntity(Json.obj(
          "query" -> Json.fromString("{ jobs(first: 1) { edges { node { id } } } }")
        ))).flatMap(response => IO(assertEquals(response.status, Status.Ok)) *> records.get)
      }
    } yield {
      val httpSpan = span(captured, "http.request")
      val resolverSpan = span(captured, "service.job.searchOpen")

      assertEquals(resolverSpan(LogField.TraceId), httpSpan(LogField.TraceId))
      assertEquals(resolverSpan.get(LogField.ParentSpanId), Some(httpSpan(LogField.SpanId)))
    }
  }

  private def capture(records: Ref[IO, Vector[DiagnosticRecord]]): Diagnostics = new Diagnostics {
    def event(event: LogEvent, requestId: Option[String], fields: Map[LogField, String]): IO[Unit] =
      records.update(_ :+ ((event, requestId, fields)))
  }

  private def span(records: Vector[DiagnosticRecord], name: String): Map[LogField, String] =
    records.collectFirst {
      case (LogEvent.SpanStarted, _, fields) if fields.get(LogField.SpanName).contains(name) => fields
    }.getOrElse(fail(s"missing $name span"))
}
