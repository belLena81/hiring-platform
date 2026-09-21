package com.example.graphQL.cats.runtime

import cats.effect.{IO, Ref, Resource}
import com.example.graphQL.cats.api.graphql.{HiringGraphQLServices, TestGraphQLSupport}
import com.example.graphQL.cats.api.http.HiringApiRoutes
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, Job, User, UserRole}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.service.{ActorContext, DatabaseProbe, Diagnostics, HealthService, HiringReadService, LogEvent, LogField, ProbeResult, ServiceFixtures}
import com.example.graphQL.cats.service.application.ApplicationService
import com.example.graphQL.cats.service.job.JobService
import io.circe.Json
import munit.CatsEffectSuite
import org.http4s.{Method, Request, Status, Uri}
import org.http4s.circe.*
import org.http4s.otel4s.middleware.trace.PerRequestFilter
import org.http4s.otel4s.middleware.trace.redact.{PathRedactor, QueryRedactor}
import org.http4s.otel4s.middleware.trace.server.ServerMiddleware
import org.http4s.syntax.literals.*
import org.typelevel.otel4s.trace.TracerProvider
import org.typelevel.ci.CIString
import org.typelevel.otel4s.oteljava.OtelJava
import scala.concurrent.duration.*

final class TracePropagationSpec extends CatsEffectSuite {
  private type DiagnosticRecord = (LogEvent, Option[String], Map[LogField, String])

  test("a served jobs resolver span is a child of its http request span") {
    OtelJava.autoConfigured[IO]().use { otel =>
      (for {
        tracer <- Resource.eval(otel.tracerProvider.get("hiring-platform-test"))
        middleware <- Resource.eval {
          given TracerProvider[IO] = otel.tracerProvider
          val redactor = new PathRedactor with QueryRedactor {
            def redactPath(path: Uri.Path): Uri.Path = path
            def redactQuery(query: org.http4s.Query): org.http4s.Query = org.http4s.Query.empty
          }
          val provider = org.http4s.otel4s.middleware.trace.server.ServerSpanDataProvider.openTelemetry(redactor)
          ServerMiddleware.builder[IO](provider)
            .withPerRequestReversePropagationFilter(PerRequestFilter.alwaysEnabled)
            .build
        }
      } yield (tracer, middleware)).use { case (tracer, middleware) =>
      for {
        records <- Ref.of[IO, Vector[DiagnosticRecord]](Vector.empty)
        usersRef <- Ref.of[IO, Map[UserId, User]](Map(
          ServiceFixtures.candidateId -> ServiceFixtures.candidate,
          ServiceFixtures.recruiterId -> ServiceFixtures.recruiter
        ))
        jobsRef <- Ref.of[IO, Map[JobId, Job]](Map(ServiceFixtures.jobId -> ServiceFixtures.openJob))
        applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](Map.empty)
        eventsRef <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
        createErrorRef <- Ref.of[IO, Option[com.example.graphQL.cats.repository.protocol.RepositoryError]](None)
        users = ServiceFixtures.InMemoryUsers(usersRef)
        jobs = ServiceFixtures.InMemoryJobs(jobsRef)
        applications = ServiceFixtures.InMemoryApplications(applicationsRef, eventsRef, createErrorRef)
        diagnostics = capture(records)
        services = HiringGraphQLServices(
          HiringReadService(users, jobs, applications),
          JobService(users, jobs),
          ApplicationService(users, jobs, applications),
          TestGraphQLSupport.cursorKey,
          TestGraphQLSupport.accountService
        )
        probe = new DatabaseProbe { def check: IO[ProbeResult] = IO.pure(ProbeResult.Ready) }
        captured <- TestGraphQLSupport.dependencies(
          hiring = services,
          authenticate = _ => IO.pure(Right(Some(ActorContext(ServiceFixtures.candidateId, UserRole.Candidate))))
        ).use { dependencies =>
          for {
            http <- new HiringApiRoutes(new HealthService(probe, diagnostics), diagnostics, dependencies, tracer)
              .httpApp(HiringApiRoutes.HttpConfig(16L, 5.seconds))
            response <- middleware.wrapHttpApp(http)(Request[IO](Method.POST, uri"/graphql").withEntity(Json.obj(
              "query" -> Json.fromString("{ jobs(first: 1) { edges { node { id } } } }")
            )))
            captured <- records.get
          } yield (response, captured)
        }
      } yield {
        val response = captured._1
        val records = captured._2
        val resolverSpan = span(captured, "graphql.field.jobs")

        assertEquals(response.status, Status.Ok)
        val traceparent = response.headers.get(CIString("traceparent")).map(_.head.value)
        assert(traceparent.exists(_.contains(resolverSpan(LogField.TraceId))))
        assert(!records.exists(_._3.get(LogField.SpanName).contains("http.request")))
        assert(!resolverSpan.keys.exists(_.key == "sequence"))
      }
    }
  }
  }

  private def capture(records: Ref[IO, Vector[DiagnosticRecord]]): Diagnostics = new Diagnostics {
    def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
      records.update(_ :+ ((event, requestId, fields)))
  }

  private def span(records: (org.http4s.Response[IO], Vector[DiagnosticRecord]), name: String): Map[LogField, String] =
    records._2.collectFirst {
      case (LogEvent.SpanSucceeded, _, fields) if fields.get(LogField.SpanName).contains(name) => fields
    }.getOrElse(fail(s"missing $name span"))
}
