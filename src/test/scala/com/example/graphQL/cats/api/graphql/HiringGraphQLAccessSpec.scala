package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import cats.effect.Ref
import cats.syntax.all.*
import com.example.graphQL.cats.application.{ActorContext, ProbeResult}
import com.example.graphQL.cats.application.service.ServiceFixtures.{InMemoryApplications, InMemoryJobs, InMemoryUsers}
import com.example.graphQL.cats.application.service.{ApplicationService, JobService}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.*
import io.circe.Json
import munit.CatsEffectSuite

import java.time.Instant
import java.util.UUID

final class HiringGraphQLAccessSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-09-17T08:00:00Z")
  private val candidateId = UserId(UUID.fromString("10000000-0000-0000-0000-000000000001"))
  private val recruiterId = UserId(UUID.fromString("10000000-0000-0000-0000-000000000002"))
  private val jobId = JobId(UUID.fromString("10000000-0000-0000-0000-000000000003"))
  private val closedJobId = JobId(UUID.fromString("10000000-0000-0000-0000-000000000004"))
  private val applicationId = ApplicationId(UUID.fromString("10000000-0000-0000-0000-000000000005"))

  private val candidate = User(candidateId, "candidate@example.com", "Candidate", UserRole.Candidate, None, now)
  private val recruiter = User(recruiterId, "recruiter@example.com", "Recruiter", UserRole.Recruiter, None, now)
  private val openJob = job(jobId, JobStatus.Open)
  private val closedJob = job(closedJobId, JobStatus.Closed)
  private val application = Application.create(applicationId, candidateId, jobId, now)

  test("hiring mutation without ActorContext returns typed unauthorized payload") {
    val query =
      s"""mutation {
         |  submitApplication(input: { jobId: "${jobId.value}" }) {
         |    application { id }
         |    errors { code message }
         |  }
         |}""".stripMargin

    execute(query, None).map { json =>
      assertEquals(json.hcursor.downField("data").downField("submitApplication").downField("errors").downArray.get[String]("code"),
        Right("UNAUTHORIZED"))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("application connection rejects a job cursor") {
    val cursor = CursorCodec.encodeJob(com.example.graphQL.cats.application.port.JobCursor(now, jobId))
    val query =
      s"""query {
         |  myApplications(first: 10, after: "$cursor") {
         |    edges { node { id } }
         |    errors { code message }
         |  }
         |}""".stripMargin

    execute(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
      val applications = json.hcursor.downField("data").downField("myApplications")
      assertEquals(applications.downField("edges").focus.flatMap(_.asArray).map(_.size), Some(0))
      assertEquals(applications.downField("errors").downArray.get[String]("code"), Right("INVALID_CURSOR"))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("injected hiring context searches only open jobs and preserves nested recruiter batching boundary") {
    val query =
      """query {
        |  jobs(first: 10, city: "Kyiv", skills: ["Scala"]) {
        |    edges {
        |      node {
        |        id
        |        title
        |        recruiter { id name }
        |      }
        |    }
        |    errors { code }
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
      val edges = json.hcursor.downField("data").downField("jobs").downField("edges").focus.getOrElse(Json.Null)
      assertEquals(edges.asArray.map(_.size), Some(1))
      assertEquals(edges.hcursor.downArray.downField("node").get[String]("id"), Right(jobId.value.toString))
      assertEquals(edges.hcursor.downArray.downField("node").downField("recruiter").get[String]("id"), Right(recruiterId.value.toString))
      assertEquals(json.hcursor.downField("data").downField("jobs").downField("errors").focus.flatMap(_.asArray).map(_.size), Some(0))
    }
  }

  test("job search rejects malformed createdAfter instead of widening results") {
    val query =
      """query {
        |  jobs(first: 10, createdAfter: "not-an-instant") {
        |    edges { node { id } }
        |    errors { code message }
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
      val jobs = json.hcursor.downField("data").downField("jobs")
      assertEquals(jobs.downField("edges").focus.flatMap(_.asArray).map(_.size), Some(0))
      assertEquals(jobs.downField("errors").downArray.get[String]("code"), Right("INVALID_CREATED_AFTER"))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("injected candidate context lists only the candidate applications") {
    val query =
      """query {
        |  myApplications(first: 10) {
        |    edges {
        |      node {
        |        id
        |        status
        |        job { id title }
        |      }
        |    }
        |    errors { code }
        |  }
        |}""".stripMargin

    execute(query, Some(ActorContext(candidateId, UserRole.Candidate))).map { json =>
      val node = json.hcursor.downField("data").downField("myApplications").downField("edges").downArray.downField("node")
      assertEquals(node.get[String]("id"), Right(applicationId.value.toString))
      assertEquals(node.get[String]("status"), Right("Created"))
      assertEquals(node.downField("job").get[String]("id"), Right(jobId.value.toString))
    }
  }

  test("candidate application history access resolves stored actor before ownership") {
    val query =
      s"""query {
         |  applicationHistory(applicationId: "${applicationId.value}", first: 10) {
         |    edges { node { id } }
         |    errors { code message }
         |  }
         |}""".stripMargin

    executeWithUsers(query, Some(ActorContext(candidateId, UserRole.Candidate)), List(recruiter)).map { json =>
      val history = json.hcursor.downField("data").downField("applicationHistory")
      assertEquals(history.downField("edges").focus.flatMap(_.asArray).map(_.size), Some(0))
      assertEquals(history.downField("errors").downArray.get[String]("code"), Right("UNAUTHORIZED"))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("application rejection uses reject input object and returns typed payload errors") {
    val query =
      s"""mutation {
         |  rejectApplication(input: { applicationId: "${applicationId.value}", feedback: "Not enough Scala" }) {
         |    application { id status }
         |    errors { code message }
         |  }
         |}""".stripMargin

    execute(query, Some(ActorContext(recruiterId, UserRole.Recruiter))).map { json =>
      val payload = json.hcursor.downField("data").downField("rejectApplication")
      assertEquals(payload.downField("application").get[String]("id"), Right(applicationId.value.toString))
      assertEquals(payload.downField("application").get[String]("status"), Right("Rejected"))
      assertEquals(payload.downField("errors").focus.flatMap(_.asArray).map(_.size), Some(0))
      assert(!json.hcursor.downField("errors").succeeded)
    }
  }

  test("application rejection and decline validation failures stay in typed payloads") {
    val reject =
      s"""mutation {
         |  rejectApplication(input: { applicationId: "${applicationId.value}", feedback: " " }) {
         |    application { id }
         |    errors { code message }
         |  }
         |}""".stripMargin
    val decline =
      s"""mutation {
         |  declineApplication(input: { applicationId: "${applicationId.value}", reason: "" }) {
         |    application { id }
         |    errors { code message }
         |  }
         |}""".stripMargin

    (execute(reject, Some(ActorContext(recruiterId, UserRole.Recruiter))),
      execute(decline, Some(ActorContext(recruiterId, UserRole.Recruiter)))).mapN { (rejectJson, declineJson) =>
      val rejectPayload = rejectJson.hcursor.downField("data").downField("rejectApplication")
      val declinePayload = declineJson.hcursor.downField("data").downField("declineApplication")
      assertEquals(rejectPayload.downField("application").focus, Some(Json.Null))
      assertEquals(rejectPayload.downField("errors").downArray.get[String]("code"), Right("REJECTION_FEEDBACK_REQUIRED"))
      assert(!rejectJson.hcursor.downField("errors").succeeded)
      assertEquals(declinePayload.downField("application").focus, Some(Json.Null))
      assertEquals(declinePayload.downField("errors").downArray.get[String]("code"), Right("DECLINE_REASON_REQUIRED"))
      assert(!declineJson.hcursor.downField("errors").succeeded)
    }
  }

  private def execute(query: String, actor: Option[ActorContext]): IO[Json] = {
    executeWithUsers(query, actor, List(candidate, recruiter))
  }

  private def executeWithUsers(query: String, actor: Option[ActorContext], users: List[User]): IO[Json] = {
    for {
      usersRef <- Ref.of[IO, Map[UserId, User]](users.map(user => user.id -> user).toMap)
      jobsRef <- Ref.of[IO, Map[JobId, Job]](List(openJob, closedJob).map(job => job.id -> job).toMap)
      applicationsRef <- Ref.of[IO, Map[ApplicationId, Application]](Map(application.id -> application))
      eventsRef <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      nextCreateError <- Ref.of[IO, Option[com.example.graphQL.cats.application.port.RepositoryError]](None)
      users = InMemoryUsers(usersRef)
      jobs = InMemoryJobs(jobsRef)
      applications = InMemoryApplications(applicationsRef, eventsRef, nextCreateError)
      services = HiringGraphQLServices(users, jobs, applications, JobService[IO](users, jobs), ApplicationService[IO](users, jobs, applications))
      request <- IO.fromOption(GraphQLRequest.parseBody(Json.obj("query" -> Json.fromString(query)).noSpaces))(
        new IllegalArgumentException("Invalid GraphQL test request"))
      result <- RequestContext.resource(IO.pure(ProbeResult.Ready), actor, Some(services)).use(HiringGraphQLSchema.executeInContext(request, _))
    } yield result.fold(failure => fail(failure.toString), identity)
  }

  private def job(id: JobId, status: JobStatus): Job =
    Job(
      id,
      recruiterId,
      "Senior Scala Developer",
      "Build backend services",
      List("Scala"),
      Set("Scala", "Cats Effect"),
      Location("Ukraine", "Kyiv", remote = true),
      status,
      now,
      now
    )
}
