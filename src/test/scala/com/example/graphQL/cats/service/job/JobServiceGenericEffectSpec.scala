package com.example.graphQL.cats.service.job

import cats.effect.Clock
import cats.effect.std.UUIDGen
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{JobStatus, Location, UserRole}
import cats.data.StateT
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, LogEvent, LogField, UseCaseError}
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.service.StateEffectFixtures.*
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.IdempotencyRequest
import com.example.graphQL.cats.service.search.EmbeddingWorkPublisher
import java.util.UUID

/** EPS-04.1: the Job context runs in an effect that is not IO and offers only error handling, a clock and UUIDs. */
final class JobServiceGenericEffectSpec extends munit.FunSuite {
  private val request =
    IdempotencyRequest.fromCanonicalInput(UUID.fromString("00000000-0000-0000-0000-000000000008"), "create")
  private val input = CreateJobInput(
    " New role ",
    " Build services ",
    List(" Scala "),
    Set(" Cats Effect "),
    Location("Cyprus", Some("Nicosia"), remote = true),
    JobStatus.Open
  )
  private val world = World(
    users = Map(candidateId -> candidate, recruiterId -> recruiter),
    uuids = List(jobId.value)
  )

  private val recordingDiagnostics = new Diagnostics[Program] {
    def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): Program[Unit] =
      StateT.modify(world => world.copy(logged = world.logged :+ event))
  }

  private def service(
      publisher: EmbeddingWorkPublisher[Program],
      diagnostics: Diagnostics[Program] = Diagnostics.noop[Program]
  ) =
    new JobService[Program](
      Users,
      Jobs,
      publisher,
      Idempotent(Receipts, summon[Clock[Program]]),
      diagnostics,
      summon[Clock[Program]],
      summon[UUIDGen[Program]]
    )

  test("EPS-04.1 a recruiter creates a job in a non-IO effect and the committed job wakes the embedding worker once") {
    val Right((after, created)) = service(countingPublisher)
      .createJob(request, ActorContext(recruiterId, UserRole.Recruiter), input)
      .value
      .run(world): @unchecked
    assertEquals(created.map(_.recruiterId), Right(recruiterId))
    assertEquals(created.map(_.title), Right("New role"))
    assertEquals(after.jobs.keySet, Set(jobId))
    assertEquals(after.events.size, 1)
    assertEquals(after.wakes, 1)
  }

  test("EPS-04.1 a candidate cannot create a job in a non-IO effect and nothing is stored") {
    val Right((after, created)) = service(countingPublisher)
      .createJob(request, ActorContext(candidateId, UserRole.Candidate), input)
      .value
      .run(world): @unchecked
    assertEquals(created, Left(UseCaseError.Domain(DomainError.Forbidden)))
    assertEquals(after.jobs, Map.empty)
    assertEquals(after.wakes, 0)
  }

  test("EPS-04.1 a failed wake never fails the committed job in a non-IO effect") {
    val Right((after, created)) = service(failingPublisher, recordingDiagnostics)
      .createJob(request, ActorContext(recruiterId, UserRole.Recruiter), input)
      .value
      .run(world): @unchecked
    assertEquals(created.map(_.id), Right(jobId))
    assertEquals(after.jobs.keySet, Set(jobId))
    assertEquals(after.logged, Vector(LogEvent.EmbeddingWakeFailed))
  }

  test("EPS-04.1 the owner closes a stored job and a view reads the closed state in a non-IO effect") {
    val stored = world.copy(jobs = Map(jobId -> openJob))
    val closeRequest =
      IdempotencyRequest.fromCanonicalInput(UUID.fromString("00000000-0000-0000-0000-000000000009"), "close")
    val Right((_, viewed)) = (for {
      closed <- service(countingPublisher).closeJob(closeRequest, ActorContext(recruiterId, UserRole.Recruiter), jobId)
      seen <- service(countingPublisher).viewJob(ActorContext(recruiterId, UserRole.Recruiter), jobId)
    } yield (closed.status, seen.status)).value.run(stored): @unchecked
    assertEquals(viewed, Right((JobStatus.Closed, JobStatus.Closed)))
  }
}
