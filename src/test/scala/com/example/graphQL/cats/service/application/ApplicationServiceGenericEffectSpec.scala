package com.example.graphQL.cats.service.application

import cats.effect.Clock
import cats.effect.std.UUIDGen
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{ApplicationStatus, UserRole}
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.service.StateEffectFixtures.*
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.IdempotencyRequest
import java.util.UUID

/** EPS-04.1: the Application context runs in an effect that is not IO and offers only a monad, a clock and UUIDs. */
final class ApplicationServiceGenericEffectSpec extends munit.FunSuite {
  private def request(name: String) =
    IdempotencyRequest.fromCanonicalInput(UUID.fromString("00000000-0000-0000-0000-000000000008"), name)

  private val service = new ApplicationService[Program](
    Users,
    Jobs,
    Applications,
    OpenReadModel,
    Idempotent(Receipts, summon[Clock[Program]]),
    summon[Clock[Program]],
    summon[UUIDGen[Program]]
  )

  private val world = World(
    users = Map(candidateId -> candidate, recruiterId -> recruiter),
    jobs = Map(jobId -> openJob),
    uuids = List(applicationId.value, UUID.fromString("00000000-0000-0000-0000-00000000000a"))
  )

  test("EPS-04.1 a candidate applies to an open job in a non-IO effect and the first history event is stored") {
    val Right((after, submitted)) = service
      .submitApplication(request("submit"), ActorContext(candidateId, UserRole.Candidate), jobId)
      .value
      .run(world): @unchecked
    assertEquals(submitted.map(_.candidateId), Right(candidateId))
    assertEquals(after.applications.keySet, Set(applicationId))
    assertEquals(after.applicationEvents.map(_.newStatus), Vector(ApplicationStatus.Created))
    assertEquals(after.events.size, 1)
  }

  test("EPS-04.1 a recruiter cannot apply to a job in a non-IO effect and nothing is stored") {
    val Right((after, submitted)) = service
      .submitApplication(request("submit"), ActorContext(recruiterId, UserRole.Recruiter), jobId)
      .value
      .run(world): @unchecked
    assertEquals(submitted, Left(UseCaseError.Domain(DomainError.Forbidden)))
    assertEquals(after.applications, Map.empty)
  }

  test("EPS-04.1 the owning recruiter accepts a stored application and the status change is appended") {
    val stored = world.copy(
      applications = Map(applicationId -> createdApplication),
      uuids = List(UUID.fromString("00000000-0000-0000-0000-00000000000a"))
    )
    val Right((after, changed)) = service
      .changeStatus(
        request("accept"),
        ActorContext(recruiterId, UserRole.Recruiter),
        applicationId,
        ApplicationStatus.Accepted,
        None,
        None
      )
      .value
      .run(stored): @unchecked
    assertEquals(changed.map(_.status), Right(ApplicationStatus.Accepted))
    assertEquals(after.applicationEvents.map(_.newStatus), Vector(ApplicationStatus.Accepted))
  }

  test("EPS-04.1 repeating a submit with the same key in a non-IO effect creates the application once") {
    val actor = ActorContext(candidateId, UserRole.Candidate)
    val replaying = new ApplicationService[Program](
      Users,
      Jobs,
      Applications,
      OpenReadModel,
      Idempotent(RememberingReceipts, summon[Clock[Program]]),
      summon[Clock[Program]],
      summon[UUIDGen[Program]]
    )
    val Right((after, results)) = (for {
      first <- replaying.submitApplication(request("submit"), actor, jobId).value
      second <- replaying.submitApplication(request("submit"), actor, jobId).value
    } yield (first, second)).run(world): @unchecked
    assertEquals(results._1.map(_.id), Right(applicationId))
    assertEquals(results._2.map(_.id), Right(applicationId))
    assertEquals(after.applications.size, 1)
  }
}
