package com.example.graphQL.cats.service.application

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, Job, User, UserRole}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.{ActorContext, SearchError, ServiceFixtures, UseCaseError}
import com.example.graphQL.cats.service.port.*
import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite
import scala.concurrent.duration.*

final class InterviewSchedulingServiceSpec extends CatsEffectSuite {
  import ServiceFixtures.*

  private val workflowId = InterviewWorkflowId(new UUID(0L, 20L))
  private val requestKey = new UUID(0L, 21L)
  private val accepted = createdApplication.copy(status = ApplicationStatus.Accepted)
  private val existing = InterviewWorkflow(
    workflowId,
    applicationId,
    candidateId,
    recruiterId,
    InterviewInterval(now.plusSeconds(600), now.plusSeconds(1200)),
    now.plusSeconds(300),
    requestKey,
    0L,
    InterviewWorkflowPhase.ReservationPending,
    Set.empty,
    recruiterId
  )

  private def service(
      workflows: InterviewWorkflowRepository,
      clock: IO[Instant],
      ids: IO[InterviewWorkflowId]
  ): IO[InterviewSchedulingService] =
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(candidateId -> candidate, recruiterId -> recruiter, adminId -> admin))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob))
      applications <- Ref.of[IO, Map[ApplicationId, Application]](Map(applicationId -> accepted))
      events <- Ref.of[IO, Vector[ApplicationEvent]](Vector.empty)
      errors <- Ref.of[IO, Option[RepositoryError]](None)
    } yield new InterviewSchedulingService(
      new InMemoryUsers(users),
      new InMemoryJobs(jobs),
      new InMemoryApplications(applications, events, errors),
      workflows,
      5.minutes,
      clock,
      ids
    )

  test("new requests evaluate injected clock and identity effects independently for each workflow") {
    for {
      clockReads <- Ref.of[IO, Int](0)
      identityReads <- Ref.of[IO, Int](0)
      writes <- Ref.of[IO, Vector[(InterviewWorkflow, Instant)]](Vector.empty)
      repository = new TestInterviewWorkflowRepository {
        override def findRequest(actor: UserId, key: UUID, fingerprint: MutationReceiptFingerprint) =
          RepositoryIO.fromEither(Right(Option.empty[InterviewWorkflow]))
        override def create(
            workflow: InterviewWorkflow,
            initialCommand: InterviewWorkflowCommand,
            key: UUID,
            fingerprint: MutationReceiptFingerprint,
            at: Instant
        ) =
          RepositoryIO.lift(writes.update(_ :+ (workflow, at))).as(InterviewWorkflowAdvanceResult.Applied)
      }
      scheduling <- service(
        repository,
        clockReads.getAndUpdate(_ + 1).map(read => now.plusSeconds(read.toLong)),
        identityReads.getAndUpdate(_ + 1).map(read => InterviewWorkflowId(new UUID(0L, 20L + read.toLong)))
      )
      first <- scheduling
        .schedule(
          ActorContext(recruiterId, UserRole.Recruiter),
          applicationId,
          now.plusSeconds(120),
          now.plusSeconds(240),
          requestKey
        )
        .value
      second <- scheduling
        .schedule(
          ActorContext(recruiterId, UserRole.Recruiter),
          applicationId,
          now.plusSeconds(600),
          now.plusSeconds(1200),
          new UUID(0L, 22L)
        )
        .value
      recorded <- writes.get
      clocks <- clockReads.get
      identities <- identityReads.get
    } yield {
      assertEquals(first.map(_.id), Right(workflowId))
      assertEquals(first.map(_.preCommitDeadline), Right(now.plusSeconds(120)))
      assertEquals(second.map(_.id), Right(InterviewWorkflowId(new UUID(0L, 21L))))
      assertEquals(second.map(_.preCommitDeadline), Right(now.plusSeconds(301)))
      assertEquals(recorded.map(_._2), Vector(now, now.plusSeconds(1)))
      assertEquals(clocks, 2)
      assertEquals(identities, 2)
    }
  }

  test("durable replay canonicalizes submillisecond interval inputs without reading time or generating identity") {
    val past = existing.copy(interval = InterviewInterval(now.minusSeconds(120), now.minusSeconds(60)))
    val expectedFingerprint = MutationReceiptFingerprint.fromCanonicalInput(
      s"${applicationId.value}|${past.interval.startsAt}|${past.interval.endsAt}"
    )
    val repository = new TestInterviewWorkflowRepository {
      override def findRequest(actor: UserId, key: UUID, fingerprint: MutationReceiptFingerprint) = {
        assertEquals(actor, recruiterId)
        assertEquals(key, requestKey)
        assertEquals(fingerprint, expectedFingerprint)
        RepositoryIO.fromEither(Right(Some(past)))
      }
    }
    for {
      scheduling <- service(
        repository,
        IO.raiseError(new AssertionError("Replay read the clock")),
        IO.raiseError(new AssertionError("Replay generated an identity"))
      )
      replay <- scheduling
        .schedule(
          ActorContext(recruiterId, UserRole.Recruiter),
          applicationId,
          past.interval.startsAt.plusNanos(123456),
          past.interval.endsAt.plusNanos(987654),
          requestKey
        )
        .value
    } yield assertEquals(replay, Right(past))
  }

  test("invalid interval observes supplied time but does not generate identity or persist intent") {
    val repository = new TestInterviewWorkflowRepository {
      override def findRequest(actor: UserId, key: UUID, fingerprint: MutationReceiptFingerprint) =
        RepositoryIO.fromEither(Right(Option.empty[InterviewWorkflow]))
    }
    for {
      reads <- Ref.of[IO, Int](0)
      scheduling <- service(
        repository,
        reads.update(_ + 1).as(now),
        IO.raiseError(new AssertionError("Invalid interval generated an identity"))
      )
      rejected <- scheduling
        .schedule(ActorContext(recruiterId, UserRole.Recruiter), applicationId, now, now.plusSeconds(60), requestKey)
        .value
      observed <- reads.get
    } yield {
      assertEquals(rejected, Left(UseCaseError.Search(SearchError.InvalidFilter("interviewInterval"))))
      assertEquals(observed, 1)
    }
  }

  test("Admin repair supplies injected time and trusted actor without generating a workflow identity") {
    for {
      repairs <- Ref.of[IO, Vector[(Instant, UserId)]](Vector.empty)
      repository = new TestInterviewWorkflowRepository {
        override def findForAdmin(id: InterviewWorkflowId) = RepositoryIO.fromEither(Right(Some(existing)))
        override def repair(workflow: InterviewWorkflow, revision: Long, key: UUID, at: Instant, actor: UserId) =
          RepositoryIO.lift(repairs.update(_ :+ (at, actor))).as(workflow)
      }
      scheduling <- service(
        repository,
        IO.pure(later),
        IO.raiseError(new AssertionError("Repair generated an identity"))
      )
      repaired <- scheduling.repair(ActorContext(adminId, UserRole.Admin), workflowId, 0L, requestKey).value
      recorded <- repairs.get
    } yield {
      assertEquals(repaired, Right(existing))
      assertEquals(recorded, Vector((later, adminId)))
    }
  }
}
