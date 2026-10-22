package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.UserRole
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.application.InterviewSchedulingService
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, UseCaseError}
import com.example.graphQL.cats.service.RepositoryError
import com.mongodb.client.model.Filters

import java.util.UUID
import scala.concurrent.duration.*

/** MR-08 (AC-08.5): interview request idempotency is a `mutation_receipts` row written in the transaction of the
  * guarded write. A repeat replays, a changed input is a conflict, a refused request stores nothing, and the interview
  * store keeps no request receipt of its own.
  */
final class InterviewRequestIdempotencyIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes

  private def serviceOn(fixture: MongoAccessEvaluationSupport.Fixture, repository: MongoInterviewWorkflowRepository) = {
    val users = MongoUserRepository.transactional(
      fixture.database,
      fixture.client,
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop[IO]
    )
    val jobs = MongoJobRepository.transactional(
      fixture.database,
      fixture.client,
      MongoEmbeddingWorkEnqueuer.disabled,
      Diagnostics.noop[IO]
    )
    val applications = MongoApplicationRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
    new InterviewSchedulingService[IO](
      users,
      jobs,
      applications,
      repository,
      Idempotent(MongoMutationReceiptRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])),
      5.minutes
    )
  }

  private def receipts(fixture: MongoAccessEvaluationSupport.Fixture): IO[Long] =
    MongoRepositoryTestSupport.count(
      fixture.database,
      MongoCollections.MutationReceipts,
      Filters.eq(MongoFields.Operation, "interviewWorkflow")
    )

  private def requestReceiptDocuments(fixture: MongoAccessEvaluationSupport.Fixture): IO[Long] =
    MongoRepositoryTestSupport.count(
      fixture.database,
      MongoCollections.InterviewWorkflows,
      Filters.eq(MongoFields.DocumentType, "requestReceipt")
    )

  private def value[A](result: Either[UseCaseError, A]): A =
    result.fold(error => fail(s"unexpected failure: $error"), identity)

  test("AC-08.5 a cancel repeated with the same key replays its workflow and writes the status once") {
    InterviewLifecycleScenario.arranged(mongoResource) { s =>
      val service = serviceOn(s.fixture, s.repository)
      val candidate = ActorContext(s.workflow.candidateId, UserRole.Candidate)
      val key = UUID.randomUUID()
      for {
        first <- service.cancel(candidate, s.workflow.id, 0L, key).value
        again <- service.cancel(candidate, s.workflow.id, 0L, key).value
        history <- s.count(MongoCollections.ApplicationEvents)
        outbox <- s.count(MongoCollections.EventOutbox)
        stored <- receipts(s.fixture)
        legacy <- requestReceiptDocuments(s.fixture)
        _ <- ApplicationStatusInvariant.assertAgrees(s.fixture.database, s.workflow.applicationId)
      } yield {
        assertEquals(value(first).phase, InterviewWorkflowPhase.CancelPending)
        assertEquals(value(again).id, value(first).id)
        assertEquals((history, outbox), (1L, 1L))
        assertEquals(stored, 1L)
        assertEquals(legacy, 0L)
      }
    }
  }

  test("AC-08.5 the same cancel sent six times in parallel returns one workflow and stores one status change") {
    InterviewLifecycleScenario.arranged(mongoResource) { s =>
      val service = serviceOn(s.fixture, s.repository)
      val candidate = ActorContext(s.workflow.candidateId, UserRole.Candidate)
      val key = UUID.randomUUID()
      for {
        outcomes <- List.fill(6)(service.cancel(candidate, s.workflow.id, 0L, key).value).parSequence
        history <- s.count(MongoCollections.ApplicationEvents)
        outbox <- s.count(MongoCollections.EventOutbox)
        rows <- s.commandRows
        stored <- receipts(s.fixture)
        current <- s.current
        _ <- ApplicationStatusInvariant.assertAgrees(s.fixture.database, s.workflow.applicationId)
      } yield {
        assertEquals(outcomes.map(_.map(_.id)).distinct, List(Right(s.workflow.id)), clue(outcomes))
        assertEquals(current.phase, InterviewWorkflowPhase.CancelPending)
        assertEquals((history, outbox, stored), (1L, 1L, 1L))
        assertEquals(rows.count(_.command.isInstanceOf[InterviewLifecycleCommand.CancelCalendarSlot]), 1)
      }
    }
  }

  test("AC-08.5 a key reused for another operation is a conflict that changes nothing") {
    InterviewLifecycleScenario.arranged(mongoResource) { s =>
      val service = serviceOn(s.fixture, s.repository)
      val candidate = ActorContext(s.workflow.candidateId, UserRole.Candidate)
      val key = UUID.randomUUID()
      for {
        requested <- service.requestReschedule(candidate, s.workflow.id, 0L, key).value
        reused <- service.cancel(candidate, s.workflow.id, 1L, key).value
        status <- s.applicationStatus
        current <- s.current
        stored <- receipts(s.fixture)
      } yield {
        assertEquals(value(requested).rescheduleRequestedAt.isDefined, true)
        assertEquals(reused, Left(UseCaseError.Repository(RepositoryError.Conflict)))
        assertEquals(status, Some("Interview"))
        assertEquals(current.revision, 1L)
        assertEquals(stored, 1L)
      }
    }
  }

  test("AC-08.5 a refused request stores no receipt, so the same key can be used once the cause is gone") {
    InterviewLifecycleScenario.arranged(mongoResource) { s =>
      val service = serviceOn(s.fixture, s.repository)
      val candidate = ActorContext(s.workflow.candidateId, UserRole.Candidate)
      val stranger = ActorContext(UserId(UUID.randomUUID()), UserRole.Candidate)
      val key = UUID.randomUUID()
      for {
        stale <- service.cancel(candidate, s.workflow.id, 7L, key).value
        refused <- service.cancel(stranger, s.workflow.id, 0L, key).value
        none <- receipts(s.fixture)
        cancelled <- service.cancel(candidate, s.workflow.id, 0L, key).value
        one <- receipts(s.fixture)
      } yield {
        assert(stale.isLeft, clue(stale))
        assert(refused.isLeft, clue(refused))
        assertEquals(none, 0L)
        assertEquals(value(cancelled).phase, InterviewWorkflowPhase.CancelPending)
        assertEquals(one, 1L)
      }
    }
  }

  test("AC-08.5 scheduling the same interval six times in parallel with one key creates one workflow and one receipt") {
    mongoResource.use { fixture =>
      val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop[IO])
      val service = serviceOn(fixture, repository)
      for {
        at <- IO.realTimeInstant.map(_.truncatedTo(java.time.temporal.ChronoUnit.MILLIS))
        template <- IO.fromEither(
          InterviewWorkflow
            .create(
              InterviewWorkflowId(UUID.randomUUID()),
              ApplicationId(UUID.randomUUID()),
              UserId(UUID.randomUUID()),
              UserId(UUID.randomUUID()),
              InterviewInterval(at.plusSeconds(172800), at.plusSeconds(176400)),
              at.plusSeconds(300),
              UUID.randomUUID(),
              ApplicationStatus.Accepted,
              UserId(UUID.randomUUID()),
              com.example.graphQL.cats.domain.model.UserRole.Recruiter
            )
            .map(workflow => workflow.copy(initiatedBy = workflow.recruiterId))
            .leftMap(error => new AssertionError(s"invalid workflow $error"))
        )
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- InterviewSchedulingFixtures.seed(fixture.database, List(template), at)
        recruiter = ActorContext(template.recruiterId, UserRole.Recruiter)
        key = UUID.randomUUID()
        outcomes <- List
          .fill(6)(
            service
              .schedule(recruiter, template.applicationId, template.interval.startsAt, template.interval.endsAt, key)
              .value
          )
          .parSequence
        workflows <- MongoRepositoryTestSupport.count(
          fixture.database,
          MongoCollections.InterviewWorkflows,
          Filters.eq(MongoFields.DocumentType, "workflow")
        )
        stored <- receipts(fixture)
        legacy <- requestReceiptDocuments(fixture)
        changed <- service
          .schedule(
            recruiter,
            template.applicationId,
            template.interval.startsAt,
            template.interval.endsAt.plusSeconds(60),
            key
          )
          .value
      } yield {
        assertEquals(outcomes.map(_.map(_.id)).distinct.size, 1, clue(outcomes))
        assert(outcomes.forall(_.isRight), clue(outcomes))
        assertEquals((workflows, stored, legacy), (1L, 1L, 0L))
        assertEquals(changed, Left(UseCaseError.Repository(RepositoryError.Conflict)))
      }
    }
  }
}
