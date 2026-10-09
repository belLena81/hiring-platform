package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.ApplicationEventId
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.port.*
import com.mongodb.client.model.Filters
import java.util.UUID
import scala.concurrent.duration.*

/** DHW-23: exactly one transition leaves a revision, the loser gets a typed outcome with no provider effect, and a
  * repeated request returns the same workflow. All races run as parallel transactions on the real replica set.
  */
final class InterviewCancellationRaceIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 6.minutes

  import InterviewLifecycleScenario.{now, replacement}

  private type Outcome = Either[RepositoryError, InterviewLifecycleOutcome]

  private def scenario[A](use: InterviewLifecycleScenario => IO[A]): IO[A] =
    InterviewLifecycleScenario.arranged(mongoResource)(use)

  private def winners(outcomes: List[Outcome]): Int =
    outcomes.count(_.exists(_.isInstanceOf[InterviewLifecycleOutcome.Applied]))

  private def staleLosers(outcomes: List[Outcome]): Int =
    outcomes.count(_ == Right(InterviewLifecycleOutcome.Rejected(InterviewWorkflowError.StaleRevision)))

  private def cancelBy(s: InterviewLifecycleScenario, role: UserRole, expected: Long): IO[Outcome] = {
    val (initiator, user) = role match {
      case UserRole.Candidate => (InterviewCancellationInitiator.Candidate, s.workflow.candidateId)
      case _                  => (InterviewCancellationInitiator.Recruiter, s.workflow.recruiterId)
    }
    s.apply(InterviewLifecycleEvent.Cancel(initiator, now), s.actor(role, user, input = "cancel"), expected, now)
  }

  private def proposeBy(s: InterviewLifecycleScenario, expected: Long, interval: InterviewInterval): IO[Outcome] =
    s.apply(
      InterviewLifecycleEvent.Propose(
        interval.startsAt,
        interval.endsAt,
        s.workflow.recruiterId,
        now,
        InterviewProposalTtl.Default
      ),
      s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = s"propose-${interval.startsAt}"),
      expected,
      now
    )

  test("cancel against cancel: one wins, one history entry, one outbox event, one provider effect") {
    scenario { s =>
      for {
        outcomes <- List(cancelBy(s, UserRole.Candidate, 0L), cancelBy(s, UserRole.Recruiter, 0L)).parSequence
        end <- s.drive(List(s.worker()), _.phase == InterviewWorkflowPhase.Cancelled)
        history <- s.count(MongoCollections.ApplicationEvents)
        outbox <- s.count(MongoCollections.EventOutbox)
        confirmed <- s.faults.confirmedCancels.get
        rows <- s.commandRows
      } yield {
        assertEquals(winners(outcomes), 1, clue(outcomes))
        assertEquals(staleLosers(outcomes), 1, clue(outcomes))
        assertEquals((history, outbox), (1L, 1L))
        assertEquals(end._1.phase, InterviewWorkflowPhase.Cancelled)
        assertEquals(confirmed.size, 1)
        assertEquals(rows.count(_.command.isInstanceOf[InterviewLifecycleCommand.CancelCalendarSlot]), 1)
      }
    }
  }

  test("cancel, accept and withdraw race for an open proposal: exactly one leaves the revision") {
    val iterations = (1 to 4).toList
    iterations.traverse { _ =>
      scenario { s =>
        for {
          proposed <- s.applied(
            InterviewLifecycleEvent.Propose(
              replacement.startsAt,
              replacement.endsAt,
              s.workflow.recruiterId,
              now,
              InterviewProposalTtl.Default
            ),
            s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = "propose"),
            0L,
            now
          )
          expected = proposed.revision
          outcomes <- List(
            cancelBy(s, UserRole.Recruiter, expected),
            s.apply(
              InterviewLifecycleEvent.AcceptProposal(now),
              s.actor(UserRole.Candidate, s.workflow.candidateId, input = "accept"),
              expected,
              now
            ),
            s.apply(
              InterviewLifecycleEvent.WithdrawProposal(now),
              s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = "withdraw"),
              expected,
              now
            )
          ).parSequence
          after <- s.current
          // Run the winner's work to completion: only the winner's provider effects may ever happen.
          _ <- s.settle(List(s.worker()))
          holdCalls <- s.faults.holdCalls.get
          cancelCalls <- s.faults.cancelCalls.get
          confirmed <- s.faults.confirmedCancels.get
        } yield {
          assertEquals(winners(outcomes), 1, clue(outcomes))
          assertEquals(staleLosers(outcomes), 2, clue(outcomes))
          assertEquals(after.revision, expected + 1)
          assert(after.proposal.isEmpty)
          after.phase match {
            case InterviewWorkflowPhase.CancelPending =>
              assertEquals((holdCalls, confirmed), (Nil, List(InterviewWorkflow.cancellationKey(s.id, 0))))
            case InterviewWorkflowPhase.RescheduleHoldPending =>
              assertEquals(holdCalls, List(InterviewWorkflow.reservationKey(s.id, 1)))
              assertEquals(confirmed, List(InterviewWorkflow.cancellationKey(s.id, 0)))
            case InterviewWorkflowPhase.Completed => assertEquals((holdCalls, cancelCalls), (Nil, Nil))
            case other                            => fail(s"unexpected winner phase $other")
          }
        }
      }
    }.void
  }

  test("cancel against a generic hire: the application is never both Hired and cancelled") {
    (1 to 4).toList.traverse { _ =>
      scenario { s =>
        // The real status-change write of the application repository (a hire), not a raw update.
        val applications =
          MongoApplicationRepository.transactional(s.fixture.database, s.fixture.client, Diagnostics.noop)
        val hire: IO[Boolean] = for {
          document <- MongoRepositoryTestSupport.findOne(
            s.fixture.database,
            MongoCollections.Applications,
            Filters.eq(MongoFields.Id, s.workflow.applicationId.value.toString)
          )
          stored <- IO
            .fromOption(document)(new AssertionError("application missing"))
            .flatMap(found =>
              IO.fromEither(
                MongoHiringCodecs.readApplication(found).toEither.leftMap(errors => new AssertionError(errors.toString))
              )
            )
          event = ApplicationEvent(
            ApplicationEventId(UUID.randomUUID()),
            stored.id,
            Some(ApplicationStatus.Interview),
            ApplicationStatus.Hired,
            s.workflow.recruiterId,
            now,
            None,
            None
          )
          written <- applications
            .updateStatusWithEvents(stored.copy(status = ApplicationStatus.Hired, updatedAt = now), event, Nil)
            .value
        } yield written.isRight
        for {
          both <- (cancelBy(s, UserRole.Candidate, 0L), hire).parTupled
          (cancel, hired) = both
          status <- s.applicationStatus
          after <- s.current
          cancels <- s.commandRows.map(_.count(_.command.isInstanceOf[InterviewLifecycleCommand.CancelCalendarSlot]))
          history <- s.count(MongoCollections.ApplicationEvents)
        } yield
          if (hired) {
            assertEquals(status, Some("Hired"))
            assertEquals(after.phase, InterviewWorkflowPhase.Completed)
            // Only the hire's own history entry exists; the cancellation wrote nothing.
            assertEquals((cancels, history), (0, 1L))
            assert(
              cancel == Right(InterviewLifecycleOutcome.ApplicationNotInterview(ApplicationStatus.Hired)) ||
                cancel == Left(RepositoryError.Conflict),
              clue(cancel)
            )
          } else {
            assertEquals(status, Some("Rejected"))
            assertEquals(after.phase, InterviewWorkflowPhase.CancelPending)
            assertEquals((cancels, history), (1, 1L))
          }
      }
    }.void
  }

  test("accept against expiry: one wins by revision, the loser has no effect") {
    (1 to 3).toList.traverse { _ =>
      scenario { s =>
        for {
          proposed <- s.applied(
            InterviewLifecycleEvent.Propose(
              replacement.startsAt,
              replacement.endsAt,
              s.workflow.recruiterId,
              now,
              InterviewProposalTtl.Default
            ),
            s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = "propose"),
            0L,
            now
          )
          due = proposed.proposal.map(_.expiresAt).getOrElse(fail("no proposal"))
          outcomes <- List(
            s.apply(
              InterviewLifecycleEvent.AcceptProposal(due.minusMillis(1)),
              s.actor(UserRole.Candidate, s.workflow.candidateId, input = "accept"),
              proposed.revision,
              now
            ),
            s.apply(
              InterviewLifecycleEvent.ProposalExpired(due),
              InterviewLifecycleOrigin.Internal(InterviewAdvanceCause.ResultReceipt(s"expiry-${UUID.randomUUID()}")),
              proposed.revision,
              due
            )
          ).parSequence
          after <- s.current
          settled <- s.settle(List(s.worker()))
          holds <- s.faults.holdCalls.get
        } yield {
          assertEquals(winners(outcomes), 1, clue(outcomes))
          assertEquals(staleLosers(outcomes), 1, clue(outcomes))
          after.phase match {
            // Accept won: the replacement is held and swapped exactly once.
            case InterviewWorkflowPhase.RescheduleHoldPending =>
              assertEquals(holds, List(InterviewWorkflow.reservationKey(s.id, 1)))
              assertEquals(settled.generation, 1)
            // Expiry won: no provider call ever happens.
            case InterviewWorkflowPhase.Completed =>
              assertEquals(holds, Nil)
              assertEquals(settled.generation, 0)
            case other => fail(s"unexpected winner phase $other")
          }
        }
      }
    }.void
  }

  test("cancel against propose, and two proposals against each other: one wins each") {
    scenario { s =>
      for {
        cancelVsPropose <- List(cancelBy(s, UserRole.Candidate, 0L), proposeBy(s, 0L, replacement)).parSequence
        after <- s.current
      } yield {
        assertEquals(winners(cancelVsPropose), 1, clue(cancelVsPropose))
        assertEquals(staleLosers(cancelVsPropose), 1, clue(cancelVsPropose))
        assertEquals(after.revision, 1L)
      }
    } *> scenario { s =>
      val other = InterviewInterval(replacement.startsAt.plusSeconds(7200), replacement.endsAt.plusSeconds(7200))
      for {
        outcomes <- List(proposeBy(s, 0L, replacement), proposeBy(s, 0L, other)).parSequence
        third <- proposeBy(s, 1L, other.copy(startsAt = other.startsAt.plusSeconds(60)))
        after <- s.current
      } yield {
        assertEquals(winners(outcomes), 1, clue(outcomes))
        assertEquals(staleLosers(outcomes), 1, clue(outcomes))
        assertEquals(third, Right(InterviewLifecycleOutcome.Rejected(InterviewWorkflowError.ProposalAlreadyOpen)))
        assertEquals(after.phase, InterviewWorkflowPhase.ProposalPending)
      }
    }
  }

  test("the same request sent in parallel returns one workflow and writes one rejection") {
    scenario { s =>
      val key = UUID.randomUUID()
      val origin = s.actor(UserRole.Candidate, s.workflow.candidateId, key, "cancel")
      val request = s.apply(
        InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Candidate, now),
        origin,
        0L,
        now
      )
      for {
        outcomes <- List.fill(6)(request).parSequence
        history <- s.count(MongoCollections.ApplicationEvents)
        outbox <- s.count(MongoCollections.EventOutbox)
        rows <- s.commandRows
        later <- request
        stored <- s.current
      } yield {
        val workflows = outcomes.map {
          case Right(InterviewLifecycleOutcome.Applied(w))   => w
          case Right(InterviewLifecycleOutcome.Duplicate(w)) => w
          case other                                         => fail(s"unexpected $other")
        }
        assertEquals(workflows.distinct.size, 1)
        assertEquals(workflows.head, stored)
        assertEquals(later, Right(InterviewLifecycleOutcome.Duplicate(stored)))
        assertEquals((history, outbox), (1L, 1L))
        assertEquals(rows.count(_.command.isInstanceOf[InterviewLifecycleCommand.CancelCalendarSlot]), 1)
      }
    }
  }

  test("a key reused for another operation or input is a conflict that changes nothing") {
    scenario { s =>
      val key = UUID.randomUUID()
      for {
        first <- s.apply(
          InterviewLifecycleEvent.RequestReschedule(now),
          s.actor(UserRole.Candidate, s.workflow.candidateId, key, "request"),
          0L,
          now
        )
        sameKeyOtherOperation <- s.apply(
          InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Candidate, now),
          s.actor(UserRole.Candidate, s.workflow.candidateId, key, "cancel"),
          1L,
          now
        )
        status <- s.applicationStatus
        after <- s.current
      } yield {
        assert(first.exists(_.isInstanceOf[InterviewLifecycleOutcome.Applied]))
        assertEquals(sameKeyOtherOperation, Left(RepositoryError.Conflict))
        assertEquals(status, Some("Interview"))
        assertEquals(after.phase, InterviewWorkflowPhase.Completed)
        assertEquals(after.revision, 1L)
      }
    }
  }

  test("two workers pumping one cancellation produce a single provider effect and one notification per recipient") {
    scenario { s =>
      for {
        _ <- cancelBy(s, UserRole.Candidate, 0L)
        end <- s.drive(List(s.worker("worker-a"), s.worker("worker-b")), _.phase == InterviewWorkflowPhase.Cancelled)
        confirmed <- s.faults.confirmedCancels.get
        receipts <- s.receipts
        _ = assertEquals(end._1.phase, InterviewWorkflowPhase.Cancelled)
      } yield {
        assertEquals(confirmed.size, 1)
        assertEquals(receipts.size, 2)
      }
    }
  }
}
