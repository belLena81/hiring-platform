package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.*
import java.time.Instant
import java.util.{Date, UUID}
import scala.concurrent.duration.*

/** DHW-20 to DHW-22 on the real replica set: a proposal never touches the slot, acceptance holds first and swaps once,
  * and every failed path leaves the old interval valid or visible repair.
  */
final class InterviewRescheduleWorkerIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  import InterviewLifecycleScenario.{interval, now, replacement}

  private def scenario[A](use: InterviewLifecycleScenario => IO[A]): IO[A] =
    InterviewLifecycleScenario.arranged(mongoResource)(use)

  private def propose(
      s: InterviewLifecycleScenario,
      expected: Long = 0L,
      at: Instant = now,
      ttl: InterviewProposalTtl = InterviewProposalTtl.Default
  ): IO[InterviewWorkflow] =
    s.applied(
      InterviewLifecycleEvent.Propose(replacement.startsAt, replacement.endsAt, s.workflow.recruiterId, at, ttl),
      s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = s"propose-$expected"),
      expected,
      at
    )

  private def accept(s: InterviewLifecycleScenario, expected: Long, at: Instant = now): IO[InterviewWorkflow] =
    s.applied(
      InterviewLifecycleEvent.AcceptProposal(at),
      s.actor(UserRole.Candidate, s.workflow.candidateId, input = s"accept-$expected"),
      expected,
      at
    )

  private def informational(receipts: List[InterviewNotificationReceipt], kind: InterviewNotificationKind) =
    receipts.filter(_.kind == kind).map(_.participant).toSet

  test("acceptance holds the replacement first, swaps once, cancels the old slot last and nobody is ever slotless") {
    scenario { s =>
      for {
        proposed <- propose(s)
        accepted <- accept(s, proposed.revision)
        end <- s.drive(List(s.worker()), w => w.phase == InterviewWorkflowPhase.Completed && w.generation == 1)
        _ <- pumpUntilReceipts(s, 3)
        oldHold <- s.reservation(0)
        newHold <- s.reservation(1)
        receipts <- s.receipts
        timeline <- s.faults.liveHolds.get
        confirmed <- s.faults.confirmedCancels.get
        status <- s.applicationStatus
      } yield {
        assertEquals(accepted.phase, InterviewWorkflowPhase.RescheduleHoldPending)
        assertEquals(end._1.interval, replacement)
        assertEquals(end._1.pendingInterval, None)
        assertEquals(end._1.proposal, None)
        assert(oldHold.exists(_.releasedAt.nonEmpty), "the old slot is cancelled")
        assert(newHold.exists(_.releasedAt.isEmpty), "the replacement stays held")
        assert(
          (oldHold.flatMap(_.releasedAt), newHold.map(_.reservedAt)).tupled.exists { case (released, reserved) =>
            !reserved.isAfter(released)
          },
          "the replacement was held before the old slot was released"
        )
        assertEquals(confirmed, List(InterviewWorkflow.cancellationKey(s.id, 0)), "the old slot is cancelled once")
        assert(timeline.nonEmpty && timeline.forall(_ >= 1), clue(timeline))
        assertEquals(
          informational(receipts, InterviewNotificationKind.Rescheduled),
          Set(InterviewParticipant.Candidate, InterviewParticipant.Recruiter)
        )
        assertEquals(
          informational(receipts, InterviewNotificationKind.RescheduleProposed),
          Set(InterviewParticipant.Candidate)
        )
        assertEquals(status, Some("Interview"))
      }
    }
  }

  test("a declined proposal touches no slot, makes no provider call and tells the recruiter") {
    scenario { s =>
      for {
        proposed <- propose(s)
        _ <- s.applied(
          InterviewLifecycleEvent.DeclineProposal(now),
          s.actor(UserRole.Candidate, s.workflow.candidateId, input = "decline"),
          proposed.revision,
          now
        )
        end <- s.drive(List(s.worker()), _ => true)
        // The informational notifications are plain due work: pump until both have been delivered.
        delivered <- IO.race(IO.sleep(20.seconds), pumpUntilReceipts(s, 2)).map(_.isRight)
        receipts <- s.receipts
        holds <- s.faults.holdCalls.get
        cancels <- s.faults.cancelCalls.get
        held <- s.isHeld(0)
        gen1 <- s.reservation(1)
      } yield {
        assertEquals(end._1.phase, InterviewWorkflowPhase.Completed)
        assert(delivered)
        assertEquals(
          informational(receipts, InterviewNotificationKind.RescheduleDeclined),
          Set(InterviewParticipant.Recruiter)
        )
        assertEquals((holds, cancels), (Nil, Nil))
        assert(held)
        assertEquals(gen1, None)
      }
    }
  }

  private def pumpUntilReceipts(s: InterviewLifecycleScenario, expected: Int): IO[Unit] = {
    val worker = s.worker("receipt-pump")
    def loop: IO[Unit] =
      s.round(worker) *> s.receipts.flatMap(r => if (r.size >= expected) IO.unit else IO.sleep(25.millis) *> loop)
    loop
  }

  test("a replacement that conflicts with another booking keeps the old interval and tells both participants") {
    scenario { s =>
      for {
        proposed <- propose(s)
        // Someone else already holds the candidate during the replacement interval.
        _ <- MongoRepositoryTestSupport.insertOne(
          s.fixture.database,
          MongoCollections.InterviewCalendarReservations,
          new org.bson.Document(MongoFields.Id, UUID.randomUUID().toString)
            .append("workflowId", UUID.randomUUID().toString)
            .append("reserveKey", UUID.randomUUID().toString)
            .append("releaseKey", UUID.randomUUID().toString)
            .append("candidateId", s.workflow.candidateId.value.toString)
            .append("recruiterId", UUID.randomUUID().toString)
            .append("participants", java.util.List.of(s.workflow.candidateId.value.toString))
            .append("startsAt", Date.from(replacement.startsAt.plusSeconds(60)))
            .append("endsAt", Date.from(replacement.endsAt))
            .append("reservedAt", Date.from(now))
        )
        _ <- accept(s, proposed.revision)
        end <- s.drive(List(s.worker()), _.phase == InterviewWorkflowPhase.Completed)
        _ <- pumpUntilReceipts(s, 2)
        receipts <- s.receipts
        held <- s.isHeld(0)
        gen1 <- s.reservation(1)
      } yield {
        assertEquals(end._1.interval, interval)
        assertEquals(end._1.generation, 0)
        assertEquals(end._1.pendingInterval, None)
        assert(held)
        assertEquals(gen1, None)
        assertEquals(
          informational(receipts, InterviewNotificationKind.RescheduleUnavailable),
          Set(InterviewParticipant.Candidate, InterviewParticipant.Recruiter)
        )
      }
    }
  }

  test("a swap the guard refuses cancels the unneeded replacement and leaves the old interval valid") {
    scenario { s =>
      for {
        proposed <- propose(s)
        _ <- accept(s, proposed.revision)
        // The application was hired between acceptance and the swap.
        _ <- s.setApplicationStatus(ApplicationStatus.Hired)
        end <- s.drive(List(s.worker()), _.phase == InterviewWorkflowPhase.Completed)
        _ <- pumpUntilReceipts(s, 2)
        receipts <- s.receipts
        oldHeld <- s.isHeld(0)
        newReleased <- s.isReleased(1)
        confirmed <- s.faults.confirmedCancels.get
      } yield {
        assertEquals(end._1.interval, interval)
        assertEquals(end._1.generation, 0)
        assert(oldHeld, "the old slot was never given up")
        assert(newReleased, "the unneeded replacement was cancelled with provider confirmation")
        assertEquals(confirmed, List(InterviewWorkflow.cancellationKey(s.id, 1)))
        assertEquals(
          informational(receipts, InterviewNotificationKind.RescheduleUnavailable),
          Set(InterviewParticipant.Candidate, InterviewParticipant.Recruiter)
        )
      }
    }
  }

  test("a compensation that cannot be confirmed is visible repair and both holds stay listed") {
    scenario { s =>
      for {
        proposed <- propose(s)
        _ <- accept(s, proposed.revision)
        _ <- s.setApplicationStatus(ApplicationStatus.Hired)
        _ <- s.faults.cancelUnavailable.set(1000)
        end <- s.drive(List(s.worker(maxAttempts = 3)), _.phase == InterviewWorkflowPhase.RepairRequired)
        oldHeld <- s.isHeld(0)
        newHeld <- s.isHeld(1)
      } yield {
        assertEquals(end._1.repairOrigin, Some(InterviewWorkflowPhase.RescheduleCompensationPending))
        assert(oldHeld && newHeld, "both holds are still there for Admin to see")
        assertEquals(end._1.interval, interval)
        assertEquals(end._1.pendingInterval, Some(replacement))
      }
    }
  }

  test("an unanswered proposal expires after a worker restart, tells both, and accepting at expiry is refused") {
    scenario { s =>
      for {
        real <- IO.realTimeInstant.map(_.truncatedTo(java.time.temporal.ChronoUnit.MILLIS))
        // Proposed "an hour minus two seconds ago" with the minimum lifetime: it falls due two seconds from now.
        ttl = InterviewProposalTtl.from(InterviewProposalTtl.Min).fold(e => fail(s"$e"), identity)
        proposed <- propose(s, at = real.minusSeconds(3598), ttl = ttl)
        dueAt = proposed.proposal.map(_.expiresAt).getOrElse(fail("no proposal"))
        early <- s.drive(List(s.worker("before-restart")), _ => true, rounds = 0)
        stillOpen <- s.current
        // The proposal cannot be accepted at or after its expiry, even though the expiry command has not run.
        late <- s.apply(
          InterviewLifecycleEvent.AcceptProposal(dueAt),
          s.actor(UserRole.Candidate, s.workflow.candidateId, input = "late-accept"),
          proposed.revision,
          dueAt
        )
        // The first worker is gone; a new one finds the durable due work.
        end <- s.drive(List(s.worker("after-restart")), _.phase == InterviewWorkflowPhase.Completed, rounds = 600)
        _ <- pumpUntilReceipts(s, 3)
        receipts <- s.receipts
        holds <- s.faults.holdCalls.get
        cancels <- s.faults.cancelCalls.get
        held <- s.isHeld(0)
      } yield {
        assertEquals(early._1.phase, InterviewWorkflowPhase.ProposalPending)
        assertEquals(stillOpen.phase, InterviewWorkflowPhase.ProposalPending)
        assertEquals(late, Right(InterviewLifecycleOutcome.Rejected(InterviewWorkflowError.ProposalExpired)))
        assertEquals(end._1.proposal, None)
        assertEquals(
          informational(receipts, InterviewNotificationKind.RescheduleExpired),
          Set(InterviewParticipant.Candidate, InterviewParticipant.Recruiter)
        )
        assertEquals((holds, cancels), (Nil, Nil))
        assert(held)
      }
    }
  }

  test("after a compensated attempt the next proposal is accepted under a fresh generation and succeeds") {
    scenario { s =>
      for {
        first <- propose(s)
        _ <- accept(s, first.revision)
        // The application is hired meanwhile, so the guarded swap refuses and the replacement hold is compensated.
        _ <- s.setApplicationStatus(ApplicationStatus.Hired)
        compensated <- s.drive(List(s.worker()), _.phase == InterviewWorkflowPhase.Completed)
        // The candidate is still interviewing after all (the hire is withdrawn), and the recruiter tries again.
        _ <- s.setApplicationStatus(ApplicationStatus.Interview)
        second <- propose(s, expected = compensated._1.revision)
        _ <- accept(s, second.revision)
        end <- s.drive(List(s.worker()), w => w.phase == InterviewWorkflowPhase.Completed && w.generation == 2)
        _ <- pumpUntilReceipts(s, 6)
        first1 <- s.reservation(1)
        second2 <- s.reservation(2)
        oldHold <- s.reservation(0)
        confirmed <- s.faults.confirmedCancels.get
      } yield {
        assertEquals(compensated._1.skippedGenerations, 1)
        assertEquals(compensated._1.generation, 0)
        assertEquals(end._1.interval, replacement)
        assertEquals(end._1.generation, 2)
        assertEquals(end._1.skippedGenerations, 0)
        assert(first1.exists(_.releasedAt.nonEmpty), "the compensated hold stays cancelled")
        assert(second2.exists(_.releasedAt.isEmpty), "the second attempt holds under its own key")
        assert(oldHold.exists(_.releasedAt.nonEmpty))
        assertEquals(
          confirmed,
          List(InterviewWorkflow.cancellationKey(s.id, 1), InterviewWorkflow.cancellationKey(s.id, 0))
        )
      }
    }
  }

  test("a replacement that starts before the old interval makes its own start the swap deadline") {
    scenario { s =>
      val soon = InterviewInterval(now.plusSeconds(3600), now.plusSeconds(7200))
      for {
        proposed <- s.applied(
          InterviewLifecycleEvent
            .Propose(soon.startsAt, soon.endsAt, s.workflow.recruiterId, now, InterviewProposalTtl.Default),
          s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = "propose-soon"),
          0L,
          now
        )
        _ <- accept(s, proposed.revision)
        // The worker's clock is past the replacement's start but well before the old interval's start.
        late = s.worker("late-worker", clock = IO.pure(soon.startsAt.plusSeconds(1)))
        end <- s.drive(List(late), _.phase == InterviewWorkflowPhase.Completed)
        oldHeld <- s.isHeld(0)
        replacementReleased <- s.isReleased(1)
      } yield {
        assertEquals(end._1.interval, interval)
        assertEquals(end._1.generation, 0)
        assert(oldHeld)
        assert(replacementReleased, "the unneeded replacement was cancelled")
      }
    }
  }

  test("informational notifications are delivered or visibly repairable even after the workflow entered repair") {
    scenario { s =>
      for {
        proposed <- propose(s)
        withdrawn <- s.applied(
          InterviewLifecycleEvent.WithdrawProposal(now),
          s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = "withdraw"),
          proposed.revision,
          now
        )
        cancelling <- s.applied(
          InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Recruiter, now),
          s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = "cancel"),
          withdrawn.revision,
          now
        )
        // The provider cancel gave up before any notification was sent: the workflow is in repair.
        failed <- s.applied(
          InterviewLifecycleEvent.RetryExhausted("provider"),
          InterviewLifecycleOrigin.Internal(InterviewAdvanceCause.ResultReceipt(s"exhausted-${UUID.randomUUID()}")),
          cancelling.revision,
          now
        )
        // And the notification provider is down too.
        _ <- s.faults.notifyUnavailable.set(1000)
        worker = s.worker(maxAttempts = 3)
        _ <- s.pumpUntil(List(worker), success(s.repository.findNotificationRepairs(s.id)).map(_.size == 2))
        stillRepair <- s.current
        _ <- s.faults.notifyUnavailable.set(0)
        requeued <- success(s.repository.repairNotifications(s.id, UUID.randomUUID(), now, UserId(UUID.randomUUID())))
        _ <- s.pumpUntil(List(worker), s.receipts.map(_.size == 2))
        receipts <- s.receipts
        remaining <- success(s.repository.findNotificationRepairs(s.id))
      } yield {
        assertEquals(failed.phase, InterviewWorkflowPhase.RepairRequired)
        assertEquals(stillRepair.phase, InterviewWorkflowPhase.RepairRequired)
        assertEquals(requeued, 2)
        assertEquals(
          receipts.map(_.kind).toSet,
          Set(InterviewNotificationKind.RescheduleProposed, InterviewNotificationKind.RescheduleWithdrawn)
        )
        assertEquals(remaining, Nil)
      }
    }
  }

  test("informational notifications issued before the workflow entered repair are still delivered on their own") {
    scenario { s =>
      for {
        proposed <- propose(s)
        withdrawn <- s.applied(
          InterviewLifecycleEvent.WithdrawProposal(now),
          s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = "withdraw"),
          proposed.revision,
          now
        )
        cancelling <- s.applied(
          InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Recruiter, now),
          s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = "cancel"),
          withdrawn.revision,
          now
        )
        failed <- s.applied(
          InterviewLifecycleEvent.RetryExhausted("provider"),
          InterviewLifecycleOrigin.Internal(InterviewAdvanceCause.ResultReceipt(s"exhausted-${UUID.randomUUID()}")),
          cancelling.revision,
          now
        )
        _ <- s.pumpUntil(List(s.worker()), s.receipts.map(_.size == 2))
        after <- s.current
        receipts <- s.receipts
        repairs <- success(s.repository.findNotificationRepairs(s.id))
      } yield {
        assertEquals(failed.phase, InterviewWorkflowPhase.RepairRequired)
        assertEquals(after.phase, InterviewWorkflowPhase.RepairRequired)
        assertEquals(receipts.size, 2)
        assertEquals(repairs, Nil)
      }
    }
  }

  private def success[A](operation: RepositoryIO[A]): IO[A] =
    operation.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"$error")), IO.pure))
}
