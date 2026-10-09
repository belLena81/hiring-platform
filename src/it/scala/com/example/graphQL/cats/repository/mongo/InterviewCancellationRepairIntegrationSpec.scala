package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.port.*
import com.mongodb.client.model.Filters
import java.util.UUID
import scala.concurrent.duration.*

/** DHW-29: Admin repair of every cancel and reschedule phase resumes the failed step by lookup, resets its retry
  * budget, records the actor and never reopens a cancelled slot. Informational notifications repair on their own.
  */
final class InterviewCancellationRepairIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 6.minutes

  import InterviewLifecycleScenario.{now, replacement}

  private def scenario[A](use: InterviewLifecycleScenario => IO[A]): IO[A] =
    InterviewLifecycleScenario.arranged(mongoResource)(use)

  private val admin = UserId(UUID.randomUUID())

  private def success[A](operation: RepositoryIO[A]): IO[A] =
    operation.value.flatMap(_.fold(error => IO.raiseError(new AssertionError(s"$error")), IO.pure))

  private def cancel(s: InterviewLifecycleScenario): IO[InterviewWorkflow] =
    s.applied(
      InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Candidate, now),
      s.actor(UserRole.Candidate, s.workflow.candidateId, input = "cancel"),
      0L,
      now
    )

  private def propose(s: InterviewLifecycleScenario, expected: Long): IO[InterviewWorkflow] =
    s.applied(
      InterviewLifecycleEvent.Propose(
        replacement.startsAt,
        replacement.endsAt,
        s.workflow.recruiterId,
        now,
        InterviewProposalTtl.Default
      ),
      s.actor(UserRole.Recruiter, s.workflow.recruiterId, input = s"propose-$expected"),
      expected,
      now
    )

  private def accept(s: InterviewLifecycleScenario, expected: Long): IO[InterviewWorkflow] =
    s.applied(
      InterviewLifecycleEvent.AcceptProposal(now),
      s.actor(UserRole.Candidate, s.workflow.candidateId, input = s"accept-$expected"),
      expected,
      now
    )

  private def internal(s: InterviewLifecycleScenario, event: InterviewLifecycleEvent, w: InterviewWorkflow) =
    s.applied(
      event,
      InterviewLifecycleOrigin.Internal(InterviewAdvanceCause.ResultReceipt(s"test-${UUID.randomUUID()}")),
      w.revision,
      now
    )

  private def exhaust(s: InterviewLifecycleScenario, w: InterviewWorkflow) =
    internal(s, InterviewLifecycleEvent.RetryExhausted("test"), w)

  private def repair(s: InterviewLifecycleScenario, w: InterviewWorkflow, key: UUID = UUID.randomUUID()) =
    success(s.repository.repair(w, w.revision, key, now, admin))

  test("a cancellation whose provider kept failing resumes by lookup after repair, with a fresh budget and an audit") {
    scenario { s =>
      val key = UUID.randomUUID()
      for {
        _ <- cancel(s)
        _ <- s.faults.cancelUnavailable.set(1000)
        failed <- s.drive(List(s.worker(maxAttempts = 3)), _.phase == InterviewWorkflowPhase.RepairRequired)
        _ <- s.faults.cancelUnavailable.set(0)
        repaired <- repair(s, failed._1, key)
        again <- repair(s, failed._1, key)
        stale <- s.repository.repair(failed._1, failed._1.revision, UUID.randomUUID(), now, admin).value
        end <- s.drive(List(s.worker(maxAttempts = 3)), _.phase == InterviewWorkflowPhase.Cancelled)
        audit <- s
          .collection(MongoCollections.InterviewWorkflowInbox)
          .flatMap(
            _.find(Filters.eq(MongoFields.Id, s"${s.id.value}:repair:$key")).first
          )
        confirmed <- s.faults.confirmedCancels.get
        status <- s.applicationStatus
      } yield {
        assertEquals(failed._1.repairOrigin, Some(InterviewWorkflowPhase.CancelPending))
        assertEquals(repaired.phase, InterviewWorkflowPhase.CancelPending)
        assertEquals(repaired.repairOrigin, None)
        assertEquals(again, repaired, "repeating the repair request returns the same workflow")
        assertEquals(stale, Left(RepositoryError.Conflict))
        assertEquals(end._1.phase, InterviewWorkflowPhase.Cancelled)
        assertEquals(audit.map(_.getString("auditActorId")), Some(admin.value.toString))
        assertEquals(confirmed, List(InterviewWorkflow.cancellationKey(s.id, 0)))
        assertEquals(status, Some("Rejected"))
      }
    }
  }

  test("repair never reopens a slot the provider already cancelled") {
    scenario { s =>
      for {
        cancelling <- cancel(s)
        // The provider cancelled, but the confirmation was lost and the budget ran out.
        _ <- s.calendarLedger
          .cancel(s.id, InterviewWorkflow.cancellationKey(s.id, 0), now)
          .value
          .flatMap(_.fold(e => IO.raiseError(new AssertionError(s"$e")), _ => IO.unit))
        before <- s.reservation(0)
        failed <- exhaust(s, cancelling)
        repaired <- repair(s, failed)
        end <- s.drive(List(s.worker()), _.phase == InterviewWorkflowPhase.Cancelled)
        after <- s.reservation(0)
        calls <- s.faults.cancelCalls.get
      } yield {
        assertEquals(repaired.phase, InterviewWorkflowPhase.CancelPending)
        assertEquals(end._1.phase, InterviewWorkflowPhase.Cancelled)
        assertEquals(after, before, "the cancelled reservation is exactly as the provider left it")
        assertEquals(calls, Nil, "found by lookup: the cancel was not even requested again")
      }
    }
  }

  test("a failed cancellation notification is repaired and delivered after the cause is gone") {
    scenario { s =>
      for {
        _ <- cancel(s)
        _ <- s.faults.notifyUnavailable.set(1000)
        failed <- s.drive(List(s.worker(maxAttempts = 2)), _.phase == InterviewWorkflowPhase.RepairRequired)
        released <- s.isReleased(0)
        _ <- s.faults.notifyUnavailable.set(0)
        repaired <- repair(s, failed._1)
        end <- s.drive(List(s.worker()), _.phase == InterviewWorkflowPhase.Cancelled)
        receipts <- s.receipts
      } yield {
        assertEquals(failed._1.repairOrigin, Some(InterviewWorkflowPhase.CancelNotificationsPending))
        assert(released, "the slot was released before the notifications failed")
        assertEquals(repaired.phase, InterviewWorkflowPhase.CancelNotificationsPending)
        assertEquals(end._1.phase, InterviewWorkflowPhase.Cancelled)
        assertEquals(receipts.size, 2)
      }
    }
  }

  test("a replacement hold that kept failing resumes by lookup and completes the reschedule after repair") {
    scenario { s =>
      for {
        proposed <- propose(s, 0L)
        _ <- accept(s, proposed.revision)
        _ <- s.faults.holdUnavailable.set(1000)
        failed <- s.drive(List(s.worker(maxAttempts = 2)), _.phase == InterviewWorkflowPhase.RepairRequired)
        _ <- s.faults.holdUnavailable.set(0)
        repaired <- repair(s, failed._1)
        end <- s.drive(List(s.worker()), w => w.phase == InterviewWorkflowPhase.Completed && w.generation == 1)
      } yield {
        assertEquals(failed._1.repairOrigin, Some(InterviewWorkflowPhase.RescheduleHoldPending))
        assertEquals(repaired.phase, InterviewWorkflowPhase.RescheduleHoldPending)
        assertEquals(end._1.interval, replacement)
      }
    }
  }

  test("a compensation that kept failing is repaired and the unneeded hold is then cancelled") {
    scenario { s =>
      for {
        proposed <- propose(s, 0L)
        _ <- accept(s, proposed.revision)
        _ <- s.setApplicationStatus(ApplicationStatus.Hired)
        _ <- s.faults.cancelUnavailable.set(1000)
        failed <- s.drive(List(s.worker(maxAttempts = 2)), _.phase == InterviewWorkflowPhase.RepairRequired)
        _ <- s.faults.cancelUnavailable.set(0)
        repaired <- repair(s, failed._1)
        end <- s.drive(List(s.worker()), w => w.phase == InterviewWorkflowPhase.Completed)
        oldHeld <- s.isHeld(0)
        newReleased <- s.isReleased(1)
      } yield {
        assertEquals(failed._1.repairOrigin, Some(InterviewWorkflowPhase.RescheduleCompensationPending))
        assertEquals(repaired.phase, InterviewWorkflowPhase.RescheduleCompensationPending)
        assertEquals(end._1.interval, s.workflow.interval)
        assert(oldHeld && newReleased)
      }
    }
  }

  test("every other reschedule phase resumes with the lookup of its own step") {
    scenario { s =>
      for {
        proposed <- propose(s, 0L)
        holding <- accept(s, proposed.revision)
        swapping <- internal(s, InterviewLifecycleEvent.HoldConfirmed, holding)
        swapRepair <- exhaust(s, swapping).flatMap(repair(s, _))
        cancellingOld <- internal(s, InterviewLifecycleEvent.SwapCommitted, swapRepair)
        oldRepair <- exhaust(s, cancellingOld).flatMap(repair(s, _))
        telling <- internal(s, InterviewLifecycleEvent.CancelConfirmed(now), oldRepair)
        tellRepair <- exhaust(s, telling).flatMap(repair(s, _))
        rows <- s.commandRows
        lookups = rows.map(_.command).collect {
          case c @ (_: InterviewLifecycleCommand.LookupRescheduleCommitReceipt |
              _: InterviewLifecycleCommand.LookupCalendarCancellation |
              _: InterviewLifecycleCommand.LookupNotificationReceipt) =>
            c
        }
      } yield {
        assertEquals(
          List(swapRepair.phase, oldRepair.phase, tellRepair.phase),
          List(
            InterviewWorkflowPhase.RescheduleSwapPending,
            InterviewWorkflowPhase.RescheduleCancelOldPending,
            InterviewWorkflowPhase.RescheduleNotificationsPending
          )
        )
        assert(lookups.contains(InterviewLifecycleCommand.LookupRescheduleCommitReceipt(s.id, 1)))
        assert(
          lookups.contains(
            InterviewLifecycleCommand.LookupCalendarCancellation(InterviewWorkflow.cancellationKey(s.id, 0))
          )
        )
        assertEquals(
          lookups.collect { case InterviewLifecycleCommand.LookupNotificationReceipt(kind, p, _) => (kind, p) }.toSet,
          Set(
            (InterviewNotificationKind.Rescheduled, InterviewParticipant.Candidate),
            (InterviewNotificationKind.Rescheduled, InterviewParticipant.Recruiter)
          )
        )
      }
    }
  }

  test("an informational notification that spent its budget needs its own repair and never changes the workflow") {
    scenario { s =>
      for {
        proposed <- propose(s, 0L)
        _ <- s.faults.notifyUnavailable.set(1000)
        // Pump until the notification's own budget is spent and it shows up for repair.
        _ <- s.pumpUntil(
          List(s.worker(maxAttempts = 3)),
          success(s.repository.findNotificationRepairs(s.id)).map(_.nonEmpty)
        )
        repairs <- success(s.repository.findNotificationRepairs(s.id))
        phase <- s.current
        _ <- s.faults.notifyUnavailable.set(0)
        count <- success(s.repository.repairNotifications(s.id, UUID.randomUUID(), now, admin))
        delivered <- IO.race(IO.sleep(20.seconds), pumpUntilReceipt(s)).map(_.isRight)
        receipts <- s.receipts
        empty <- success(s.repository.findNotificationRepairs(s.id))
      } yield {
        assertEquals(repairs.size, 1)
        assertEquals(phase.phase, InterviewWorkflowPhase.ProposalPending)
        assertEquals(phase.revision, proposed.revision, "a failed informational message changes no workflow state")
        assertEquals(count, 1)
        assert(delivered)
        assertEquals(receipts.map(_.kind), List(InterviewNotificationKind.RescheduleProposed))
        assertEquals(empty, Nil)
      }
    }
  }

  private def pumpUntilReceipt(s: InterviewLifecycleScenario): IO[Unit] = {
    val worker = s.worker("repair-pump")
    def loop: IO[Unit] =
      s.round(worker) *> s.receipts.flatMap(r => if (r.nonEmpty) IO.unit else IO.sleep(25.millis) *> loop)
    loop
  }

  private def spendBudget(s: InterviewLifecycleScenario, stepId: String): IO[Unit] =
    s.collection(MongoCollections.InterviewWorkflowCommands)
      .flatMap(
        _.updateOne(
          Filters.eq(MongoFields.Id, s"${s.id.value}:${s.id.value}:$stepId"),
          com.mongodb.client.model.Updates.set("executionAttempts", Int.box(6))
        ).void
      )

  private def stored(s: InterviewLifecycleScenario, stepId: String): IO[InterviewWorkflowCommandRecord] =
    success(s.repository.findCommand(s.id, s"${s.id.value}:$stepId")).flatMap(found =>
      IO.fromOption(found)(new AssertionError(s"missing command $stepId"))
    )

  test(
    "a spent execution budget found at claim time moves a cancel to repair and an informational notification alone"
  ) {
    scenario { s =>
      for {
        cancelling <- cancel(s)
        _ <- spendBudget(s, s"${cancelling.revision}:0")
        cancelRow <- stored(s, s"${cancelling.revision}:0")
        claimed <- success(s.repository.claimExecution(cancelRow, "worker", now, now.plusSeconds(60), 5))
        afterCancel <- s.current
        cancelAfter <- stored(s, s"${cancelling.revision}:0")
      } yield {
        assertEquals(claimed, InterviewExecutionClaimOutcome.RepairRequired)
        assertEquals(afterCancel.phase, InterviewWorkflowPhase.RepairRequired)
        assertEquals(afterCancel.repairOrigin, Some(InterviewWorkflowPhase.CancelPending))
        assertEquals(cancelAfter.state, InterviewWorkflowCommandState.RepairRequired)
      }
    } *> scenario { s =>
      for {
        proposed <- propose(s, 0L)
        _ <- spendBudget(s, s"${proposed.revision}:1")
        notifyRow <- stored(s, s"${proposed.revision}:1")
        claimed <- success(s.repository.claimExecution(notifyRow, "worker", now, now.plusSeconds(60), 5))
        after <- s.current
        repairs <- success(s.repository.findNotificationRepairs(s.id))
      } yield {
        assert(notifyRow.command.isInstanceOf[InterviewLifecycleCommand.Notify])
        assertEquals(claimed, InterviewExecutionClaimOutcome.RepairRequired)
        assertEquals(after.phase, InterviewWorkflowPhase.ProposalPending)
        assertEquals(after.revision, proposed.revision)
        assertEquals(repairs.map(_.stepId), List(s"${s.id.value}:${proposed.revision}:1"))
      }
    }
  }

  test("a publication that can never be delivered moves a cancel to repair and an informational notification alone") {
    scenario { s =>
      for {
        cancelling <- cancel(s)
        claims <- success(s.repository.claimDueCommands("publisher", now.plusSeconds(1), now.plusSeconds(60), 4))
        claim <- IO.fromOption(claims.find(_.record.stepId.endsWith(s":${cancelling.revision}:0")))(
          new AssertionError("cancel intent not claimable")
        )
        resolution <- success(s.repository.requireRepair(claim, now.plusSeconds(2), "publication_exhausted"))
        after <- s.current
      } yield {
        assertEquals(resolution, InterviewPublicationResolution.Repaired)
        assertEquals(after.phase, InterviewWorkflowPhase.RepairRequired)
        assertEquals(after.repairOrigin, Some(InterviewWorkflowPhase.CancelPending))
      }
    } *> scenario { s =>
      for {
        proposed <- propose(s, 0L)
        claims <- success(s.repository.claimDueCommands("publisher", now.plusSeconds(1), now.plusSeconds(60), 4))
        claim <- IO.fromOption(claims.find(_.record.command.isInstanceOf[InterviewLifecycleCommand.Notify]))(
          new AssertionError("notification not claimable")
        )
        resolution <- success(s.repository.requireRepair(claim, now.plusSeconds(2), "publication_exhausted"))
        after <- s.current
        repairs <- success(s.repository.findNotificationRepairs(s.id))
      } yield {
        assertEquals(resolution, InterviewPublicationResolution.Repaired)
        assertEquals(after.phase, InterviewWorkflowPhase.ProposalPending)
        assertEquals(after.revision, proposed.revision)
        assertEquals(repairs.size, 1)
      }
    }
  }
}
