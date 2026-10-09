package com.example.graphQL.cats.repository.mongo

import cats.effect.{Deferred, IO}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.workflow.*
import scala.concurrent.duration.*

/** DHW-17 and DHW-18 on the real replica set: the provider confirms the cancel before the slot counts as released,
  * participants are told afterwards, and every crash window converges with one provider effect.
  */
final class InterviewCancellationWorkerIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  import InterviewLifecycleScenario.now

  private def scenario[A](use: InterviewLifecycleScenario => IO[A]): IO[A] =
    InterviewLifecycleScenario.arranged(mongoResource)(use)

  private def cancel(s: InterviewLifecycleScenario, expected: Long = 0L): IO[InterviewWorkflow] =
    s.applied(
      InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Candidate, now),
      s.actor(UserRole.Candidate, s.workflow.candidateId, input = "cancel"),
      expected,
      now
    )

  private val key = InterviewWorkflow.cancellationKey(_, 0)

  test("the slot is released only after the provider confirms, then both participants are told and the workflow ends") {
    scenario { s =>
      for {
        _ <- cancel(s)
        gate <- Deferred[IO, Unit]
        _ <- s.faults.cancelGate.set(Some(gate))
        worker = s.worker()
        // The provider has not confirmed: the application is already rejected, the slot is still held, nobody is told.
        publishing <- (s.round(worker).replicateA_(3)).start
        _ <- IO.sleep(500.millis)
        beforeConfirm <- (s.current, s.isHeld(0), s.applicationStatus, s.receipts).tupled
        _ <- gate.complete(()).void
        _ <- publishing.joinWithNever
        end <- s.drive(List(worker), _.phase == InterviewWorkflowPhase.Cancelled)
        afterwards <- (s.isReleased(0), s.applicationStatus, s.receipts, s.faults.confirmedCancels.get).tupled
      } yield {
        val (workflowBefore, heldBefore, statusBefore, receiptsBefore) = beforeConfirm
        assertEquals(workflowBefore.phase, InterviewWorkflowPhase.CancelPending)
        assert(heldBefore, "the slot must stay held until the provider confirms the cancel")
        assertEquals(statusBefore, Some("Rejected"))
        assertEquals(receiptsBefore, Nil)
        val (releasedAfter, statusAfter, receiptsAfter, confirmed) = afterwards
        assert(releasedAfter)
        assertEquals(statusAfter, Some("Rejected"))
        assertEquals(end._1.phase, InterviewWorkflowPhase.Cancelled)
        assert(end._1.cancelledAt.nonEmpty)
        assertEquals(
          receiptsAfter.map(r => (r.kind, r.participant)).toSet,
          Set(
            (InterviewNotificationKind.Cancelled, InterviewParticipant.Candidate),
            (InterviewNotificationKind.Cancelled, InterviewParticipant.Recruiter)
          )
        )
        assertEquals(confirmed, List(key(s.id)))
      }
    }
  }

  test("a crash after the cancellation commit and before any provider call resumes on restart") {
    scenario { s =>
      for {
        _ <- cancel(s)
        // Nothing runs: the process died right after the commit. The durable intent and the rejection are all there is.
        mid <- (s.current, s.isHeld(0), s.applicationStatus, s.faults.cancelCalls.get).tupled
        restarted = s.worker("restarted-worker")
        end <- s.drive(List(restarted), _.phase == InterviewWorkflowPhase.Cancelled)
        released <- s.isReleased(0)
        status <- s.applicationStatus
        confirmed <- s.faults.confirmedCancels.get
      } yield {
        assertEquals(mid._1.phase, InterviewWorkflowPhase.CancelPending)
        assert(mid._2)
        assertEquals(mid._3, Some("Rejected"))
        assertEquals(mid._4, Nil)
        assert(released)
        assertEquals(status, Some("Rejected"))
        assertEquals(end._1.phase, InterviewWorkflowPhase.Cancelled)
        assertEquals(confirmed.size, 1)
      }
    }
  }

  test("a crash after the provider confirmed the cancel and before the workflow records it converges with one effect") {
    scenario { s =>
      for {
        _ <- cancel(s)
        _ <- s.faults.crashAfterCancel.set(true)
        // A short lease lets the restarted worker reclaim the command the dead one was executing.
        dying = s.worker("dying-worker", claimLease = 1.second)
        first <- s.drive(List(dying), _ => false, rounds = 3).attempt
        mid <- (s.current, s.isReleased(0)).tupled
        restarted = s.worker("restarted-worker", claimLease = 1.second)
        end <- s.drive(List(restarted), _.phase == InterviewWorkflowPhase.Cancelled, rounds = 800)
        confirmed <- s.faults.confirmedCancels.get
        calls <- s.faults.cancelCalls.get
        status <- s.applicationStatus
        receipts <- s.receipts
      } yield {
        assert(first.isLeft)
        // The provider already released the slot, but the workflow has not recorded it: still CancelPending.
        assertEquals(mid._1.phase, InterviewWorkflowPhase.CancelPending)
        assert(mid._2)
        assertEquals(end._1.phase, InterviewWorkflowPhase.Cancelled)
        assertEquals(confirmed, List(key(s.id)), "the provider confirmed the cancel exactly once")
        assert(calls.forall(_ == key(s.id)), "every attempt reused the same key")
        assertEquals(status, Some("Rejected"))
        assertEquals(receipts.size, 2)
      }
    }
  }

  test("an unknown cancel outcome is answered by a lookup and a retry of the same key, never by an assumption") {
    scenario { s =>
      for {
        _ <- cancel(s)
        _ <- s.faults.cancelUnavailable.set(2)
        worker = s.worker()
        end <- s.drive(List(worker), _.phase == InterviewWorkflowPhase.Cancelled)
        calls <- s.faults.cancelCalls.get
        confirmed <- s.faults.confirmedCancels.get
        rows <- s.commandRows
      } yield {
        assertEquals(end._1.phase, InterviewWorkflowPhase.Cancelled)
        assertEquals(calls, List(key(s.id), key(s.id), key(s.id)))
        assertEquals(confirmed, List(key(s.id)))
        // Each unavailable answer was followed by a lookup of the same key before the retry.
        assert(rows.exists(_.command.isInstanceOf[InterviewLifecycleCommand.LookupCalendarCancellation]))
      }
    }
  }

  test("a permanently failing provider leaves visible repair with the application rejected and the slot still held") {
    scenario { s =>
      for {
        _ <- cancel(s)
        _ <- s.faults.cancelUnavailable.set(1000)
        worker = s.worker(maxAttempts = 3)
        end <- s.drive(List(worker), _.phase == InterviewWorkflowPhase.RepairRequired)
        held <- s.isHeld(0)
        status <- s.applicationStatus
      } yield {
        assertEquals(end._1.phase, InterviewWorkflowPhase.RepairRequired)
        assertEquals(end._1.repairOrigin, Some(InterviewWorkflowPhase.CancelPending))
        assert(held, "an unconfirmed cancel never releases the slot")
        assertEquals(status, Some("Rejected"))
      }
    }
  }

  test("a crash after the provider recorded a notification is answered by the lookup, not by a second delivery") {
    scenario { s =>
      for {
        _ <- cancel(s)
        _ <- s.faults.crashAfterDelivery.set(true)
        dying = s.worker("dying-worker", claimLease = 1.second)
        end <- s.drive(List(dying), _.phase == InterviewWorkflowPhase.Cancelled, rounds = 800)
        deliveries <- s.faults.deliveries.get
        receipts <- s.receipts
        rows <- s.commandRows
      } yield {
        assertEquals(end._1.phase, InterviewWorkflowPhase.Cancelled)
        assertEquals(receipts.size, 2, "one receipt per recipient and kind")
        assertEquals(deliveries.size, 2, "each recipient was sent to exactly once")
        assertEquals(deliveries.distinct.size, 2)
        assert(
          rows.exists(_.command.isInstanceOf[InterviewLifecycleCommand.LookupNotificationReceipt]),
          "the recorded delivery was found by lookup"
        )
      }
    }
  }

  test("a crash before the provider recorded a notification sends it again: a duplicate attempt, never a silent loss") {
    scenario { s =>
      for {
        _ <- cancel(s)
        _ <- s.faults.crashBeforeDelivery.set(true)
        dying = s.worker("dying-worker", claimLease = 1.second)
        end <- s.drive(List(dying), _.phase == InterviewWorkflowPhase.Cancelled, rounds = 800)
        deliveries <- s.faults.deliveries.get
        receipts <- s.receipts
      } yield {
        assertEquals(end._1.phase, InterviewWorkflowPhase.Cancelled)
        assertEquals(receipts.size, 2, "one receipt per recipient and kind")
        assertEquals(deliveries.size, 3, "the crashed delivery was attempted again")
        assertEquals(deliveries.groupBy(identity).values.map(_.size).toList.sorted, List(1, 2))
      }
    }
  }
}
