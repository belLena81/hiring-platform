package com.example.graphQL.cats.service.application

import cats.data.EitherT
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.InterviewLifecycleFixtures.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.port.*
import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** DHW-17, DHW-18, DHW-21, DHW-22 and DHW-24 at the worker boundary: each cancel and reschedule intent asks its
  * provider for exactly its effect, an unconfirmed answer is never a confirmation, and results become the right event.
  */
final class InterviewLifecycleExecutorSpec extends CatsEffectSuite {
  import InterviewLifecycleCommand as C

  private val at = now.plusSeconds(10)
  private val settings = InterviewWorkerSettings("executor", 1.second, 60.seconds, 5.seconds, 3, 1.second, 30.seconds)
  private def key(suffix: String) = s"${workflowId.value}:$suffix"
  private def stableId(value: String) = InterviewMessagePolicy.stableId(value)

  private final case class Calls(
      cancels: Vector[String],
      reserves: Vector[String],
      lookups: Vector[String],
      notifies: Vector[(InterviewNotificationKind, String)],
      notificationLookups: Vector[String],
      approvals: Int
  )
  private val noCalls = Calls(Vector.empty, Vector.empty, Vector.empty, Vector.empty, Vector.empty, 0)

  private final case class Providers(
      cancel: InterviewProviderIO[InterviewCalendarCancellation] =
        EitherT.rightT(InterviewCalendarCancellation.Cancelled(at)),
      reserve: InterviewProviderIO[InterviewCalendarReservation] = EitherT.leftT(InterviewProviderError.Unavailable),
      lookup: InterviewProviderIO[Option[InterviewCalendarReservation]] = EitherT.rightT(None),
      delivers: Boolean = true,
      notificationLookup: InterviewProviderIO[Option[InterviewNotificationReceipt]] = EitherT.rightT(None),
      approval: Either[RepositoryError, Unit] = Right(()),
      hasApproval: Either[RepositoryError, Boolean] = Right(false)
  )

  private def reservation(released: Option[Instant], held: InterviewInterval = interval) =
    InterviewCalendarReservation(workflowId, key("reserve:g1"), candidateId, recruiterId, held, now, released)

  private def receiptFor(kind: InterviewNotificationKind, participant: InterviewParticipant, k: String) =
    InterviewNotificationReceipt(workflowId, UserId(UUID.randomUUID()), participant, kind, k, at)

  private def record(
      workflow: InterviewWorkflow,
      command: InterviewLifecycleCommand,
      result: Option[InterviewCommandResult]
  ) =
    InterviewWorkflowCommandRecord(
      workflow.id,
      s"${workflow.id.value}:${workflow.revision}:0",
      workflow.revision,
      command,
      if (result.isDefined) InterviewWorkflowCommandState.ResultPublished else InterviewWorkflowCommandState.Published,
      1,
      now,
      now,
      result,
      1
    )

  private def messageFor(
      workflow: InterviewWorkflow,
      stored: InterviewWorkflowCommandRecord,
      result: Option[InterviewResult]
  ) = {
    val commandId = stableId(stored.stepId)
    InterviewMessage(
      if (result.isDefined) stableId(s"$commandId:result") else commandId,
      workflow.id.value,
      stored.stepId,
      InterviewMessagePolicy.step(stored.command),
      stored.revision,
      if (result.isDefined) commandId else workflow.id.value,
      workflow.preCommitDeadline,
      result,
      stored.occurredAt
    )
  }

  /** Executes one command through `receiveCommand`; returns the result the worker recorded and what it called. */
  private def execute(
      workflow: InterviewWorkflow,
      command: InterviewLifecycleCommand,
      providers: Providers = Providers(),
      clock: Instant = at
  ): IO[(Option[InterviewCommandResult], Calls, Boolean)] =
    for {
      calls <- Ref.of[IO, Calls](noCalls)
      recorded <- Ref.of[IO, Option[InterviewCommandResult]](None)
      stored = record(workflow, command, None)
      claim = ClaimedInterviewWorkflowCommand(stored, "executor", new UUID(0L, 90L), clock.plusSeconds(60))
      repository = new TestInterviewWorkflowRepository {
        override def findForAdmin(id: InterviewWorkflowId) = RepositoryIO.fromEither(Right(Some(workflow)))
        override def findCommand(id: InterviewWorkflowId, step: String) = RepositoryIO.fromEither(Right(Some(stored)))
        override def claimExecution(
            value: InterviewWorkflowCommandRecord,
            workerId: String,
            now: Instant,
            leaseUntil: Instant,
            maxAttempts: Int
        ) = RepositoryIO.fromEither(Right(InterviewExecutionClaimOutcome.Acquired(claim)))
        override def recordResult(
            value: ClaimedInterviewWorkflowCommand,
            result: InterviewCommandResult,
            now: Instant
        ) =
          RepositoryIO.lift(recorded.set(Some(result)))
        override def approveRescheduledInterval(
            value: InterviewWorkflow,
            now: Instant,
            execution: ClaimedInterviewWorkflowCommand
        ) = RepositoryIO.lift(calls.update(c => c.copy(approvals = c.approvals + 1))) *>
          RepositoryIO.fromEither(providers.approval)
        override def hasRescheduleApproval(id: InterviewWorkflowId, generation: Int) =
          RepositoryIO.fromEither(providers.hasApproval)
      }
      calendar = new InterviewCalendarProvider {
        override def reserve(
            id: InterviewWorkflowId,
            k: String,
            candidate: UserId,
            recruiter: UserId,
            held: InterviewInterval,
            now: Instant,
            execution: Option[ClaimedInterviewWorkflowCommand]
        ) = EitherT.liftF[IO, InterviewProviderError, Unit](calls.update(c => c.copy(reserves = c.reserves :+ k))) *>
          providers.reserve
        override def lookup(id: InterviewWorkflowId, k: String) =
          EitherT.liftF[IO, InterviewProviderError, Unit](calls.update(c => c.copy(lookups = c.lookups :+ k))) *>
            providers.lookup
        override def cancel(
            id: InterviewWorkflowId,
            k: String,
            now: Instant,
            execution: Option[ClaimedInterviewWorkflowCommand]
        ) = EitherT.liftF[IO, InterviewProviderError, Unit](calls.update(c => c.copy(cancels = c.cancels :+ k))) *>
          providers.cancel
        override def release(k: String, now: Instant, execution: Option[ClaimedInterviewWorkflowCommand]) =
          EitherT.leftT[IO, Unit](InterviewProviderError.Unavailable)
      }
      notifications = new InterviewNotificationProvider {
        override def notify(
            id: InterviewWorkflowId,
            recipient: UserId,
            participant: InterviewParticipant,
            kind: InterviewNotificationKind,
            k: String,
            now: Instant,
            execution: Option[ClaimedInterviewWorkflowCommand]
        ) = EitherT.liftF[IO, InterviewProviderError, Unit](
          calls.update(c => c.copy(notifies = c.notifies :+ (kind -> k)))
        ) *>
          (if (providers.delivers) EitherT.rightT[IO, InterviewProviderError](receiptFor(kind, participant, k))
           else EitherT.leftT[IO, InterviewNotificationReceipt](InterviewProviderError.Unavailable))
        override def lookup(k: String) =
          EitherT.liftF[IO, InterviewProviderError, Unit](
            calls.update(c => c.copy(notificationLookups = c.notificationLookups :+ k))
          ) *> providers.notificationLookup
      }
      worker = new InterviewWorkflowWorker(
        repository,
        calendar,
        notifications,
        settings,
        Diagnostics.noop,
        IO.pure(clock)
      )
      durable <- worker.receiveCommand(messageFor(workflow, stored, None))
      result <- recorded.get
      seen <- calls.get
    } yield (result, seen, durable)

  private val cancelling = workflowIn(InterviewWorkflowPhase.CancelPending)
  private val cancelKey = InterviewWorkflow.cancellationKey(workflowId, 0)

  test("DHW-17 a confirmed or already confirmed cancel is the only thing that counts as released") {
    List(
      InterviewCalendarCancellation.Cancelled(at) -> InterviewCommandResult.Succeeded,
      InterviewCalendarCancellation.AlreadyCancelled(at) -> InterviewCommandResult.Succeeded,
      // Definitive "no such reservation": rejected, so it ends in repair instead of a cancel and lookup loop.
      InterviewCalendarCancellation.UnknownReservation -> InterviewCommandResult.Rejected
    ).traverse_ { case (answer, expected) =>
      execute(cancelling, C.CancelCalendarSlot(cancelKey), Providers(cancel = EitherT.rightT(answer))).map {
        case (result, calls, durable) =>
          assertEquals(result, Some(expected))
          assertEquals(calls.cancels, Vector(cancelKey))
          assert(durable)
      }
    }
  }

  test("DHW-18 an unavailable or refused provider cancel is an unknown outcome, never a confirmation") {
    List(InterviewProviderError.Unavailable, InterviewProviderError.Conflict, InterviewProviderError.InvalidReceipt)
      .traverse_ { error =>
        execute(cancelling, C.CancelCalendarSlot(cancelKey), Providers(cancel = EitherT.leftT(error))).map {
          case (result, calls, _) =>
            assertEquals(result, Some(InterviewCommandResult.OutcomeUnknown))
            assertEquals(calls.cancels, Vector(cancelKey))
        }
      }
  }

  test(
    "DHW-18 the cancel lookup finds a released slot, reports a held or missing one as absent, and fails as unknown"
  ) {
    val lookup = C.LookupCalendarCancellation(cancelKey)
    for {
      released <- execute(cancelling, lookup, Providers(lookup = EitherT.rightT(Some(reservation(Some(at))))))
      held <- execute(cancelling, lookup, Providers(lookup = EitherT.rightT(Some(reservation(None)))))
      missing <- execute(cancelling, lookup)
      failing <- execute(cancelling, lookup, Providers(lookup = EitherT.leftT(InterviewProviderError.Unavailable)))
    } yield {
      assertEquals(released._1, Some(InterviewCommandResult.Found))
      assertEquals(held._1, Some(InterviewCommandResult.Absent))
      assertEquals(missing._1, Some(InterviewCommandResult.Absent))
      assertEquals(failing._1, Some(InterviewCommandResult.OutcomeUnknown))
      // A lookup reads; it never asks the provider to cancel.
      assertEquals(List(released, held, missing, failing).map(_._2.cancels), List.fill(4)(Vector.empty))
    }
  }

  private val holding =
    workflowIn(InterviewWorkflowPhase.RescheduleHoldPending, _.copy(pendingInterval = Some(replacement)))
  private val holdKey = key("reserve:g1")

  test("DHW-21 the replacement hold succeeds, conflicts, or is looked up when the provider refuses") {
    val hold = C.HoldReplacementSlot(holdKey, replacement)
    for {
      held <- execute(holding, hold, Providers(reserve = EitherT.rightT(reservation(None, replacement))))
      unavailable <- execute(holding, hold)
      refusedButHeld <- execute(
        holding,
        hold,
        Providers(
          reserve = EitherT.leftT(InterviewProviderError.Conflict),
          lookup = EitherT.rightT(Some(reservation(None, replacement)))
        )
      )
      refused <- execute(holding, hold, Providers(reserve = EitherT.leftT(InterviewProviderError.Conflict)))
      refusedUnknown <- execute(
        holding,
        hold,
        Providers(
          reserve = EitherT.leftT(InterviewProviderError.Conflict),
          lookup = EitherT.leftT(InterviewProviderError.Unavailable)
        )
      )
    } yield {
      assertEquals(held._1, Some(InterviewCommandResult.Succeeded))
      assertEquals(unavailable._1, Some(InterviewCommandResult.OutcomeUnknown))
      assertEquals(refusedButHeld._1, Some(InterviewCommandResult.Succeeded))
      assertEquals(refused._1, Some(InterviewCommandResult.Rejected))
      assertEquals(refusedUnknown._1, Some(InterviewCommandResult.OutcomeUnknown))
      assertEquals(held._2.reserves, Vector(holdKey))
      assertEquals(held._2.cancels, Vector.empty)
    }
  }

  test("DHW-21 the hold lookup reports a live hold found and anything else absent") {
    val lookup = C.LookupReplacementHold(holdKey)
    for {
      found <- execute(holding, lookup, Providers(lookup = EitherT.rightT(Some(reservation(None, replacement)))))
      cancelled <- execute(
        holding,
        lookup,
        Providers(lookup = EitherT.rightT(Some(reservation(Some(at), replacement))))
      )
      failing <- execute(holding, lookup, Providers(lookup = EitherT.leftT(InterviewProviderError.Unavailable)))
    } yield {
      assertEquals(found._1, Some(InterviewCommandResult.Found))
      assertEquals(cancelled._1, Some(InterviewCommandResult.Absent))
      assertEquals(failing._1, Some(InterviewCommandResult.OutcomeUnknown))
    }
  }

  private val swapping =
    workflowIn(InterviewWorkflowPhase.RescheduleSwapPending, _.copy(pendingInterval = Some(replacement)))

  test("DHW-22 the swap guard approves, refuses, or is unknown, and a started interval is refused without asking") {
    val commit = C.CommitRescheduledInterval(replacement, 1)
    for {
      approved <- execute(swapping, commit)
      refused <- execute(swapping, commit, Providers(approval = Left(RepositoryError.Conflict)))
      unknown <- execute(swapping, commit, Providers(approval = Left(RepositoryError.Unavailable)))
      started <- execute(swapping, commit, clock = swapping.interval.startsAt)
      // A replacement that starts before the old interval makes its own start the deadline.
      earlier = swapping.copy(pendingInterval =
        Some(
          InterviewInterval(
            swapping.interval.startsAt.minusSeconds(3600),
            swapping.interval.startsAt.minusSeconds(1800)
          )
        )
      )
      replacementStarted <- execute(
        earlier,
        C.CommitRescheduledInterval(earlier.pendingInterval.getOrElse(replacement), 1),
        clock = swapping.interval.startsAt.minusSeconds(3600)
      )
    } yield {
      assertEquals(replacementStarted._1, Some(InterviewCommandResult.Rejected))
      assertEquals(replacementStarted._2.approvals, 0)
      assertEquals(approved._1, Some(InterviewCommandResult.Succeeded))
      assertEquals(refused._1, Some(InterviewCommandResult.Rejected))
      assertEquals(unknown._1, Some(InterviewCommandResult.OutcomeUnknown))
      assertEquals(started._1, Some(InterviewCommandResult.Rejected))
      assertEquals(started._2.approvals, 0)
    }
  }

  test("DHW-22 the swap receipt lookup reports found, absent or unknown") {
    val lookup = C.LookupRescheduleCommitReceipt(workflowId, 1)
    for {
      found <- execute(swapping, lookup, Providers(hasApproval = Right(true)))
      absent <- execute(swapping, lookup)
      unknown <- execute(swapping, lookup, Providers(hasApproval = Left(RepositoryError.Unavailable)))
    } yield {
      assertEquals(found._1, Some(InterviewCommandResult.Found))
      assertEquals(absent._1, Some(InterviewCommandResult.Absent))
      assertEquals(unknown._1, Some(InterviewCommandResult.OutcomeUnknown))
    }
  }

  test("DHW-20 an expiry command makes no provider call") {
    val proposing = proposalPending(now.plusSeconds(5))
    execute(proposing, C.ExpireProposal(now.plusSeconds(5))).map { case (result, calls, _) =>
      assertEquals(result, Some(InterviewCommandResult.Succeeded))
      assertEquals(calls, noCalls)
    }
  }

  private val cancelNotifying = workflowIn(InterviewWorkflowPhase.CancelNotificationsPending)
  private val cancelledKey = key("cancelled:notify:Candidate")

  test("DHW-24 a round notification is sent once with its kind and answers unknown when the provider fails") {
    val notify = C.Notify(InterviewNotificationKind.Cancelled, InterviewParticipant.Candidate, cancelledKey)
    for {
      sent <- execute(cancelNotifying, notify)
      failed <- execute(cancelNotifying, notify, Providers(delivers = false))
    } yield {
      assertEquals(sent._1, Some(InterviewCommandResult.Succeeded))
      assertEquals(sent._2.notifies, Vector(InterviewNotificationKind.Cancelled -> cancelledKey))
      // Round deliveries are not looked up first: the provider receipt is the idempotent record.
      assertEquals(sent._2.notificationLookups, Vector.empty)
      assertEquals(failed._1, Some(InterviewCommandResult.OutcomeUnknown))
    }
  }

  test("DHW-24 an informational notification is looked up first, so a recorded delivery is not sent again") {
    val declined = key("rescheduleDeclined:r4:notify:Recruiter")
    val notify = C.Notify(InterviewNotificationKind.RescheduleDeclined, InterviewParticipant.Recruiter, declined)
    val completed = workflowIn(InterviewWorkflowPhase.Completed)
    val receipt = receiptFor(InterviewNotificationKind.RescheduleDeclined, InterviewParticipant.Recruiter, declined)
    for {
      already <- execute(completed, notify, Providers(notificationLookup = EitherT.rightT(Some(receipt))))
      fresh <- execute(completed, notify)
      unknown <- execute(
        completed,
        notify,
        Providers(notificationLookup = EitherT.leftT(InterviewProviderError.Unavailable))
      )
      failing <- execute(completed, notify, Providers(delivers = false))
    } yield {
      assertEquals(already._1, Some(InterviewCommandResult.Succeeded))
      assertEquals(already._2.notifies, Vector.empty)
      assertEquals(fresh._1, Some(InterviewCommandResult.Succeeded))
      assertEquals(fresh._2.notifies, Vector(InterviewNotificationKind.RescheduleDeclined -> declined))
      assertEquals(unknown._1, Some(InterviewCommandResult.OutcomeUnknown))
      assertEquals(unknown._2.notifies, Vector.empty, "an unreadable receipt store is not a reason to send")
      assertEquals(failing._1, Some(InterviewCommandResult.OutcomeUnknown))
    }
  }

  test("a notification receipt lookup is a read: found, absent or unknown") {
    val lookup =
      C.LookupNotificationReceipt(InterviewNotificationKind.Cancelled, InterviewParticipant.Candidate, cancelledKey)
    val receipt = receiptFor(InterviewNotificationKind.Cancelled, InterviewParticipant.Candidate, cancelledKey)
    for {
      found <- execute(cancelNotifying, lookup, Providers(notificationLookup = EitherT.rightT(Some(receipt))))
      absent <- execute(cancelNotifying, lookup)
      unknown <- execute(
        cancelNotifying,
        lookup,
        Providers(notificationLookup = EitherT.leftT(InterviewProviderError.Unavailable))
      )
    } yield {
      assertEquals(found._1, Some(InterviewCommandResult.Found))
      assertEquals(absent._1, Some(InterviewCommandResult.Absent))
      assertEquals(unknown._1, Some(InterviewCommandResult.OutcomeUnknown))
      assertEquals(found._2.notifies, Vector.empty)
    }
  }

  // --- Applying recorded results -------------------------------------------------------------------------------

  private final case class Applied(
      lifecycle: Vector[(Long, InterviewLifecycleEvent, InterviewLifecycleOrigin, Option[Instant])],
      settlements: Vector[InterviewNotificationSettlement],
      quarantined: Vector[String]
  )

  private def receive(
      workflow: InterviewWorkflow,
      command: InterviewLifecycleCommand,
      result: InterviewResult,
      attempts: Long = 1L,
      outcome: InterviewLifecycleOutcome = InterviewLifecycleOutcome.Duplicate(cancelling)
  ): IO[(Boolean, Applied)] =
    for {
      applied <- Ref.of[IO, Applied](Applied(Vector.empty, Vector.empty, Vector.empty))
      stored = record(workflow, command, Some(InterviewCommandResult.valueOf(result.toString)))
      repository = new TestInterviewWorkflowRepository {
        override def findForAdmin(id: InterviewWorkflowId) = RepositoryIO.fromEither(Right(Some(workflow)))
        override def findCommand(id: InterviewWorkflowId, step: String) = RepositoryIO.fromEither(Right(Some(stored)))
        override def attemptCount(id: InterviewWorkflowId, requested: InterviewCommand) =
          RepositoryIO.fromEither(Right(attempts))
        override def applyLifecycle(
            id: InterviewWorkflowId,
            expectedRevision: Long,
            event: InterviewLifecycleEvent,
            origin: InterviewLifecycleOrigin,
            now: Instant,
            availableAt: Option[Instant]
        ) = RepositoryIO
          .lift(
            applied.update(a => a.copy(lifecycle = a.lifecycle :+ ((expectedRevision, event, origin, availableAt))))
          )
          .as(outcome)
        override def settleNotificationResult(
            value: InterviewWorkflowCommandRecord,
            settlement: InterviewNotificationSettlement,
            now: Instant
        ) = RepositoryIO.lift(applied.update(a => a.copy(settlements = a.settlements :+ settlement))).as(true)
        override def quarantine(identity: String, now: Instant) =
          RepositoryIO.lift(applied.update(a => a.copy(quarantined = a.quarantined :+ identity)))
      }
      worker = new InterviewWorkflowWorker(
        repository,
        new InterviewCalendarProvider {
          def reserve(
              id: InterviewWorkflowId,
              k: String,
              c: UserId,
              r: UserId,
              i: InterviewInterval,
              n: Instant,
              e: Option[ClaimedInterviewWorkflowCommand]
          ) = EitherT.leftT(InterviewProviderError.Unavailable)
          def lookup(id: InterviewWorkflowId, k: String) = EitherT.leftT(InterviewProviderError.Unavailable)
          def cancel(id: InterviewWorkflowId, k: String, n: Instant, e: Option[ClaimedInterviewWorkflowCommand]) =
            EitherT.leftT(InterviewProviderError.Unavailable)
          def release(k: String, n: Instant, e: Option[ClaimedInterviewWorkflowCommand]) =
            EitherT.leftT(InterviewProviderError.Unavailable)
        },
        new InterviewNotificationProvider {
          def notify(
              id: InterviewWorkflowId,
              r: UserId,
              p: InterviewParticipant,
              k: InterviewNotificationKind,
              key: String,
              n: Instant,
              e: Option[ClaimedInterviewWorkflowCommand]
          ) = EitherT.leftT(InterviewProviderError.Unavailable)
          def lookup(k: String) = EitherT.leftT(InterviewProviderError.Unavailable)
        },
        settings,
        Diagnostics.noop,
        IO.pure(at)
      )
      durable <- worker.receiveResult(messageFor(workflow, stored, Some(result)))
      seen <- applied.get
    } yield (durable, seen)

  test("DHW-17 a confirmed cancel result moves the workflow through the lifecycle policy with an internal cause") {
    receive(cancelling, C.CancelCalendarSlot(cancelKey), InterviewResult.Succeeded).map { case (durable, seen) =>
      assert(durable)
      assertEquals(seen.lifecycle.map(_._1), Vector(cancelling.revision))
      assertEquals(seen.lifecycle.map(_._2), Vector(InterviewLifecycleEvent.CancelConfirmed(at)))
      assert(seen.lifecycle.forall(_._3.isInstanceOf[InterviewLifecycleOrigin.Internal]))
      assertEquals(seen.lifecycle.map(_._4), Vector(None), "a confirmed result has no retry delay")
    }
  }

  test("DHW-18 an unknown cancel result is retried later by lookup, and exhaustion is repair") {
    for {
      unknown <- receive(cancelling, C.CancelCalendarSlot(cancelKey), InterviewResult.OutcomeUnknown, attempts = 1L)
      exhausted <- receive(cancelling, C.CancelCalendarSlot(cancelKey), InterviewResult.OutcomeUnknown, attempts = 3L)
      absent <- receive(cancelling, C.LookupCalendarCancellation(cancelKey), InterviewResult.Absent, attempts = 2L)
    } yield {
      assertEquals(unknown._2.lifecycle.map(_._2), Vector(InterviewLifecycleEvent.CancelOutcomeUnknown))
      assertEquals(unknown._2.lifecycle.map(_._4.isDefined), Vector(true), "the lookup waits for the backoff")
      assertEquals(exhausted._2.lifecycle.map(_._2), Vector(InterviewLifecycleEvent.RetryExhausted("provider")))
      assertEquals(absent._2.lifecycle.map(_._2), Vector(InterviewLifecycleEvent.CancelLookupAbsent))
    }
  }

  test("DHW-22 hold, swap and compensation results become their lifecycle events") {
    for {
      rejected <- receive(holding, C.HoldReplacementSlot(holdKey, replacement), InterviewResult.Rejected)
      swapRejected <- receive(swapping, C.CommitRescheduledInterval(replacement, 1), InterviewResult.Rejected)
      swapCommitted <- receive(swapping, C.CommitRescheduledInterval(replacement, 1), InterviewResult.Succeeded)
      swapFound <- receive(swapping, C.LookupRescheduleCommitReceipt(workflowId, 1), InterviewResult.Found)
    } yield {
      assertEquals(rejected._2.lifecycle.map(_._2), Vector(InterviewLifecycleEvent.HoldRejected))
      assertEquals(swapRejected._2.lifecycle.map(_._2), Vector(InterviewLifecycleEvent.SwapRejected))
      assertEquals(swapCommitted._2.lifecycle.map(_._2), Vector(InterviewLifecycleEvent.SwapCommitted))
      assertEquals(swapFound._2.lifecycle.map(_._2), Vector(InterviewLifecycleEvent.SwapLookupFound))
    }
  }

  test("DHW-20 a delivered expiry becomes the expiry event stamped when it is applied") {
    receive(proposalPending(now.plusSeconds(5)), C.ExpireProposal(now.plusSeconds(5)), InterviewResult.Succeeded).map {
      case (_, seen) => assertEquals(seen.lifecycle.map(_._2), Vector(InterviewLifecycleEvent.ProposalExpired(at)))
    }
  }

  test("DHW-20 an expiry whose execution crashed before its result was recorded still expires the proposal") {
    receive(proposalPending(now.plusSeconds(5)), C.ExpireProposal(now.plusSeconds(5)), InterviewResult.OutcomeUnknown)
      .map { case (durable, seen) =>
        assert(durable)
        assertEquals(seen.lifecycle.map(_._2), Vector(InterviewLifecycleEvent.ProposalExpired(at)))
      }
  }

  test(
    "DHW-24 an informational result never advances the workflow: success is done, failure retries then needs repair"
  ) {
    val notify = C.Notify(
      InterviewNotificationKind.RescheduleDeclined,
      InterviewParticipant.Recruiter,
      key("rescheduleDeclined:r4:notify:Recruiter")
    )
    val completed = workflowIn(InterviewWorkflowPhase.Completed)
    for {
      ok <- receive(completed, notify, InterviewResult.Succeeded)
      retry <- receive(completed, notify, InterviewResult.OutcomeUnknown, attempts = 1L)
      rejected <- receive(completed, notify, InterviewResult.Rejected, attempts = 2L)
      spent <- receive(completed, notify, InterviewResult.OutcomeUnknown, attempts = 3L)
    } yield {
      List(ok, retry, rejected, spent).foreach(result => assertEquals(result._2.lifecycle, Vector.empty))
      assertEquals(ok._2.settlements, Vector.empty)
      assert(retry._2.settlements.headOption.exists(_.isInstanceOf[InterviewNotificationSettlement.Retry]))
      assert(rejected._2.settlements.headOption.exists(_.isInstanceOf[InterviewNotificationSettlement.Retry]))
      assertEquals(spent._2.settlements, Vector(InterviewNotificationSettlement.Exhausted("notification_exhausted")))
    }
  }

  test("DHW-17 a refused transition is quarantined while a stale revision is retried later") {
    for {
      refused <- receive(
        cancelling,
        C.CancelCalendarSlot(cancelKey),
        InterviewResult.Succeeded,
        outcome = InterviewLifecycleOutcome.Rejected(InterviewWorkflowError.InvalidTransition)
      )
      stale <- receive(
        cancelling,
        C.CancelCalendarSlot(cancelKey),
        InterviewResult.Succeeded,
        outcome = InterviewLifecycleOutcome.Rejected(InterviewWorkflowError.StaleRevision)
      )
    } yield {
      assert(refused._1)
      assertEquals(refused._2.quarantined.size, 1)
      assert(!stale._1)
      assertEquals(stale._2.quarantined, Vector.empty)
    }
  }

  test("DHW-18 a lifecycle command message past the replay window fails its step instead of being executed") {
    val expiredAt = at.plus(java.time.Duration.ofDays(8))
    for {
      applied <- Ref.of[IO, Vector[(InterviewLifecycleEvent, InterviewLifecycleOrigin)]](Vector.empty)
      settled <- Ref.of[IO, Vector[InterviewNotificationSettlement]](Vector.empty)
      stored = record(cancelling, C.CancelCalendarSlot(cancelKey), None)
      informational = record(
        workflowIn(InterviewWorkflowPhase.Completed),
        C.Notify(
          InterviewNotificationKind.RescheduleDeclined,
          InterviewParticipant.Recruiter,
          key("rescheduleDeclined:r3:notify:Recruiter")
        ),
        None
      )
      current <- Ref.of[IO, (InterviewWorkflow, InterviewWorkflowCommandRecord)](cancelling -> stored)
      repository = new TestInterviewWorkflowRepository {
        override def findForAdmin(id: InterviewWorkflowId) = RepositoryIO.lift(current.get.map(value => Some(value._1)))
        override def findCommand(id: InterviewWorkflowId, step: String) =
          RepositoryIO.lift(current.get.map(value => Some(value._2)))
        override def applyLifecycle(
            id: InterviewWorkflowId,
            expectedRevision: Long,
            event: InterviewLifecycleEvent,
            origin: InterviewLifecycleOrigin,
            now: Instant,
            availableAt: Option[Instant]
        ) =
          RepositoryIO.lift(applied.update(_ :+ (event -> origin))).as(InterviewLifecycleOutcome.Duplicate(cancelling))
        override def settleNotificationResult(
            value: InterviewWorkflowCommandRecord,
            settlement: InterviewNotificationSettlement,
            now: Instant
        ) = RepositoryIO.lift(settled.update(_ :+ settlement)).as(true)
      }
      unavailableCalendar = new InterviewCalendarProvider {
        def reserve(
            id: InterviewWorkflowId,
            k: String,
            c: UserId,
            r: UserId,
            i: InterviewInterval,
            n: Instant,
            e: Option[ClaimedInterviewWorkflowCommand]
        ) = EitherT.liftF(IO.raiseError(new AssertionError("a late message must not reach the provider")))
        def lookup(id: InterviewWorkflowId, k: String) =
          EitherT.liftF(IO.raiseError(new AssertionError("a late message must not reach the provider")))
        def cancel(id: InterviewWorkflowId, k: String, n: Instant, e: Option[ClaimedInterviewWorkflowCommand]) =
          EitherT.liftF(IO.raiseError(new AssertionError("a late message must not reach the provider")))
        def release(k: String, n: Instant, e: Option[ClaimedInterviewWorkflowCommand]) =
          EitherT.liftF(IO.raiseError(new AssertionError("a late message must not reach the provider")))
      }
      silentNotifications = new InterviewNotificationProvider {
        def notify(
            id: InterviewWorkflowId,
            r: UserId,
            p: InterviewParticipant,
            k: InterviewNotificationKind,
            key: String,
            n: Instant,
            e: Option[ClaimedInterviewWorkflowCommand]
        ) = EitherT.liftF(IO.raiseError(new AssertionError("a late message must not reach the provider")))
        def lookup(k: String) =
          EitherT.liftF(IO.raiseError(new AssertionError("a late message must not reach the provider")))
      }
      worker = new InterviewWorkflowWorker(
        repository,
        unavailableCalendar,
        silentNotifications,
        settings,
        Diagnostics.noop,
        IO.pure(expiredAt)
      )
      first <- worker.receiveCommand(messageFor(cancelling, stored, None))
      _ <- current.set(workflowIn(InterviewWorkflowPhase.Completed) -> informational)
      second <- worker.receiveCommand(messageFor(workflowIn(InterviewWorkflowPhase.Completed), informational, None))
      events <- applied.get
      settlements <- settled.get
    } yield {
      assert(first && second)
      assertEquals(events.map(_._1), Vector(InterviewLifecycleEvent.RetryExhausted("replay_expired")))
      assert(events.forall(_._2.isInstanceOf[InterviewLifecycleOrigin.Internal]))
      assertEquals(settlements, Vector(InterviewNotificationSettlement.Exhausted("replay_expired")))
    }
  }

  private final case class EarlyExpiry(durable: Boolean, requeued: Vector[Instant], quarantined: Vector[String])

  /** Delivers an expiry message that is a day early; `requeue` is the repository's answer, `current` the re-read row.
    */
  private def earlyExpiry(
      due: Instant,
      requeue: Either[RepositoryError, Boolean],
      current: Option[InterviewWorkflowCommandRecord] => Option[InterviewWorkflowCommandRecord] = identity
  ): IO[EarlyExpiry] = {
    val open = proposalPending(due)
    val stored = record(open, C.ExpireProposal(due), None).copy(occurredAt = due, availableAt = due)
    for {
      deferred <- Ref.of[IO, Vector[Instant]](Vector.empty)
      applied <- Ref.of[IO, Vector[InterviewLifecycleEvent]](Vector.empty)
      quarantined <- Ref.of[IO, Vector[String]](Vector.empty)
      reads <- Ref.of[IO, Int](0)
      repository = new TestInterviewWorkflowRepository {
        override def findForAdmin(id: InterviewWorkflowId) = RepositoryIO.fromEither(Right(Some(open)))
        // The first read is the message's own lookup; a later read is the re-read after a failed requeue.
        override def findCommand(id: InterviewWorkflowId, step: String) =
          RepositoryIO.lift(reads.getAndUpdate(_ + 1).map(n => if (n == 0) Some(stored) else current(Some(stored))))
        override def deferExpiry(value: InterviewWorkflowCommandRecord, availableAt: Instant) =
          RepositoryIO.lift(deferred.update(_ :+ availableAt)) *> RepositoryIO.fromEither(requeue)
        override def applyLifecycle(
            id: InterviewWorkflowId,
            expectedRevision: Long,
            event: InterviewLifecycleEvent,
            origin: InterviewLifecycleOrigin,
            at: Instant,
            availableAt: Option[Instant]
        ) = RepositoryIO.lift(applied.update(_ :+ event)).as(InterviewLifecycleOutcome.Duplicate(open))
        override def quarantine(identity: String, at: Instant) =
          RepositoryIO.lift(quarantined.update(_ :+ identity))
      }
      never = new InterviewCalendarProvider {
        private def refuse[A]: InterviewProviderIO[A] =
          EitherT.liftF(IO.raiseError(new AssertionError("an early expiry must not reach the provider")))
        def reserve(
            id: InterviewWorkflowId,
            k: String,
            c: UserId,
            r: UserId,
            i: InterviewInterval,
            n: Instant,
            e: Option[ClaimedInterviewWorkflowCommand]
        ) = refuse
        def lookup(id: InterviewWorkflowId, k: String) = refuse
        def cancel(id: InterviewWorkflowId, k: String, n: Instant, e: Option[ClaimedInterviewWorkflowCommand]) = refuse
        def release(k: String, n: Instant, e: Option[ClaimedInterviewWorkflowCommand]) = refuse
      }
      silent = new InterviewNotificationProvider {
        def notify(
            id: InterviewWorkflowId,
            r: UserId,
            p: InterviewParticipant,
            k: InterviewNotificationKind,
            key: String,
            n: Instant,
            e: Option[ClaimedInterviewWorkflowCommand]
        ) = EitherT.liftF(IO.raiseError(new AssertionError("no notification")))
        def lookup(k: String) = EitherT.liftF(IO.raiseError(new AssertionError("no notification")))
      }
      worker = new InterviewWorkflowWorker(repository, never, silent, settings, Diagnostics.noop, IO.pure(now))
      durable <- worker.receiveCommand(messageFor(open, stored, None))
      requeued <- deferred.get
      events <- applied.get
      refused <- quarantined.get
    } yield {
      assertEquals(events, Vector.empty)
      EarlyExpiry(durable, requeued, refused)
    }
  }

  private val oneDayAhead = now.plus(java.time.Duration.ofDays(1))

  test("DHW-20 an early expiry is requeued after its due time plus a bounded backoff and acknowledged") {
    earlyExpiry(oneDayAhead, Right(true)).map { result =>
      assert(result.durable, "the message is acknowledged so the partition does not restart-loop")
      // 24 hours behind, clamped to the 30 second maximum backoff of the test settings.
      assertEquals(result.requeued, Vector(oneDayAhead.plusSeconds(30)))
      assertEquals(result.quarantined, Vector.empty)
    }
  }

  test("DHW-20 an early expiry just beyond the skew tolerance waits as long as it is behind") {
    val due = now.plusSeconds(6)
    earlyExpiry(due, Right(true)).map(result => assertEquals(result.requeued, Vector(due.plusSeconds(6))))
  }

  test("DHW-20 an expiry that could not be requeued is acknowledged only when its row is settled elsewhere") {
    def withState(state: InterviewWorkflowCommandState, result: Option[InterviewCommandResult] = None) =
      (row: Option[InterviewWorkflowCommandRecord]) => row.map(_.copy(state = state, result = result))
    for {
      superseded <- earlyExpiry(oneDayAhead, Right(false), withState(InterviewWorkflowCommandState.Superseded))
      repair <- earlyExpiry(oneDayAhead, Right(false), withState(InterviewWorkflowCommandState.RepairRequired))
      gone <- earlyExpiry(oneDayAhead, Right(false), _ => None)
      claimed <- earlyExpiry(oneDayAhead, Right(false), withState(InterviewWorkflowCommandState.Claimed))
      published <- earlyExpiry(oneDayAhead, Right(false), withState(InterviewWorkflowCommandState.Published))
      failing <- earlyExpiry(oneDayAhead, Left(RepositoryError.Unavailable))
    } yield {
      assert(superseded.durable && repair.durable && gone.durable)
      assert(!claimed.durable && !published.durable, "an unsettled row must not lose its message")
      assert(!failing.durable)
      assertEquals(List(superseded, repair, gone, claimed, published, failing).flatMap(_.quarantined), Nil)
    }
  }

  test("DHW-18 a cancel the provider cannot find ends in repair, not in a lookup loop") {
    receive(cancelling, C.CancelCalendarSlot(cancelKey), InterviewResult.Rejected).map { case (durable, seen) =>
      assert(durable)
      assertEquals(seen.lifecycle.map(_._2), Vector(InterviewLifecycleEvent.RetryExhausted("unknown_reservation")))
    }
  }
}
