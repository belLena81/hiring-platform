package com.example.graphQL.cats.service.application

import com.example.graphQL.cats.domain.workflow.*
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.*
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

enum InterviewMessageRejection(val code: String) {
  case Future extends InterviewMessageRejection("future")
  case Expired extends InterviewMessageRejection("expired")
  case Unknown extends InterviewMessageRejection("unknown")
  case Inconsistent extends InterviewMessageRejection("inconsistent")
  case InvalidTransition extends InterviewMessageRejection("invalid_transition")
}

enum InterviewReplayDisposition {
  case Timely, Expired, Deferred, Future
}

enum InterviewCommandAdmission {
  case Acknowledge
  case Execute(workflow: InterviewWorkflow, command: InterviewWorkflowCommandRecord)
  case Quarantine(reason: InterviewMessageRejection)
}

/** Pure application policy over service-owned observations; the worker interprets its decisions. */
object InterviewMessagePolicy {
  def stableId(value: String): UUID = UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8))

  def replayDisposition(
      message: InterviewMessage,
      now: Instant,
      replayWindow: FiniteDuration,
      clockSkewTolerance: FiniteDuration = scala.concurrent.duration.Duration.Zero
  ): InterviewReplayDisposition =
    if (message.occurredAt.isAfter(now.plusMillis(clockSkewTolerance.toMillis))) InterviewReplayDisposition.Future
    else if (message.occurredAt.isAfter(now)) InterviewReplayDisposition.Deferred
    else if (message.occurredAt.isBefore(now.minusMillis(replayWindow.toMillis))) InterviewReplayDisposition.Expired
    else InterviewReplayDisposition.Timely

  private[application] def applicable(workflow: InterviewWorkflow, command: InterviewWorkflowCommandRecord): Boolean =
    command.state != InterviewWorkflowCommandState.Superseded &&
      InterviewCommands.isApplicable(workflow, command.revision, command.command)

  private def identityMatches(
      workflow: InterviewWorkflow,
      command: InterviewWorkflowCommandRecord,
      message: InterviewMessage
  ): Boolean = {
    val commandId = stableId(message.stepId)
    val resultMessage = message.result.isDefined
    command.revision == message.revision && command.occurredAt == message.occurredAt &&
    step(command.command) == message.step && message.deadline == workflow.preCommitDeadline &&
    message.messageId == (if (resultMessage) stableId(s"$commandId:result") else commandId) &&
    message.causationId == (if (resultMessage) commandId else workflow.id.value)
  }

  def commandAdmission(
      workflow: Option[InterviewWorkflow],
      command: Option[InterviewWorkflowCommandRecord],
      message: InterviewMessage
  ): InterviewCommandAdmission = (workflow, command) match {
    case (None, _) | (_, None) => InterviewCommandAdmission.Quarantine(InterviewMessageRejection.Unknown)
    // A recorded result already owns acknowledgment; keep this before identity checks for timely commands.
    case (Some(_), Some(stored)) if stored.result.nonEmpty => InterviewCommandAdmission.Acknowledge
    case (Some(current), Some(stored))
        if applicable(current, stored) && message.result.isEmpty && identityMatches(current, stored, message) =>
      InterviewCommandAdmission.Execute(current, stored)
    case _ => InterviewCommandAdmission.Quarantine(InterviewMessageRejection.Inconsistent)
  }

  def resultAdmission(
      workflow: Option[InterviewWorkflow],
      command: Option[InterviewWorkflowCommandRecord],
      message: InterviewMessage
  ): Either[InterviewMessageRejection, (InterviewWorkflow, InterviewWorkflowCommandRecord)] =
    (workflow, command) match {
      case (None, _) => Left(InterviewMessageRejection.Unknown)
      case (Some(current), Some(stored))
          if applicable(current, stored) && identityMatches(current, stored, message) && message.result.nonEmpty &&
            stored.result == message.result.map(InterviewCommandResult.fromTransportResult) =>
        Right((current, stored))
      case _ => Left(InterviewMessageRejection.Inconsistent)
    }

  def expiredDecision(
      workflow: Option[InterviewWorkflow],
      command: Option[InterviewWorkflowCommandRecord],
      message: InterviewMessage
  ): Either[InterviewMessageRejection, InterviewWorkflowDecision] =
    (workflow, command) match {
      // Expired admission intentionally does not use the timely acknowledgment or result-equality shortcuts.
      case (Some(current), Some(stored)) if applicable(current, stored) && identityMatches(current, stored, message) =>
        InterviewWorkflow
          .decide(current, current.revision, InterviewWorkflowEvent.RetryExhausted("replay_expired"))
          .left
          .map(_ => InterviewMessageRejection.Expired)
      case _ => Left(InterviewMessageRejection.Expired)
    }

  def futureDecision(
      workflow: Option[InterviewWorkflow],
      command: Option[InterviewWorkflowCommandRecord],
      message: InterviewMessage
  ): Either[InterviewMessageRejection, InterviewWorkflowDecision] =
    (workflow, command) match {
      case (Some(current), Some(stored)) if applicable(current, stored) && identityMatches(current, stored, message) =>
        InterviewWorkflow
          .decide(current, current.revision, InterviewWorkflowEvent.RetryExhausted("future_message"))
          .left
          .map(_ => InterviewMessageRejection.Future)
      case _ => Left(InterviewMessageRejection.Future)
    }

  def resultEvent(message: InterviewMessage, attempts: Long, maxAttempts: Int): Option[InterviewWorkflowEvent] =
    if (
      attempts >= maxAttempts &&
      (message.result.contains(InterviewResult.OutcomeUnknown) || message.result.contains(InterviewResult.Absent))
    ) Some(InterviewWorkflowEvent.RetryExhausted("provider"))
    else event(message)

  def retryAvailableAt(
      result: Option[InterviewResult],
      attempts: Long,
      now: Instant,
      initialMillis: Long,
      maximumMillis: Long
  ): Option[Instant] =
    Option.when(result.contains(InterviewResult.OutcomeUnknown) || result.contains(InterviewResult.Absent))(
      now.plusMillis(InterviewWorkflowPolicy.backoffMillis(attempts, initialMillis, maximumMillis))
    )

  def step(command: InterviewCommand): InterviewStep = command match {
    case InterviewWorkflowCommand.ReserveCalendarSlot(_)                    => InterviewStep.Reserve
    case InterviewWorkflowCommand.LookupCalendarReservation(_)              => InterviewStep.LookupReservation
    case InterviewWorkflowCommand.CommitAcceptedToInterview(_)              => InterviewStep.CommitHiring
    case InterviewWorkflowCommand.LookupStatusCommitReceipt(_)              => InterviewStep.LookupCommit
    case InterviewWorkflowCommand.ReleaseCalendarSlot(_)                    => InterviewStep.Release
    case InterviewWorkflowCommand.Notify(InterviewParticipant.Candidate, _) => InterviewStep.NotifyCandidate
    case InterviewWorkflowCommand.Notify(InterviewParticipant.Recruiter, _) => InterviewStep.NotifyRecruiter
    case InterviewWorkflowCommand.LookupNotificationReceipt(InterviewParticipant.Candidate, _) =>
      InterviewStep.LookupNotifyCandidate
    case InterviewWorkflowCommand.LookupNotificationReceipt(InterviewParticipant.Recruiter, _) =>
      InterviewStep.LookupNotifyRecruiter
    case InterviewWorkflowCommand.RequireRepair(_)                              => InterviewStep.RequireRepair
    case InterviewLifecycleCommand.CancelCalendarSlot(_)                        => InterviewStep.CancelSlot
    case InterviewLifecycleCommand.LookupCalendarCancellation(_)                => InterviewStep.LookupCancellation
    case InterviewLifecycleCommand.HoldReplacementSlot(_, _)                    => InterviewStep.HoldReplacement
    case InterviewLifecycleCommand.LookupReplacementHold(_)                     => InterviewStep.LookupReplacementHold
    case InterviewLifecycleCommand.CommitRescheduledInterval(_, _)              => InterviewStep.CommitReschedule
    case InterviewLifecycleCommand.LookupRescheduleCommitReceipt(_, _)          => InterviewStep.LookupRescheduleCommit
    case InterviewLifecycleCommand.ExpireProposal(_)                            => InterviewStep.ExpireProposal
    case InterviewLifecycleCommand.Notify(_, InterviewParticipant.Candidate, _) => InterviewStep.NotifyCandidate
    case InterviewLifecycleCommand.Notify(_, InterviewParticipant.Recruiter, _) => InterviewStep.NotifyRecruiter
    case InterviewLifecycleCommand.LookupNotificationReceipt(_, InterviewParticipant.Candidate, _) =>
      InterviewStep.LookupNotifyCandidate
    case InterviewLifecycleCommand.LookupNotificationReceipt(_, InterviewParticipant.Recruiter, _) =>
      InterviewStep.LookupNotifyRecruiter
    case InterviewLifecycleCommand.RequireRepair(_) => InterviewStep.RequireRepair
  }

  def event(message: InterviewMessage): Option[InterviewWorkflowEvent] = message.result.flatMap { result =>
    import InterviewResult.*
    import InterviewStep.*
    import InterviewWorkflowEvent.*
    (message.step, result) match {
      case (Reserve, Succeeded)            => Some(ReservationConfirmed)
      case (Reserve, Rejected)             => Some(ReservationRejected)
      case (Reserve, _)                    => Some(ReservationOutcomeUnknown)
      case (LookupReservation, Found)      => Some(ReservationLookupFound)
      case (LookupReservation, Absent)     => Some(ReservationLookupAbsent)
      case (LookupReservation, _)          => Some(ReservationOutcomeUnknown)
      case (CommitHiring, Succeeded)       => Some(StatusCommitted)
      case (CommitHiring, Rejected)        => Some(StatusCommitRejected)
      case (CommitHiring, _)               => Some(StatusCommitOutcomeUnknown)
      case (LookupCommit, Found)           => Some(StatusLookupFound)
      case (LookupCommit, Absent)          => Some(StatusLookupAbsent)
      case (LookupCommit, _)               => Some(StatusCommitOutcomeUnknown)
      case (Release, Succeeded)            => Some(ReservationReleased)
      case (Release, _)                    => Some(ReservationReleaseOutcomeUnknown)
      case (NotifyCandidate, Succeeded)    => Some(NotificationDelivered(InterviewParticipant.Candidate))
      case (NotifyRecruiter, Succeeded)    => Some(NotificationDelivered(InterviewParticipant.Recruiter))
      case (NotifyCandidate, _)            => Some(NotificationOutcomeUnknown(InterviewParticipant.Candidate))
      case (NotifyRecruiter, _)            => Some(NotificationOutcomeUnknown(InterviewParticipant.Recruiter))
      case (LookupNotifyCandidate, Found)  => Some(NotificationLookupFound(InterviewParticipant.Candidate))
      case (LookupNotifyRecruiter, Found)  => Some(NotificationLookupFound(InterviewParticipant.Recruiter))
      case (LookupNotifyCandidate, Absent) => Some(NotificationLookupAbsent(InterviewParticipant.Candidate))
      case (LookupNotifyRecruiter, Absent) => Some(NotificationLookupAbsent(InterviewParticipant.Recruiter))
      case (LookupNotifyCandidate, _)      => Some(NotificationOutcomeUnknown(InterviewParticipant.Candidate))
      case (LookupNotifyRecruiter, _)      => Some(NotificationOutcomeUnknown(InterviewParticipant.Recruiter))
      case _                               => Some(RetryExhausted("provider"))
    }
  }

  /** The lifecycle transition a recorded command result stands for. `at` is when the result is being applied. */
  def lifecycleEvent(
      command: InterviewLifecycleCommand,
      result: InterviewResult,
      at: Instant
  ): Option[InterviewLifecycleEvent] = {
    import InterviewLifecycleCommand as C
    import InterviewLifecycleEvent as E
    import InterviewResult.*
    command match {
      case C.CancelCalendarSlot(_) =>
        Some(result match {
          case Succeeded => E.CancelConfirmed(at)
          // The provider says there is no such reservation: definitive, so repair instead of a lookup loop.
          case Rejected => E.RetryExhausted("unknown_reservation")
          case _        => E.CancelOutcomeUnknown
        })
      case C.LookupCalendarCancellation(_) =>
        Some(result match {
          case Found  => E.CancelLookupConfirmed(at)
          case Absent => E.CancelLookupAbsent
          case _      => E.CancelOutcomeUnknown
        })
      case C.HoldReplacementSlot(_, _) =>
        Some(result match {
          case Succeeded => E.HoldConfirmed
          case Rejected  => E.HoldRejected
          case _         => E.HoldOutcomeUnknown
        })
      case C.LookupReplacementHold(_) =>
        Some(result match {
          case Found  => E.HoldLookupFound
          case Absent => E.HoldLookupAbsent
          case _      => E.HoldOutcomeUnknown
        })
      case C.CommitRescheduledInterval(_, _) =>
        Some(result match {
          case Succeeded => E.SwapCommitted
          case Rejected  => E.SwapRejected
          case _         => E.SwapOutcomeUnknown
        })
      case C.LookupRescheduleCommitReceipt(_, _) =>
        Some(result match {
          case Found  => E.SwapLookupFound
          case Absent => E.SwapLookupAbsent
          case _      => E.SwapOutcomeUnknown
        })
      // Expiry has no external effect, so even an unknown outcome (a crash before the result was recorded) is answered
      // by applying it: the revision decides whether the proposal is still there to expire.
      case C.ExpireProposal(_)            => Some(E.ProposalExpired(at))
      case C.Notify(kind, participant, _) =>
        Option.when(InterviewCommands.isRoundKind(kind))(
          if (result == Succeeded) E.NotificationDelivered(participant) else E.NotificationOutcomeUnknown(participant)
        )
      case C.LookupNotificationReceipt(_, participant, _) =>
        Some(result match {
          case Found  => E.NotificationLookupFound(participant)
          case Absent => E.NotificationLookupAbsent(participant)
          case _      => E.NotificationOutcomeUnknown(participant)
        })
      case C.RequireRepair(_) => None
    }
  }

  /** A retried lookup or unknown outcome that has spent its budget is exhausted instead of retried again. */
  def lifecycleResultEvent(
      command: InterviewLifecycleCommand,
      result: InterviewResult,
      at: Instant,
      attempts: Long,
      maxAttempts: Int
  ): Option[InterviewLifecycleEvent] =
    if (attempts >= maxAttempts && (result == InterviewResult.OutcomeUnknown || result == InterviewResult.Absent))
      Some(InterviewLifecycleEvent.RetryExhausted("provider"))
    else lifecycleEvent(command, result, at)

  /** How a late or out-of-order cancel or reschedule message is failed: through the workflow, or for an informational
    * notification through its own command only.
    */
  /** When a too-early proposal expiry becomes available again: after its due time, plus the distance the consumer's
    * clock is behind clamped to the worker's backoff bounds, so a skewed consumer does not receive it in a hot loop.
    */
  def expiryDeferral(
      due: Instant,
      consumerNow: Instant,
      initialBackoff: scala.concurrent.duration.FiniteDuration,
      maxBackoff: scala.concurrent.duration.FiniteDuration
  ): Instant = {
    val behind = java.time.Duration.between(consumerNow, due).toMillis
    due.plusMillis(behind.max(initialBackoff.toMillis).min(maxBackoff.toMillis))
  }

  enum LifecycleReplayFailure {
    case Workflow(event: InterviewLifecycleEvent)
    case Notification

    /** A proposal's expiry that arrives too early is simply not delivered yet. */
    case Defer
  }

  def lifecycleReplayFailure(
      workflow: Option[InterviewWorkflow],
      command: Option[InterviewWorkflowCommandRecord],
      message: InterviewMessage,
      step: String,
      rejection: InterviewMessageRejection,
      now: Instant
  ): Either[InterviewMessageRejection, LifecycleReplayFailure] =
    (workflow, command) match {
      case (Some(current), Some(stored)) if applicable(current, stored) && identityMatches(current, stored, message) =>
        if (InterviewCommands.isInformational(stored.command)) Right(LifecycleReplayFailure.Notification)
        else if (rejection == InterviewMessageRejection.Future && InterviewCommands.isExpiry(stored.command))
          Right(LifecycleReplayFailure.Defer)
        else {
          val event = InterviewCommands.giveUpEvent(stored.command, step, now)
          InterviewLifecyclePolicy
            .decide(current, current.revision, event)
            .as(LifecycleReplayFailure.Workflow(event))
            .left
            .map(_ => rejection)
        }
      case _ => Left(rejection)
    }

}
