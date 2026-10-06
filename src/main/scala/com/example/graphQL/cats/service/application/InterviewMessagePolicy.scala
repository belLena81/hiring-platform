package com.example.graphQL.cats.service.application

import com.example.graphQL.cats.domain.workflow.*
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
  case Timely, Expired, Future
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
      replayWindow: FiniteDuration
  ): InterviewReplayDisposition =
    if (message.occurredAt.isAfter(now)) InterviewReplayDisposition.Future
    else if (message.occurredAt.isBefore(now.minusMillis(replayWindow.toMillis))) InterviewReplayDisposition.Expired
    else InterviewReplayDisposition.Timely

  private def applicable(workflow: InterviewWorkflow, command: InterviewWorkflowCommandRecord): Boolean =
    command.state != InterviewWorkflowCommandState.Superseded &&
      InterviewWorkflow.commandIsApplicable(workflow, command.revision, command.command)

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
  ): Either[InterviewMessageRejection, (InterviewWorkflow, List[InterviewWorkflowCommand])] =
    (workflow, command) match {
      // Expired admission intentionally does not use the timely acknowledgment or result-equality shortcuts.
      case (Some(current), Some(stored)) if applicable(current, stored) && identityMatches(current, stored, message) =>
        InterviewWorkflow
          .decide(current, current.revision, InterviewWorkflowEvent.RetryExhausted("replay_expired"))
          .left
          .map(_ => InterviewMessageRejection.Expired)
      case _ => Left(InterviewMessageRejection.Expired)
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

  def step(command: InterviewWorkflowCommand): InterviewStep = command match {
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
    case InterviewWorkflowCommand.RequireRepair(_) => InterviewStep.RequireRepair
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
}
