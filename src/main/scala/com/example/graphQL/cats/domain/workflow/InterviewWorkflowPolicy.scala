package com.example.graphQL.cats.domain.workflow

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import java.util.UUID

enum InterviewAdvanceCause {
  case ResultReceipt(messageId: String)
  case ReplayExpired(messageId: UUID)
  case AdminRepair(requestKey: UUID, actorId: UserId)
  case PublicationExhausted(stepId: String)
  case ExecutionExhausted(stepId: String)

  def receiptIdentity: String = this match {
    case ResultReceipt(id)          => id
    case ReplayExpired(id)          => s"expired:$id"
    case AdminRepair(key, _)        => s"repair:$key"
    case PublicationExhausted(step) => s"publication-exhausted:$step"
    case ExecutionExhausted(step)   => s"execution-exhausted:$step"
  }

  def resetsAttempts: Boolean = this match {
    case AdminRepair(_, _) => true
    case _                 => false
  }

  def auditActor: Option[UserId] = this match {
    case AdminRepair(_, actor) => Some(actor)
    case _                     => None
  }
}

enum InterviewPublicationDisposition {
  case Send, Supersede, RequireRepair
}

/** Immutable decisions; persistence owns observation, authorization and atomic application. */
object InterviewWorkflowPolicy {
  def repair(
      workflow: InterviewWorkflow,
      expectedRevision: Long,
      hiringCommitted: Boolean
  ): Either[InterviewWorkflowError, InterviewWorkflowDecision] =
    if (workflow.revision != expectedRevision || expectedRevision == Long.MaxValue)
      Left(InterviewWorkflowError.StaleRevision)
    else if (workflow.phase != InterviewWorkflowPhase.RepairRequired)
      Left(InterviewWorkflowError.InvalidTransition)
    else {
      val phase =
        if (hiringCommitted) InterviewWorkflowPhase.NotificationsPending else InterviewWorkflowPhase.ReservationPending
      val commands =
        if (hiringCommitted)
          InterviewParticipant.values.toList
            .filterNot(workflow.notified)
            .map(participant =>
              InterviewWorkflowCommand
                .LookupNotificationReceipt(participant, s"${workflow.id.value}:notify:$participant")
            )
        else List(InterviewWorkflowCommand.LookupCalendarReservation(workflow.id))
      Right(InterviewWorkflowDecision(workflow.copy(revision = expectedRevision + 1L, phase = phase), commands))
    }

  def publicationDisposition(
      workflow: InterviewWorkflow,
      commandRevision: Long,
      command: InterviewWorkflowCommand,
      hasResult: Boolean,
      resultApplied: Boolean,
      publicationAttempts: Int,
      maxAttempts: Int
  ): InterviewPublicationDisposition = {
    val participantDone = command match {
      case InterviewWorkflowCommand.Notify(participant, _)                    => workflow.notified.contains(participant)
      case InterviewWorkflowCommand.LookupNotificationReceipt(participant, _) => workflow.notified.contains(participant)
      case _                                                                  => false
    }
    val administrative = command match {
      case InterviewWorkflowCommand.RequireRepair(_) => true
      case _                                         => false
    }
    if (
      administrative || !InterviewWorkflow.commandIsApplicable(workflow, commandRevision, command) ||
      ((hasResult && resultApplied) || participantDone)
    ) InterviewPublicationDisposition.Supersede
    else if (publicationAttempts > maxAttempts) InterviewPublicationDisposition.RequireRepair
    else InterviewPublicationDisposition.Send
  }

  def backoffMillis(attempt: Long, initialMillis: Long, maximumMillis: Long): Long =
    (0L until math.min(math.max(attempt - 1L, 0L), 63L)).foldLeft(math.min(initialMillis, maximumMillis)) {
      (delay, _) =>
        if (delay >= maximumMillis - delay) maximumMillis else delay * 2L
    }
}
