package com.example.graphQL.cats.domain.workflow

import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import java.time.Instant
import java.util.UUID

final case class InterviewWorkflowId(value: UUID)

final case class InterviewInterval(startsAt: Instant, endsAt: Instant)

object InterviewInterval {
  def validate(startsAt: Instant, endsAt: Instant, now: Instant): Either[InterviewWorkflowError, InterviewInterval] = {
    val canonicalStart = startsAt.truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
    val canonicalEnd = endsAt.truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
    if (!canonicalStart.isAfter(now)) Left(InterviewWorkflowError.StartMustBeInFuture)
    else if (!canonicalEnd.isAfter(canonicalStart)) Left(InterviewWorkflowError.EndMustFollowStart)
    else Right(InterviewInterval(canonicalStart, canonicalEnd))
  }
}

enum InterviewWorkflowError {
  case StartMustBeInFuture
  case EndMustFollowStart
  case ApplicationMustBeAccepted
  case StaleRevision
  case InvalidTransition
}

enum InterviewWorkflowPhase {
  case ReservationPending
  case StatusCommitPending
  case CompensationPending
  case NotificationsPending
  case Completed
  case RepairRequired
}

enum InterviewParticipant {
  case Candidate, Recruiter
}

enum InterviewWorkflowEvent {
  case ReservationConfirmed
  case ReservationRejected
  case ReservationOutcomeUnknown
  case ReservationLookupFound
  case ReservationLookupAbsent
  case StatusCommitted
  case StatusCommitRejected
  case StatusCommitOutcomeUnknown
  case StatusLookupFound
  case StatusLookupAbsent
  case ReservationReleased
  case ReservationReleaseOutcomeUnknown
  case NotificationDelivered(participant: InterviewParticipant)
  case NotificationOutcomeUnknown(participant: InterviewParticipant)
  case NotificationLookupFound(participant: InterviewParticipant)
  case NotificationLookupAbsent(participant: InterviewParticipant)
  case RetryExhausted(step: String)
}

enum InterviewWorkflowCommand {
  case ReserveCalendarSlot(idempotencyKey: String)
  case LookupCalendarReservation(workflowId: InterviewWorkflowId)
  case CommitAcceptedToInterview(expectedStatus: ApplicationStatus)
  case LookupStatusCommitReceipt(workflowId: InterviewWorkflowId)
  case ReleaseCalendarSlot(idempotencyKey: String)
  case Notify(participant: InterviewParticipant, idempotencyKey: String)
  case LookupNotificationReceipt(participant: InterviewParticipant, idempotencyKey: String)
  case RequireRepair(reason: String)
}

final case class InterviewWorkflow(
    id: InterviewWorkflowId,
    applicationId: ApplicationId,
    candidateId: UserId,
    recruiterId: UserId,
    interval: InterviewInterval,
    preCommitDeadline: Instant,
    idempotencyKey: UUID,
    revision: Long,
    phase: InterviewWorkflowPhase,
    notified: Set[InterviewParticipant],
    initiatedBy: UserId
)

final case class InterviewWorkflowDecision(workflow: InterviewWorkflow, commands: List[InterviewWorkflowCommand])

object InterviewWorkflow {

  /** Pure command fencing policy shared by the worker and persistence adapter. */
  def commandIsApplicable(
      workflow: InterviewWorkflow,
      commandRevision: Long,
      command: InterviewWorkflowCommand
  ): Boolean = {
    val notification = command match {
      case InterviewWorkflowCommand.Notify(_, _) | InterviewWorkflowCommand.LookupNotificationReceipt(_, _) => true
      case _                                                                                                => false
    }
    val expectedPhase = command match {
      case InterviewWorkflowCommand.ReserveCalendarSlot(_) | InterviewWorkflowCommand.LookupCalendarReservation(_) =>
        InterviewWorkflowPhase.ReservationPending
      case InterviewWorkflowCommand.CommitAcceptedToInterview(_) |
          InterviewWorkflowCommand.LookupStatusCommitReceipt(_) =>
        InterviewWorkflowPhase.StatusCommitPending
      case InterviewWorkflowCommand.ReleaseCalendarSlot(_) => InterviewWorkflowPhase.CompensationPending
      case InterviewWorkflowCommand.Notify(_, _) | InterviewWorkflowCommand.LookupNotificationReceipt(_, _) =>
        InterviewWorkflowPhase.NotificationsPending
      case _ => InterviewWorkflowPhase.RepairRequired
    }
    workflow.phase == expectedPhase &&
    (workflow.revision == commandRevision || (notification && commandRevision <= workflow.revision))
  }

  def initialCommand(workflow: InterviewWorkflow): InterviewWorkflowCommand =
    InterviewWorkflowCommand.ReserveCalendarSlot(s"${workflow.id.value}:reserve")

  def create(
      id: InterviewWorkflowId,
      applicationId: ApplicationId,
      candidateId: UserId,
      recruiterId: UserId,
      interval: InterviewInterval,
      preCommitDeadline: Instant,
      idempotencyKey: UUID,
      applicationStatus: ApplicationStatus,
      initiatedBy: Option[UserId] = None
  ): Either[InterviewWorkflowError, InterviewWorkflow] =
    Either.cond(
      applicationStatus == ApplicationStatus.Accepted,
      InterviewWorkflow(
        id,
        applicationId,
        candidateId,
        recruiterId,
        interval,
        preCommitDeadline,
        idempotencyKey,
        revision = 0L,
        phase = InterviewWorkflowPhase.ReservationPending,
        notified = Set.empty,
        initiatedBy = initiatedBy.getOrElse(recruiterId)
      ),
      InterviewWorkflowError.ApplicationMustBeAccepted
    )

  /** Pure policy: the caller persists the returned state and command intent in one transaction. */
  def decide(
      workflow: InterviewWorkflow,
      expectedRevision: Long,
      event: InterviewWorkflowEvent
  ): Either[InterviewWorkflowError, InterviewWorkflowDecision] =
    if (workflow.revision != expectedRevision || expectedRevision == Long.MaxValue)
      Left(InterviewWorkflowError.StaleRevision)
    else
      transition(workflow, event).map { case (phase, notified, commands) =>
        InterviewWorkflowDecision(
          workflow.copy(revision = workflow.revision + 1L, phase = phase, notified = notified),
          commands
        )
      }

  private def transition(
      workflow: InterviewWorkflow,
      event: InterviewWorkflowEvent
  ): Either[
    InterviewWorkflowError,
    (InterviewWorkflowPhase, Set[InterviewParticipant], List[InterviewWorkflowCommand])
  ] = {
    val releaseKey = s"${workflow.id.value}:release"
    val notifyKey = (participant: InterviewParticipant) => s"${workflow.id.value}:notify:${participant.toString}"
    (workflow.phase, event) match {
      case (InterviewWorkflowPhase.ReservationPending, InterviewWorkflowEvent.ReservationConfirmed) =>
        Right(
          (
            InterviewWorkflowPhase.StatusCommitPending,
            workflow.notified,
            List(InterviewWorkflowCommand.CommitAcceptedToInterview(ApplicationStatus.Accepted))
          )
        )
      case (InterviewWorkflowPhase.ReservationPending, InterviewWorkflowEvent.ReservationOutcomeUnknown) =>
        Right(
          (
            InterviewWorkflowPhase.ReservationPending,
            workflow.notified,
            List(InterviewWorkflowCommand.LookupCalendarReservation(workflow.id))
          )
        )
      case (InterviewWorkflowPhase.ReservationPending, InterviewWorkflowEvent.ReservationLookupFound) =>
        Right(
          (
            InterviewWorkflowPhase.StatusCommitPending,
            workflow.notified,
            List(InterviewWorkflowCommand.CommitAcceptedToInterview(ApplicationStatus.Accepted))
          )
        )
      case (InterviewWorkflowPhase.ReservationPending, InterviewWorkflowEvent.ReservationLookupAbsent) =>
        Right(
          (
            InterviewWorkflowPhase.ReservationPending,
            workflow.notified,
            List(InterviewWorkflowCommand.ReserveCalendarSlot(s"${workflow.id.value}:reserve"))
          )
        )
      case (InterviewWorkflowPhase.ReservationPending, InterviewWorkflowEvent.ReservationRejected) =>
        Right(
          (
            InterviewWorkflowPhase.RepairRequired,
            workflow.notified,
            List(InterviewWorkflowCommand.RequireRepair("calendar reservation rejected"))
          )
        )
      case (InterviewWorkflowPhase.ReservationPending, InterviewWorkflowEvent.RetryExhausted(step)) =>
        Right(
          (
            InterviewWorkflowPhase.RepairRequired,
            workflow.notified,
            List(InterviewWorkflowCommand.RequireRepair(s"retry exhausted: $step"))
          )
        )
      case (InterviewWorkflowPhase.StatusCommitPending, InterviewWorkflowEvent.StatusCommitted) =>
        Right(
          (
            InterviewWorkflowPhase.NotificationsPending,
            workflow.notified,
            List(
              InterviewWorkflowCommand
                .Notify(InterviewParticipant.Candidate, notifyKey(InterviewParticipant.Candidate)),
              InterviewWorkflowCommand.Notify(InterviewParticipant.Recruiter, notifyKey(InterviewParticipant.Recruiter))
            )
          )
        )
      case (InterviewWorkflowPhase.StatusCommitPending, InterviewWorkflowEvent.StatusCommitOutcomeUnknown) =>
        Right(
          (
            InterviewWorkflowPhase.StatusCommitPending,
            workflow.notified,
            List(InterviewWorkflowCommand.LookupStatusCommitReceipt(workflow.id))
          )
        )
      case (InterviewWorkflowPhase.StatusCommitPending, InterviewWorkflowEvent.StatusLookupFound) =>
        Right(
          (
            InterviewWorkflowPhase.NotificationsPending,
            workflow.notified,
            List(
              InterviewWorkflowCommand
                .Notify(InterviewParticipant.Candidate, notifyKey(InterviewParticipant.Candidate)),
              InterviewWorkflowCommand.Notify(InterviewParticipant.Recruiter, notifyKey(InterviewParticipant.Recruiter))
            )
          )
        )
      case (InterviewWorkflowPhase.StatusCommitPending, InterviewWorkflowEvent.StatusLookupAbsent) =>
        Right(
          (
            InterviewWorkflowPhase.StatusCommitPending,
            workflow.notified,
            List(InterviewWorkflowCommand.CommitAcceptedToInterview(ApplicationStatus.Accepted))
          )
        )
      case (InterviewWorkflowPhase.StatusCommitPending, InterviewWorkflowEvent.StatusCommitRejected) =>
        Right(
          (
            InterviewWorkflowPhase.CompensationPending,
            workflow.notified,
            List(InterviewWorkflowCommand.ReleaseCalendarSlot(releaseKey))
          )
        )
      case (InterviewWorkflowPhase.StatusCommitPending, InterviewWorkflowEvent.RetryExhausted(step)) =>
        Right(
          (
            InterviewWorkflowPhase.RepairRequired,
            workflow.notified,
            List(InterviewWorkflowCommand.RequireRepair(s"retry exhausted: $step"))
          )
        )
      case (InterviewWorkflowPhase.CompensationPending, InterviewWorkflowEvent.ReservationReleaseOutcomeUnknown) =>
        Right(
          (
            InterviewWorkflowPhase.CompensationPending,
            workflow.notified,
            List(InterviewWorkflowCommand.ReleaseCalendarSlot(releaseKey))
          )
        )
      case (InterviewWorkflowPhase.CompensationPending, InterviewWorkflowEvent.ReservationReleased) =>
        Right(
          (
            InterviewWorkflowPhase.RepairRequired,
            workflow.notified,
            List(InterviewWorkflowCommand.RequireRepair("application status was not committed"))
          )
        )
      case (InterviewWorkflowPhase.CompensationPending, InterviewWorkflowEvent.RetryExhausted(step)) =>
        Right(
          (
            InterviewWorkflowPhase.RepairRequired,
            workflow.notified,
            List(InterviewWorkflowCommand.RequireRepair(s"compensation failed: $step"))
          )
        )
      case (InterviewWorkflowPhase.NotificationsPending, InterviewWorkflowEvent.NotificationDelivered(participant)) =>
        val delivered = workflow.notified + participant
        val next =
          if (delivered.size == 2) InterviewWorkflowPhase.Completed else InterviewWorkflowPhase.NotificationsPending
        Right((next, delivered, Nil))
      case (
            InterviewWorkflowPhase.NotificationsPending,
            InterviewWorkflowEvent.NotificationOutcomeUnknown(participant)
          ) =>
        Right(
          (
            InterviewWorkflowPhase.NotificationsPending,
            workflow.notified,
            List(InterviewWorkflowCommand.LookupNotificationReceipt(participant, notifyKey(participant)))
          )
        )
      case (InterviewWorkflowPhase.NotificationsPending, InterviewWorkflowEvent.NotificationLookupFound(participant)) =>
        val delivered = workflow.notified + participant
        val next =
          if (delivered.size == 2) InterviewWorkflowPhase.Completed else InterviewWorkflowPhase.NotificationsPending
        Right((next, delivered, Nil))
      case (
            InterviewWorkflowPhase.NotificationsPending,
            InterviewWorkflowEvent.NotificationLookupAbsent(participant)
          ) =>
        Right(
          (
            InterviewWorkflowPhase.NotificationsPending,
            workflow.notified,
            List(InterviewWorkflowCommand.Notify(participant, notifyKey(participant)))
          )
        )
      case (InterviewWorkflowPhase.NotificationsPending, InterviewWorkflowEvent.RetryExhausted(step)) =>
        Right(
          (
            InterviewWorkflowPhase.RepairRequired,
            workflow.notified,
            List(InterviewWorkflowCommand.RequireRepair(s"retry exhausted: $step"))
          )
        )
      case _ => Left(InterviewWorkflowError.InvalidTransition)
    }
  }
}
