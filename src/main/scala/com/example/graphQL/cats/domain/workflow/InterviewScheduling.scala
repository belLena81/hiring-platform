package com.example.graphQL.cats.domain.workflow

import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import java.time.Instant
import java.util.UUID

final case class InterviewWorkflowId(value: UUID)

final case class InterviewInterval(startsAt: Instant, endsAt: Instant)

object InterviewInterval {

  /** Stored times have millisecond precision, so every comparison and fingerprint uses the truncated instant. */
  def canonical(instant: Instant): Instant = instant.truncatedTo(java.time.temporal.ChronoUnit.MILLIS)

  def validate(startsAt: Instant, endsAt: Instant, now: Instant): Either[InterviewWorkflowError, InterviewInterval] = {
    val canonicalStart = canonical(startsAt)
    val canonicalEnd = canonical(endsAt)
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
  case InterviewAlreadyStarted
  case InterviewAlreadyCancelled
  case InterviewNotSettled
  case RescheduleIntervalUnchanged
  case ProposalAlreadyOpen
  case NoOpenProposal
  case ProposalExpired
  case ProposalTtlOutOfRange
  case NoRescheduleRequest
}

enum InterviewWorkflowPhase {
  case ReservationPending
  case StatusCommitPending
  case CompensationPending
  case NotificationsPending
  case Completed
  case RepairRequired
  case ProposalPending
  case CancelPending
  case CancelNotificationsPending
  case Cancelled
  case RescheduleHoldPending
  case RescheduleSwapPending
  case RescheduleCancelOldPending
  case RescheduleNotificationsPending
  case RescheduleCompensationPending
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
  case RetryExhausted(step: InterviewFailureCode)
}

enum InterviewWorkflowCommand {
  case ReserveCalendarSlot(idempotencyKey: String)
  case LookupCalendarReservation(workflowId: InterviewWorkflowId)
  case CommitAcceptedToInterview(expectedStatus: ApplicationStatus)
  case LookupStatusCommitReceipt(workflowId: InterviewWorkflowId)
  case ReleaseCalendarSlot(idempotencyKey: String)
  case Notify(participant: InterviewParticipant, idempotencyKey: String)
  case LookupNotificationReceipt(participant: InterviewParticipant, idempotencyKey: String)
  case RequireRepair(reason: InterviewRepairReason)
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
    initiatedBy: UserId,
    generation: Int = 0,
    pendingInterval: Option[InterviewInterval] = None,
    cancelledAt: Option[Instant] = None,
    proposal: Option[InterviewRescheduleProposal] = None,
    rescheduleRequestedAt: Option[Instant] = None,
    /** The cancel or reschedule phase that exhausted its retries; present only while `RepairRequired` after such a
      * failure, so Admin repair can resume that step by lookup. Scheduling failures leave it empty.
      */
    repairOrigin: Option[InterviewWorkflowPhase] = None,
    /** Reservation generations consumed by failed replacement attempts above the live one. A compensated attempt leaves
      * its (cancelled) reservation stored under its key, so the next attempt must use a fresh generation. Between a
      * committed swap and the old slot's cancellation it is the gap that locates the old reservation.
      */
    skippedGenerations: Int = 0
) {

  /** Generation of the replacement hold the workflow is attempting (hold, swap and compensation phases). */
  def replacementGeneration: Int = generation + 1 + skippedGenerations

  /** Generation of the reservation a committed swap retired (valid while the old slot is being cancelled). */
  def retiredGeneration: Int = generation - 1 - skippedGenerations
}

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

  /** The original generation keeps the scheduling key; each replacement hold gets its own generation-qualified key. */
  def reservationKey(id: InterviewWorkflowId, generation: Int): String =
    if (generation == 0) s"${id.value}:reserve" else s"${id.value}:reserve:g$generation"

  def cancellationKey(id: InterviewWorkflowId, generation: Int): String = s"${id.value}:cancel:g$generation"

  def initialCommand(workflow: InterviewWorkflow): InterviewWorkflowCommand =
    InterviewWorkflowCommand.ReserveCalendarSlot(reservationKey(workflow.id, 0))

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

  /** One stale-revision rule for every interview decision; `Long.MaxValue` could not be incremented. */
  def revisionMatches(workflow: InterviewWorkflow, expectedRevision: Long): Boolean =
    workflow.revision == expectedRevision && expectedRevision != Long.MaxValue

  /** The stable idempotency key of a scheduling notification. */
  def notificationKey(id: InterviewWorkflowId, participant: InterviewParticipant): String =
    s"${id.value}:notify:$participant"

  /** Pure policy: the caller persists the returned state and command intent in one transaction. */
  def decide(
      workflow: InterviewWorkflow,
      expectedRevision: Long,
      event: InterviewWorkflowEvent
  ): Either[InterviewWorkflowError, InterviewWorkflowDecision] =
    if (!revisionMatches(workflow, expectedRevision)) Left(InterviewWorkflowError.StaleRevision)
    else
      transition(workflow, event).map { case (phase, notified, commands) =>
        InterviewWorkflowDecision(
          workflow.copy(revision = workflow.revision + 1L, phase = phase, notified = notified),
          commands
        )
      }

  private type Step = (InterviewWorkflowPhase, Set[InterviewParticipant], List[InterviewWorkflowCommand])

  private def transition(
      workflow: InterviewWorkflow,
      event: InterviewWorkflowEvent
  ): Either[InterviewWorkflowError, Step] = {
    import InterviewWorkflowPhase.*
    import InterviewWorkflowEvent as Event
    import InterviewWorkflowCommand as Command
    val releaseKey = s"${workflow.id.value}:release"
    def notifyKey(participant: InterviewParticipant) = notificationKey(workflow.id, participant)
    def move(phase: InterviewWorkflowPhase, commands: Command*): Either[InterviewWorkflowError, Step] =
      Right((phase, workflow.notified, commands.toList))
    def delivered(participant: InterviewParticipant): Either[InterviewWorkflowError, Step] = {
      val all = workflow.notified + participant
      Right((if (all.size == 2) Completed else NotificationsPending, all, Nil))
    }
    val commit = Command.CommitAcceptedToInterview(ApplicationStatus.Accepted)
    val notifyBoth = InterviewParticipant.values.toList.map(p => Command.Notify(p, notifyKey(p)))

    (workflow.phase, event) match {
      case (ReservationPending, Event.ReservationConfirmed | Event.ReservationLookupFound) =>
        move(StatusCommitPending, commit)
      case (ReservationPending, Event.ReservationOutcomeUnknown) =>
        move(ReservationPending, Command.LookupCalendarReservation(workflow.id))
      case (ReservationPending, Event.ReservationLookupAbsent) =>
        move(ReservationPending, Command.ReserveCalendarSlot(reservationKey(workflow.id, 0)))
      case (ReservationPending, Event.ReservationRejected) =>
        move(RepairRequired, Command.RequireRepair(InterviewRepairReason.CalendarReservationRejected))
      case (ReservationPending | StatusCommitPending | NotificationsPending, Event.RetryExhausted(step)) =>
        move(RepairRequired, Command.RequireRepair(InterviewRepairReason.RetryExhausted(step)))
      case (StatusCommitPending, Event.StatusCommitted | Event.StatusLookupFound) =>
        move(NotificationsPending, notifyBoth*)
      case (StatusCommitPending, Event.StatusCommitOutcomeUnknown) =>
        move(StatusCommitPending, Command.LookupStatusCommitReceipt(workflow.id))
      case (StatusCommitPending, Event.StatusLookupAbsent) =>
        move(StatusCommitPending, commit)
      case (StatusCommitPending, Event.StatusCommitRejected) =>
        move(CompensationPending, Command.ReleaseCalendarSlot(releaseKey))
      case (CompensationPending, Event.ReservationReleaseOutcomeUnknown) =>
        move(CompensationPending, Command.ReleaseCalendarSlot(releaseKey))
      case (CompensationPending, Event.ReservationReleased) =>
        move(RepairRequired, Command.RequireRepair(InterviewRepairReason.StatusNotCommitted))
      case (CompensationPending, Event.RetryExhausted(step)) =>
        move(RepairRequired, Command.RequireRepair(InterviewRepairReason.CompensationFailed(step)))
      case (NotificationsPending, Event.NotificationDelivered(p))      => delivered(p)
      case (NotificationsPending, Event.NotificationLookupFound(p))    => delivered(p)
      case (NotificationsPending, Event.NotificationOutcomeUnknown(p)) =>
        move(NotificationsPending, Command.LookupNotificationReceipt(p, notifyKey(p)))
      case (NotificationsPending, Event.NotificationLookupAbsent(p)) =>
        move(NotificationsPending, Command.Notify(p, notifyKey(p)))
      case _ => Left(InterviewWorkflowError.InvalidTransition)
    }
  }
}
