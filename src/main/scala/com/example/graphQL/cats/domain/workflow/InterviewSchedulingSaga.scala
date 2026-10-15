package com.example.graphQL.cats.domain.workflow

import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.hiring.saga.*

/** The interview scheduling workflow as a saga definition: reserve a calendar slot, commit the Accepted -> Interview
  * status (compensated by releasing the slot), then notify both participants. Every phase name, command and idempotency
  * key is the one already stored and published; only the decision rules moved into the shared engine.
  */
object InterviewSchedulingSaga {
  import InterviewWorkflowCommand as Command
  import InterviewWorkflowEvent as Event
  import InterviewWorkflowPhase as Phase

  type Step = SagaStep[InterviewWorkflow, InterviewWorkflowPhase, InterviewWorkflowCommand]
  type Definition = SagaDefinition[
    InterviewWorkflow,
    InterviewWorkflowPhase,
    InterviewWorkflowEvent,
    InterviewWorkflowCommand,
    InterviewFailureCode,
    InterviewRepairReason,
    InterviewWorkflowError
  ]
  private type Node = PhaseNode[
    InterviewWorkflow,
    InterviewWorkflowPhase,
    InterviewWorkflowCommand,
    InterviewRepairReason,
    InterviewWorkflowEvent,
    InterviewWorkflowError
  ]

  /** Step names; an outcome is only accepted by the phase whose step carries its name. */
  val ReserveCalendarSlotStep = "reserveCalendarSlot"
  val CommitAcceptedToInterviewStep = "commitAcceptedToInterview"
  val ReleaseCalendarSlotStep = "releaseCalendarSlot"
  val NotifyParticipantsStep = "notifyParticipants"

  private def releaseKey(w: InterviewWorkflow): String = s"${w.id.value}:release"
  private def commit: InterviewWorkflowCommand = Command.CommitAcceptedToInterview(ApplicationStatus.Accepted)
  private def notifyBoth(w: InterviewWorkflow): List[InterviewWorkflowCommand] =
    InterviewParticipant.values.toList.map(p => Command.Notify(p, InterviewWorkflow.notificationKey(w.id, p)))

  val releaseCalendarSlot: Step = SagaStep(
    ReleaseCalendarSlotStep,
    Phase.CompensationPending,
    releaseKey,
    w => List(Command.ReleaseCalendarSlot(releaseKey(w))),
    w => List(Command.ReleaseCalendarSlot(releaseKey(w))),
    StepUndo.Irreversible("a released slot is not reclaimed; Admin repair reconciles the workflow"),
    RetryBudget("interviewWorkflow.maxAttempts")
  )

  val reserveCalendarSlot: Step = SagaStep(
    ReserveCalendarSlotStep,
    Phase.ReservationPending,
    w => InterviewWorkflow.reservationKey(w.id, 0),
    w => List(Command.ReserveCalendarSlot(InterviewWorkflow.reservationKey(w.id, 0))),
    w => List(Command.LookupCalendarReservation(w.id)),
    StepUndo.Irreversible("a rejected reservation holds nothing to release"),
    RetryBudget("interviewWorkflow.maxAttempts")
  )

  // The `:commit` and `:notify` keys below are catalogue-only: they are never emitted and must not be used as
  // idempotency keys. The commit has no provider key (the status change is guarded by the revision and a receipt), and
  // each notification uses its per-participant `InterviewWorkflow.notificationKey`.
  val commitAcceptedToInterview: Step = SagaStep(
    CommitAcceptedToInterviewStep,
    Phase.StatusCommitPending,
    w => s"${w.id.value}:commit",
    _ => List(commit),
    w => List(Command.LookupStatusCommitReceipt(w.id)),
    StepUndo.Compensate(releaseCalendarSlot),
    RetryBudget("interviewWorkflow.maxAttempts")
  )

  /** Deliveries are per participant triggers, so this step has no confirmation route of its own. */
  val notifyParticipants: Step = SagaStep(
    NotifyParticipantsStep,
    Phase.NotificationsPending,
    w => s"${w.id.value}:notify",
    notifyBoth,
    _ => Nil,
    StepUndo.Irreversible("a delivered notification cannot be recalled"),
    RetryBudget("interviewWorkflow.maxAttempts")
  )

  private val nodes: Map[InterviewWorkflowPhase, Node] = Map(
    Phase.ReservationPending -> PhaseNode(
      reserveCalendarSlot,
      LookupSupport.Full,
      Some((_, _) => Right(SagaMove.advance(Phase.StatusCommitPending, List(commit)))),
      Rejection.ToRepair(InterviewRepairReason.CalendarReservationRejected)
    ),
    Phase.StatusCommitPending -> PhaseNode(
      commitAcceptedToInterview,
      LookupSupport.Full,
      Some((w, _) => Right(SagaMove.advance(Phase.NotificationsPending, notifyBoth(w)))),
      Rejection.Compensate()
    ),
    Phase.CompensationPending -> PhaseNode(
      releaseCalendarSlot,
      LookupSupport.UnknownOnly,
      Some((_, _) => Right(SagaMove.Repair(InterviewRepairReason.StatusNotCommitted))),
      Rejection.Unsupported()
    ),
    Phase.NotificationsPending -> PhaseNode(
      notifyParticipants,
      LookupSupport.Unsupported,
      None,
      Rejection.Unsupported()
    )
  )

  private type Move =
    SagaMove[InterviewWorkflow, InterviewWorkflowPhase, InterviewWorkflowCommand, InterviewRepairReason]

  private def delivered(
      w: InterviewWorkflow,
      participant: InterviewParticipant
  ): Either[SagaError[InterviewWorkflowError], Move] = {
    val all = w.notified + participant
    Right(
      SagaMove.Advance[InterviewWorkflow, InterviewWorkflowPhase, InterviewWorkflowCommand, InterviewRepairReason](
        if (all.size == 2) Phase.Completed else Phase.NotificationsPending,
        Nil,
        _.copy(notified = all)
      )
    )
  }

  private def notificationTrigger(
      w: InterviewWorkflow,
      event: InterviewWorkflowEvent
  ): Either[SagaError[InterviewWorkflowError], SagaMove[
    InterviewWorkflow,
    InterviewWorkflowPhase,
    InterviewWorkflowCommand,
    InterviewRepairReason
  ]] = {
    def key(p: InterviewParticipant) = InterviewWorkflow.notificationKey(w.id, p)
    if (w.phase != Phase.NotificationsPending) Left(SagaError.InvalidTransition)
    else
      event match {
        case Event.NotificationDelivered(p)      => delivered(w, p)
        case Event.NotificationLookupFound(p)    => delivered(w, p)
        case Event.NotificationOutcomeUnknown(p) =>
          Right(SagaMove.advance(Phase.NotificationsPending, List(Command.LookupNotificationReceipt(p, key(p)))))
        case Event.NotificationLookupAbsent(p) =>
          Right(SagaMove.advance(Phase.NotificationsPending, List(Command.Notify(p, key(p)))))
        case _ => Left(SagaError.InvalidTransition)
      }
  }

  private object access extends SagaInstanceAccess[InterviewWorkflow, InterviewWorkflowPhase] {
    def revision(instance: InterviewWorkflow): Long = instance.revision
    def phase(instance: InterviewWorkflow): InterviewWorkflowPhase = instance.phase
    def repairOrigin(instance: InterviewWorkflow): Option[InterviewWorkflowPhase] = instance.repairOrigin
    def withRevisionAndPhase(instance: InterviewWorkflow, revision: Long, phase: InterviewWorkflowPhase) =
      instance.copy(revision = revision, phase = phase)
    def withRepairOrigin(instance: InterviewWorkflow, origin: Option[InterviewWorkflowPhase]) =
      instance.copy(repairOrigin = origin)
  }

  /** Scheduling failures leave `repairOrigin` empty; Admin repair of a scheduling workflow is a separate policy. */
  val definition: Definition = SagaDefinition(
    name = "interviewScheduling",
    access = access,
    phases = List(
      Phase.ReservationPending,
      Phase.StatusCommitPending,
      Phase.CompensationPending,
      Phase.NotificationsPending,
      Phase.Completed,
      Phase.RepairRequired
    ),
    terminal = Set(Phase.Completed),
    eventDriven = Set.empty,
    repairPhase = Phase.RepairRequired,
    nodes = nodes,
    repair = RepairPolicy(
      recordsOrigin = false,
      requireRepair = Command.RequireRepair(_),
      exhausted = InterviewRepairReason.RetryExhausted(_),
      compensationFailed = InterviewRepairReason.CompensationFailed(_)
    ),
    triggers = notificationTrigger
  )

  /** Maps a scheduling event onto the engine input; the retry step is the one the current phase runs. */
  def input(
      workflow: InterviewWorkflow,
      event: InterviewWorkflowEvent
  ): SagaInput[InterviewWorkflowEvent, InterviewFailureCode] = {
    def outcome(step: String, o: StepOutcome[InterviewFailureCode]) = SagaInput.Outcome(step, o, None)
    event match {
      case Event.ReservationConfirmed             => outcome(ReserveCalendarSlotStep, StepOutcome.Confirmed)
      case Event.ReservationRejected              => outcome(ReserveCalendarSlotStep, StepOutcome.Rejected)
      case Event.ReservationOutcomeUnknown        => outcome(ReserveCalendarSlotStep, StepOutcome.OutcomeUnknown)
      case Event.ReservationLookupFound           => outcome(ReserveCalendarSlotStep, StepOutcome.LookupFound)
      case Event.ReservationLookupAbsent          => outcome(ReserveCalendarSlotStep, StepOutcome.LookupAbsent)
      case Event.StatusCommitted                  => outcome(CommitAcceptedToInterviewStep, StepOutcome.Confirmed)
      case Event.StatusCommitRejected             => outcome(CommitAcceptedToInterviewStep, StepOutcome.Rejected)
      case Event.StatusCommitOutcomeUnknown       => outcome(CommitAcceptedToInterviewStep, StepOutcome.OutcomeUnknown)
      case Event.StatusLookupFound                => outcome(CommitAcceptedToInterviewStep, StepOutcome.LookupFound)
      case Event.StatusLookupAbsent               => outcome(CommitAcceptedToInterviewStep, StepOutcome.LookupAbsent)
      case Event.ReservationReleased              => outcome(ReleaseCalendarSlotStep, StepOutcome.Confirmed)
      case Event.ReservationReleaseOutcomeUnknown => outcome(ReleaseCalendarSlotStep, StepOutcome.OutcomeUnknown)
      case Event.RetryExhausted(step)             =>
        outcome(definition.nodes.get(workflow.phase).fold("")(_.step.name), StepOutcome.RetryExhausted(step))
      case notification => SagaInput.Trigger(notification)
    }
  }

  def toWorkflowError(error: SagaError[InterviewWorkflowError]): InterviewWorkflowError = error match {
    case SagaError.StaleRevision      => InterviewWorkflowError.StaleRevision
    case SagaError.InvalidTransition  => InterviewWorkflowError.InvalidTransition
    case SagaError.Definition(domain) => domain
  }
}
