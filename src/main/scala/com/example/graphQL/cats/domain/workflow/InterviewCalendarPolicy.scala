package com.example.graphQL.cats.domain.workflow

/** Pure correspondence between a workflow's reservation generation and its provider idempotency keys. */
object InterviewCalendarKeys {
  def reservationGeneration(workflowId: InterviewWorkflowId, key: String): Option[Int] =
    generation(key, s"${workflowId.value}:reserve")(InterviewWorkflow.reservationKey(workflowId, _))

  def cancellationGeneration(workflowId: InterviewWorkflowId, key: String): Option[Int] =
    generation(key, s"${workflowId.value}:cancel")(InterviewWorkflow.cancellationKey(workflowId, _))

  /** The reservation key that a reserve key or a cancel key addresses; `None` for any unrelated key. */
  def reservationKeyFor(workflowId: InterviewWorkflowId, key: String): Option[String] =
    reservationGeneration(workflowId, key)
      .orElse(cancellationGeneration(workflowId, key))
      .map(InterviewWorkflow.reservationKey(workflowId, _))

  /** The cancel key that releases the reservation a reserve key addresses; `None` unless it is a canonical reserve key.
    */
  def cancellationKeyFor(workflowId: InterviewWorkflowId, reserveKey: String): Option[String] =
    reservationGeneration(workflowId, reserveKey).map(InterviewWorkflow.cancellationKey(workflowId, _))

  private def generation(key: String, prefix: String)(canonical: Int => String): Option[Int] =
    if (key == canonical(0)) Some(0)
    else key.stripPrefix(s"$prefix:g").toIntOption.filter(value => value > 0 && canonical(value) == key)
}

/** Pure workflow fences: the provider may only change a reservation while the workflow requires exactly that effect. */
object InterviewCalendarFence {

  /** Generation of the reservation the workflow currently requires to be cancelled. */
  def cancellableGeneration(workflow: InterviewWorkflow): Option[Int] = workflow.phase match {
    case InterviewWorkflowPhase.CancelPending                 => Some(workflow.generation)
    case InterviewWorkflowPhase.RescheduleCancelOldPending    => Some(workflow.retiredGeneration)
    case InterviewWorkflowPhase.RescheduleCompensationPending => Some(workflow.replacementGeneration)
    case _                                                    => None
  }

  /** Generation of the replacement hold the workflow currently requires, after the candidate accepted. */
  def holdableGeneration(workflow: InterviewWorkflow): Option[Int] = workflow.phase match {
    case InterviewWorkflowPhase.ReservationPending    => Some(0)
    case InterviewWorkflowPhase.RescheduleHoldPending => Some(workflow.replacementGeneration)
    case _                                            => None
  }

  /** The swap must happen before either the old or the replacement interval starts. */
  def swapDeadline(workflow: InterviewWorkflow): java.time.Instant =
    workflow.pendingInterval
      .map(_.startsAt)
      .filter(_.isBefore(workflow.interval.startsAt))
      .getOrElse(workflow.interval.startsAt)

  /** The interval a hold of `generation` must cover for this workflow. */
  def holdInterval(workflow: InterviewWorkflow, generation: Int): Option[InterviewInterval] =
    if (generation == 0) Some(workflow.interval) else workflow.pendingInterval
}

/** Notification fences: round kinds are delivered only in their own phase. Informational kinds never gate a phase and
  * are deliverable in every phase, repair included: they are delivered or visibly repaired, never dropped.
  */
object InterviewNotificationFence {
  def permits(kind: InterviewNotificationKind, phase: InterviewWorkflowPhase): Boolean = kind match {
    case InterviewNotificationKind.Scheduled   => phase == InterviewWorkflowPhase.NotificationsPending
    case InterviewNotificationKind.Cancelled   => phase == InterviewWorkflowPhase.CancelNotificationsPending
    case InterviewNotificationKind.Rescheduled => phase == InterviewWorkflowPhase.RescheduleNotificationsPending
    case _                                     => true
  }

  /** Only the scheduling notification requires the hiring commit; later kinds follow other durable events. */
  def requiresHiringCommit(kind: InterviewNotificationKind): Boolean = kind == InterviewNotificationKind.Scheduled
}

/** Whether the caller still owns the command execution it sends for. `Absent` means no claim was supplied. */
enum InterviewClaimStanding {
  case Held, Lost, Absent
}

/** Why an external send is refused before any provider call. Each refusal is a definitive no-send except
  * `CheckUnavailable`, which means the check itself could not run (nothing is sent either).
  */
enum InterviewNotificationSendRefusal {
  case ClaimNotHeld
  case ParticipantInactive
  case KindNotAllowedInPhase
  case HiringNotCommitted
  case WorkflowNotFound
  case CheckUnavailable
}

/** The facts an external send is checked against, read from durable state in separate reads without a snapshot; any
  * skew falls inside the documented check-to-call window.
  */
final case class InterviewNotificationSendFacts(
    claim: InterviewClaimStanding,
    participantsActive: Boolean,
    phase: InterviewWorkflowPhase,
    hiringCommitted: Boolean
)

/** The pure send policy: the same four predicates the receipt ledger applies before it records a delivery (claim,
  * participants, kind allowed in the phase, hiring committed before `Scheduled`), checked in that order. Unlike the
  * ledger, which accepts a call without a claim, an external send with no claim is refused: the runtime always supplies
  * one, so a missing claim is a wiring error that must not send.
  */
object InterviewNotificationSendPolicy {
  def decide(
      kind: InterviewNotificationKind,
      facts: InterviewNotificationSendFacts
  ): Either[InterviewNotificationSendRefusal, Unit] = {
    import InterviewNotificationSendRefusal.*
    for {
      _ <- Either.cond(facts.claim == InterviewClaimStanding.Held, (), ClaimNotHeld)
      _ <- Either.cond(facts.participantsActive, (), ParticipantInactive)
      _ <- Either.cond(InterviewNotificationFence.permits(kind, facts.phase), (), KindNotAllowedInPhase)
      _ <- Either.cond(
        !InterviewNotificationFence.requiresHiringCommit(kind) || facts.hiringCommitted,
        (),
        HiringNotCommitted
      )
    } yield ()
  }
}
