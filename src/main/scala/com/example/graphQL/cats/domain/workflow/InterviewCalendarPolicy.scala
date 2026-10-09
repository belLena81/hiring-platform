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
}
