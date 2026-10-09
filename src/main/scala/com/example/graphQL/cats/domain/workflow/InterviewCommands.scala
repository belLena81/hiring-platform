package com.example.graphQL.cats.domain.workflow

/** Any durable interview intent: scheduling work or cancellation and rescheduling work. One executor runs both. */
type InterviewCommand = InterviewWorkflowCommand | InterviewLifecycleCommand

object InterviewCommands {
  import InterviewWorkflowPhase as Phase

  /** A notification kind that belongs to a phase round (cancellation, successful reschedule) rather than to a single
    * informational event. Round deliveries gate the phase; informational ones never do.
    */
  def isRoundKind(kind: InterviewNotificationKind): Boolean = kind match {
    case InterviewNotificationKind.Cancelled | InterviewNotificationKind.Rescheduled => true
    case _                                                                           => false
  }

  /** True for a kind-qualified notification that never gates a workflow phase. */
  def isInformational(command: InterviewCommand): Boolean = command match {
    case InterviewLifecycleCommand.Notify(kind, _, _) => !isRoundKind(kind)
    case _                                            => false
  }

  def isExpiry(command: InterviewCommand): Boolean = command match {
    case _: InterviewLifecycleCommand.ExpireProposal => true
    case _                                           => false
  }

  /** The event that ends a lifecycle step which can no longer run (retries or delivery given up). Expiry has no
    * external effect, so giving up on it is expiring the proposal; every other step enters repair.
    */
  def giveUpEvent(command: InterviewCommand, step: String, now: java.time.Instant): InterviewLifecycleEvent =
    command match {
      case InterviewLifecycleCommand.ExpireProposal(_) => InterviewLifecycleEvent.ProposalExpired(now)
      case _                                           => InterviewLifecycleEvent.RetryExhausted(step)
    }

  /** Pure fencing shared by the worker and the persistence adapter: a command may only run while the workflow still
    * needs exactly that effect. Round notifications tolerate older revisions because each delivery advances it.
    */
  def isApplicable(workflow: InterviewWorkflow, commandRevision: Long, command: InterviewCommand): Boolean =
    command match {
      case scheduling: InterviewWorkflowCommand =>
        InterviewWorkflow.commandIsApplicable(workflow, commandRevision, scheduling)
      case lifecycle: InterviewLifecycleCommand => lifecycleIsApplicable(workflow, commandRevision, lifecycle)
    }

  private def lifecycleIsApplicable(
      workflow: InterviewWorkflow,
      commandRevision: Long,
      command: InterviewLifecycleCommand
  ): Boolean = {
    import InterviewLifecycleCommand as C
    val current = workflow.revision == commandRevision
    def cancelFor(key: String): Boolean =
      current && InterviewCalendarFence
        .cancellableGeneration(workflow)
        .exists(generation => InterviewCalendarKeys.cancellationGeneration(workflow.id, key).contains(generation))
    def roundFor(kind: InterviewNotificationKind, participant: InterviewParticipant): Boolean = {
      val phase =
        if (kind == InterviewNotificationKind.Cancelled) Phase.CancelNotificationsPending
        else Phase.RescheduleNotificationsPending
      workflow.phase == phase && commandRevision <= workflow.revision && !workflow.notified.contains(participant)
    }
    command match {
      case C.CancelCalendarSlot(key)                                => cancelFor(key)
      case C.LookupCalendarCancellation(k)                          => cancelFor(k)
      case C.HoldReplacementSlot(_, _) | C.LookupReplacementHold(_) =>
        current && workflow.phase == Phase.RescheduleHoldPending
      case C.CommitRescheduledInterval(_, _) | C.LookupRescheduleCommitReceipt(_, _) =>
        current && workflow.phase == Phase.RescheduleSwapPending
      case C.ExpireProposal(_)            => current && workflow.phase == Phase.ProposalPending
      case C.Notify(kind, participant, _) =>
        if (isRoundKind(kind)) roundFor(kind, participant)
        else commandRevision <= workflow.revision
      case C.LookupNotificationReceipt(kind, participant, _) => isRoundKind(kind) && roundFor(kind, participant)
      case C.RequireRepair(_)                                => workflow.phase == Phase.RepairRequired
    }
  }
}
