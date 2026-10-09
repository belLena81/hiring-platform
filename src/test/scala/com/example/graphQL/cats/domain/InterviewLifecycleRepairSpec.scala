package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.InterviewLifecycleFixtures.*
import com.example.graphQL.cats.domain.workflow.*
import munit.FunSuite

/** Admin repair resumes the step that exhausted its retries by lookup and never reissues a provider effect. */
class InterviewLifecycleRepairSpec extends FunSuite {
  import InterviewLifecycleCommand as Command
  import InterviewLifecycleEvent as Event
  import InterviewWorkflowPhase as Phase

  private def key(suffix: String) = s"${workflowId.value}:$suffix"

  private val repairable: List[(InterviewWorkflow, List[InterviewLifecycleCommand])] = List(
    workflowIn(Phase.CancelPending) ->
      List(Command.LookupCalendarCancellation(key("cancel:g0"))),
    workflowIn(Phase.CancelNotificationsPending, _.copy(notified = Set(InterviewParticipant.Candidate))) ->
      List(
        Command.LookupNotificationReceipt(
          InterviewNotificationKind.Cancelled,
          InterviewParticipant.Recruiter,
          key("cancelled:notify:Recruiter")
        )
      ),
    workflowIn(Phase.RescheduleHoldPending, _.copy(pendingInterval = Some(replacement))) ->
      List(Command.LookupReplacementHold(key("reserve:g1"))),
    workflowIn(Phase.RescheduleSwapPending, _.copy(pendingInterval = Some(replacement))) ->
      List(Command.LookupRescheduleCommitReceipt(workflowId, 1)),
    workflowIn(Phase.RescheduleCancelOldPending, _.copy(generation = 1, interval = replacement)) ->
      List(Command.LookupCalendarCancellation(key("cancel:g0"))),
    workflowIn(Phase.RescheduleCompensationPending, _.copy(pendingInterval = Some(replacement))) ->
      List(Command.LookupCalendarCancellation(key("cancel:g1"))),
    workflowIn(Phase.RescheduleNotificationsPending, _.copy(generation = 1, interval = replacement)) ->
      InterviewParticipant.values.toList.map(participant =>
        Command.LookupNotificationReceipt(
          InterviewNotificationKind.Rescheduled,
          participant,
          key(s"rescheduled:g1:notify:$participant")
        )
      )
  )

  repairable.foreach { case (workflow, lookups) =>
    test(s"DHW-29 a retry-exhausted ${workflow.phase} records its origin and repair resumes it by lookup") {
      val failed = decided(workflow, Event.RetryExhausted("provider")).workflow
      assertEquals(failed.phase, Phase.RepairRequired)
      assertEquals(failed.repairOrigin, Some(workflow.phase))
      val repaired = decided(failed, Event.Repair)
      assertEquals(repaired.workflow.phase, workflow.phase)
      assertEquals(repaired.workflow.repairOrigin, None)
      assertEquals(repaired.workflow.revision, failed.revision + 1)
      assertEquals(repaired.commands, lookups)
      // Repair only looks things up: no hold, cancel or notification is requested again before the lookup answers.
      assert(
        repaired.commands.forall {
          case _: Command.LookupCalendarCancellation | _: Command.LookupReplacementHold |
              _: Command.LookupRescheduleCommitReceipt | _: Command.LookupNotificationReceipt =>
            true
          case _ => false
        },
        clue(repaired.commands)
      )
    }
  }

  test("DHW-29 repair needs a recorded origin, the failed state and the current revision") {
    val noOrigin = workflowIn(Phase.RepairRequired)
    assertEquals(decide(noOrigin, Event.Repair), Left(InterviewWorkflowError.InvalidTransition))
    val failed = decided(workflowIn(Phase.CancelPending), Event.RetryExhausted("provider")).workflow
    assertEquals(
      InterviewLifecyclePolicy.decide(failed, failed.revision - 1, Event.Repair),
      Left(InterviewWorkflowError.StaleRevision)
    )
    Phase.values.filterNot(_ == Phase.RepairRequired).foreach { phase =>
      val workflow = workflowIn(phase, _.copy(proposal = None))
      assert(decide(workflow, Event.Repair).isLeft, clue(phase))
    }
  }

  test("DHW-29 a proposal is waiting, not failing: it has nothing to repair and is resolved by withdrawal") {
    assertEquals(decide(proposalPending(), Event.Repair), Left(InterviewWorkflowError.InvalidTransition))
    assertEquals(
      decided(proposalPending(), Event.WithdrawProposal(now)).workflow.phase,
      Phase.Completed
    )
  }

  test("DHW-29 a repaired step that fails again records the same origin once more") {
    val workflow = workflowIn(Phase.CancelPending)
    val repaired = decided(decided(workflow, Event.RetryExhausted("provider")).workflow, Event.Repair).workflow
    val failedAgain = decided(repaired, Event.RetryExhausted("provider")).workflow
    assertEquals(failedAgain.repairOrigin, Some(Phase.CancelPending))
  }
}
