package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.InterviewLifecycleFixtures.*
import com.example.graphQL.cats.domain.workflow.*
import munit.FunSuite

/** A compensated attempt leaves its cancelled reservation stored under its key, so the next attempt needs a fresh one.
  */
class InterviewRescheduleGenerationSpec extends FunSuite {
  import InterviewLifecycleCommand as Command
  import InterviewLifecycleEvent as Event
  import InterviewWorkflowPhase as Phase

  private def key(suffix: String) = s"${workflowId.value}:$suffix"

  private def accept(w: InterviewWorkflow) = decided(w, Event.AcceptProposal(now))

  test("DHW-22 a compensated attempt consumes its generation and the next acceptance holds under a fresh key") {
    val first = accept(proposalPending())
    assertEquals(first.commands, List(Command.HoldReplacementSlot(key("reserve:g1"), replacement)))
    val swapping = decided(first.workflow, Event.HoldConfirmed).workflow
    val compensating = decided(swapping, Event.SwapRejected)
    assertEquals(compensating.commands, List(Command.CancelCalendarSlot(key("cancel:g1"))))
    val completed = decided(compensating.workflow, Event.CancelConfirmed(now)).workflow
    assertEquals(completed.phase, Phase.Completed)
    assertEquals(completed.skippedGenerations, 1)
    assertEquals(completed.generation, 0)
    // Propose again on the same workflow and accept: the released g1 reservation is not reused.
    val proposed = decided(completed, propose()).workflow
    val second = accept(proposed)
    assertEquals(second.commands, List(Command.HoldReplacementSlot(key("reserve:g2"), replacement)))
    assertEquals(
      decided(second.workflow, Event.HoldOutcomeUnknown).commands,
      List(Command.LookupReplacementHold(key("reserve:g2")))
    )
    val swapping2 = decided(second.workflow, Event.HoldConfirmed)
    assertEquals(swapping2.commands, List(Command.CommitRescheduledInterval(replacement, 2)))
    val committed = decided(swapping2.workflow, Event.SwapCommitted)
    assertEquals(committed.commands, List(Command.CancelCalendarSlot(key("cancel:g0"))))
    assertEquals(committed.workflow.generation, 2)
    assertEquals(committed.workflow.retiredGeneration, 0)
    assertEquals(InterviewCalendarFence.cancellableGeneration(committed.workflow), Some(0))
    val told = decided(committed.workflow, Event.CancelConfirmed(now)).workflow
    assertEquals(told.skippedGenerations, 0)
    assertEquals(told.generation, 2)
  }

  test("DHW-22 a refused hold creates no reservation, so its generation is not consumed") {
    val holding = accept(proposalPending()).workflow
    val back = decided(holding, Event.HoldRejected).workflow
    assertEquals(back.skippedGenerations, 0)
    assertEquals(
      accept(decided(back, propose()).workflow).commands,
      List(Command.HoldReplacementSlot(key("reserve:g1"), replacement))
    )
  }

  test("DHW-22 the calendar fences follow the attempted generation in every reschedule phase") {
    val skipped =
      workflowIn(Phase.RescheduleHoldPending, _.copy(skippedGenerations = 2, pendingInterval = Some(replacement)))
    assertEquals(InterviewCalendarFence.holdableGeneration(skipped), Some(3))
    assertEquals(
      InterviewCalendarFence.cancellableGeneration(skipped.copy(phase = Phase.RescheduleCompensationPending)),
      Some(3)
    )
    assertEquals(
      InterviewCalendarFence.cancellableGeneration(
        skipped.copy(phase = Phase.RescheduleCancelOldPending, generation = 3, pendingInterval = None)
      ),
      Some(0)
    )
  }

  test("DHW-29 repair of a later attempt resumes the lookups of its own generation") {
    val failed = decided(
      workflowIn(Phase.RescheduleSwapPending, _.copy(skippedGenerations = 1, pendingInterval = Some(replacement))),
      Event.RetryExhausted("provider")
    ).workflow
    assertEquals(
      decided(failed, Event.Repair).commands,
      List(Command.LookupRescheduleCommitReceipt(workflowId, 2))
    )
  }
}
