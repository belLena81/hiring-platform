package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.InterviewLifecycleFixtures.*
import com.example.graphQL.cats.domain.workflow.*
import munit.FunSuite

final class InterviewCalendarPolicySpec extends FunSuite {
  private val id = workflowId

  test("reserve and cancel keys map to their generation and to the reservation they address") {
    assertEquals(InterviewCalendarKeys.reservationGeneration(id, InterviewWorkflow.reservationKey(id, 0)), Some(0))
    assertEquals(InterviewCalendarKeys.reservationGeneration(id, InterviewWorkflow.reservationKey(id, 3)), Some(3))
    assertEquals(InterviewCalendarKeys.cancellationGeneration(id, InterviewWorkflow.cancellationKey(id, 0)), Some(0))
    assertEquals(InterviewCalendarKeys.cancellationGeneration(id, InterviewWorkflow.cancellationKey(id, 2)), Some(2))
    assertEquals(
      InterviewCalendarKeys.reservationKeyFor(id, InterviewWorkflow.cancellationKey(id, 2)),
      Some(InterviewWorkflow.reservationKey(id, 2))
    )
  }

  test("non-canonical, foreign and malformed keys address nothing") {
    val other = InterviewWorkflowId(java.util.UUID.randomUUID())
    List(
      s"${id.value}:reserve:g0",
      s"${id.value}:reserve:g01",
      s"${id.value}:reserve:g-1",
      s"${id.value}:release",
      InterviewWorkflow.reservationKey(other, 1),
      "",
      "unrelated"
    ).foreach(key => assertEquals(InterviewCalendarKeys.reservationGeneration(id, key), None, clue(key)))
    assertEquals(InterviewCalendarKeys.reservationKeyFor(id, InterviewWorkflow.cancellationKey(other, 1)), None)
    assertEquals(InterviewCalendarKeys.cancellationGeneration(id, s"${id.value}:cancel:g00"), None)
  }

  test("the provider cancels exactly the generation the workflow phase demands") {
    import InterviewWorkflowPhase as Phase
    val generation = 2
    def demanded(phase: Phase) =
      InterviewCalendarFence.cancellableGeneration(workflowIn(phase, _.copy(generation = generation)))
    assertEquals(demanded(Phase.CancelPending), Some(2))
    assertEquals(demanded(Phase.RescheduleCancelOldPending), Some(1))
    assertEquals(demanded(Phase.RescheduleCompensationPending), Some(3))
    Phase.values
      .filterNot(Set(Phase.CancelPending, Phase.RescheduleCancelOldPending, Phase.RescheduleCompensationPending))
      .foreach { phase =>
        assertEquals(demanded(phase), None, clue(phase))
      }
  }

  test("a hold is only permitted for the original reservation or after a candidate accepted a proposal") {
    import InterviewWorkflowPhase as Phase
    val holding = workflowIn(Phase.RescheduleHoldPending, _.copy(generation = 1, pendingInterval = Some(replacement)))
    assertEquals(InterviewCalendarFence.holdableGeneration(holding), Some(2))
    assertEquals(InterviewCalendarFence.holdInterval(holding, 2), Some(replacement))
    assertEquals(InterviewCalendarFence.holdableGeneration(workflowIn(Phase.ReservationPending)), Some(0))
    assertEquals(InterviewCalendarFence.holdInterval(workflowIn(Phase.ReservationPending), 0), Some(interval))
    assertEquals(InterviewCalendarFence.holdableGeneration(completed), None)
    assertEquals(InterviewCalendarFence.holdableGeneration(proposalPending()), None)
  }

  test(
    "notification kinds are deliverable only in the phase that waits for them; informational kinds in every phase"
  ) {
    import InterviewNotificationKind as Kind
    import InterviewWorkflowPhase as Phase
    assert(InterviewNotificationFence.permits(Kind.Scheduled, Phase.NotificationsPending))
    assert(!InterviewNotificationFence.permits(Kind.Scheduled, Phase.Completed))
    assert(InterviewNotificationFence.permits(Kind.Cancelled, Phase.CancelNotificationsPending))
    assert(!InterviewNotificationFence.permits(Kind.Cancelled, Phase.CancelPending))
    assert(InterviewNotificationFence.permits(Kind.Rescheduled, Phase.RescheduleNotificationsPending))
    val informational = Kind.values.toList.filterNot(Set(Kind.Scheduled, Kind.Cancelled, Kind.Rescheduled))
    informational.foreach { kind =>
      Phase.values.foreach(phase => assert(InterviewNotificationFence.permits(kind, phase), clue((kind, phase))))
    }
  }
}
