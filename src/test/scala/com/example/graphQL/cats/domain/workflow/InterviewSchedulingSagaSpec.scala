package com.example.graphQL.cats.domain.workflow

import com.example.graphQL.cats.domain.workflow.characterization.InterviewCharacterizationFixtures.*
import com.example.hiring.saga.{SagaDefinitionChecks, SagaStatus, StepUndo}

/** SAG-03 for the interview scheduling definition, plus the step-name mapping of every scheduling event. */
class InterviewSchedulingSagaSpec extends munit.FunSuite {
  import InterviewWorkflowPhase as Phase

  private val definition = InterviewSchedulingSaga.definition
  private val sample = workflow(Phase.ReservationPending)

  test("SAG-03 the scheduling definition is well formed") {
    assertEquals(SagaDefinitionChecks.problems(definition, sample), Nil)
  }

  test("SAG-03.1 every scheduling step declares its undo; only the status commit is compensated, by the release") {
    val undos = definition.steps.map(step => step.name -> step.undo.getClass.getSimpleName.stripSuffix("$")).toMap
    assertEquals(
      undos.keySet,
      Set("reserveCalendarSlot", "commitAcceptedToInterview", "releaseCalendarSlot", "notifyParticipants")
    )
    assertEquals(
      definition.steps.collect {
        case step @ com.example.hiring.saga.SagaStep(_, _, _, _, _, StepUndo.Compensate(target), _) =>
          step.name -> target.name
      },
      List("commitAcceptedToInterview" -> "releaseCalendarSlot")
    )
  }

  test("SAG-03.4 every scheduling phase has a node or is terminal or repair; lifecycle phases have none") {
    val schedulingPhases =
      Set(Phase.ReservationPending, Phase.StatusCommitPending, Phase.CompensationPending, Phase.NotificationsPending)
    assertEquals(definition.nodes.keySet, schedulingPhases)
    assertEquals(definition.status(Phase.CompensationPending), SagaStatus.Compensating)
    assertEquals(definition.status(Phase.Completed), SagaStatus.Completed)
    assertEquals(definition.status(Phase.RepairRequired), SagaStatus.RepairRequired)
    assert(!definition.repair.recordsOrigin, "scheduling failures leave repairOrigin empty")
  }

  test("SAG-04.1 every scheduling event maps to the step of the phase that handles it") {
    val reserve = workflow(Phase.ReservationPending)
    val expectations = List(
      InterviewWorkflowEvent.ReservationConfirmed -> InterviewSchedulingSaga.ReserveCalendarSlotStep,
      InterviewWorkflowEvent.ReservationLookupAbsent -> InterviewSchedulingSaga.ReserveCalendarSlotStep,
      InterviewWorkflowEvent.StatusCommitted -> InterviewSchedulingSaga.CommitAcceptedToInterviewStep,
      InterviewWorkflowEvent.StatusLookupAbsent -> InterviewSchedulingSaga.CommitAcceptedToInterviewStep,
      InterviewWorkflowEvent.ReservationReleased -> InterviewSchedulingSaga.ReleaseCalendarSlotStep,
      InterviewWorkflowEvent.ReservationReleaseOutcomeUnknown -> InterviewSchedulingSaga.ReleaseCalendarSlotStep
    )
    expectations.foreach { case (event, step) =>
      InterviewSchedulingSaga.input(reserve, event) match {
        case com.example.hiring.saga.SagaInput.Outcome(name, _, _) => assertEquals(name, step, event.toString)
        case other                                                 => fail(s"$event mapped to $other")
      }
    }
  }
}
