package com.example.graphQL.cats.service.application

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.*
import java.time.Instant
import java.util.UUID
import munit.FunSuite

final class InterviewExecutionPolicySpec extends FunSuite {
  private val now = Instant.parse("2026-10-06T12:00:00Z")
  private val workflow = InterviewWorkflow(
    InterviewWorkflowId(new UUID(0L, 1L)),
    ApplicationId(new UUID(0L, 2L)),
    UserId(new UUID(0L, 3L)),
    UserId(new UUID(0L, 4L)),
    InterviewInterval(now.plusSeconds(600), now.plusSeconds(1200)),
    now.plusSeconds(300),
    new UUID(0L, 5L),
    0L,
    InterviewWorkflowPhase.ReservationPending,
    Set.empty,
    UserId(new UUID(0L, 4L))
  )
  private val command = InterviewWorkflowCommandRecord(
    workflow.id,
    "reservation-step",
    0L,
    InterviewWorkflow.initialCommand(workflow),
    InterviewWorkflowCommandState.Pending,
    0,
    now,
    now
  )

  test("recorded results and obsolete commands stop admission before lease or budget observation") {
    val executing = command.copy(state = InterviewWorkflowCommandState.Executing)
    assertEquals(
      InterviewExecutionPolicy.admission(workflow, executing.copy(result = Some(InterviewCommandResult.Succeeded))),
      InterviewExecutionAdmission.AlreadyHandled
    )
    assertEquals(
      InterviewExecutionPolicy.admission(workflow.copy(revision = 1L), executing),
      InterviewExecutionAdmission.AlreadyHandled
    )
    assertEquals(
      InterviewExecutionPolicy.admission(workflow, command.copy(state = InterviewWorkflowCommandState.Superseded)),
      InterviewExecutionAdmission.AlreadyHandled
    )
    assertEquals(
      InterviewExecutionPolicy.admission(workflow.copy(phase = InterviewWorkflowPhase.Completed), command),
      InterviewExecutionAdmission.AlreadyHandled
    )
  }

  test("only eligible executions inspect leases and only queued publication states inspect budgets") {
    assertEquals(
      InterviewExecutionPolicy.admission(workflow, command.copy(state = InterviewWorkflowCommandState.Executing)),
      InterviewExecutionAdmission.InspectLease
    )
    List(
      InterviewWorkflowCommandState.Pending,
      InterviewWorkflowCommandState.Claimed,
      InterviewWorkflowCommandState.Published
    ).foreach { state =>
      assertEquals(
        InterviewExecutionPolicy.admission(workflow, command.copy(state = state)),
        InterviewExecutionAdmission.CheckBudget
      )
    }
    List(
      InterviewWorkflowCommandState.ResultPending,
      InterviewWorkflowCommandState.ResultPublished,
      InterviewWorkflowCommandState.RepairRequired
    ).foreach { state =>
      assertEquals(
        InterviewExecutionPolicy.admission(workflow, command.copy(state = state)),
        InterviewExecutionAdmission.AlreadyHandled
      )
    }
  }

  test("active leases are busy while equality and past expiry require reconciliation") {
    val lease = InterviewExecutionLease("worker", new UUID(0L, 6L), now.plusNanos(1))
    assertEquals(InterviewExecutionPolicy.lease(lease, now), InterviewExecutionLeaseDisposition.Busy)
    assertEquals(
      InterviewExecutionPolicy.lease(lease.copy(until = now), now),
      InterviewExecutionLeaseDisposition.Reconcile
    )
    assertEquals(
      InterviewExecutionPolicy.lease(lease.copy(until = now.minusNanos(1)), now),
      InterviewExecutionLeaseDisposition.Reconcile
    )
  }

  test("fresh intent consumes one slot and prepaid intent executes its existing final slot") {
    assertEquals(InterviewExecutionPolicy.budget(command, 4L, 5), InterviewExecutionBudgetDisposition.Acquire(1))
    assertEquals(InterviewExecutionPolicy.budget(command, 5L, 5), InterviewExecutionBudgetDisposition.RequireRepair)
    val prepaid = command.copy(executionAttempts = 1)
    assertEquals(InterviewExecutionPolicy.budget(prepaid, 5L, 5), InterviewExecutionBudgetDisposition.Acquire(0))
    assertEquals(InterviewExecutionPolicy.budget(prepaid, 6L, 5), InterviewExecutionBudgetDisposition.RequireRepair)
  }

  test("attempt arithmetic stays in Long at the Int budget boundary") {
    assertEquals(
      InterviewExecutionPolicy.budget(command, Int.MaxValue.toLong - 1L, Int.MaxValue),
      InterviewExecutionBudgetDisposition.Acquire(1)
    )
    assertEquals(
      InterviewExecutionPolicy.budget(command, Int.MaxValue.toLong, Int.MaxValue),
      InterviewExecutionBudgetDisposition.RequireRepair
    )
  }

  test("outstanding notification revisions remain eligible for their recipient budget") {
    val notifying = workflow.copy(revision = 2L, phase = InterviewWorkflowPhase.NotificationsPending)
    val notification = command.copy(
      revision = 1L,
      command = InterviewWorkflowCommand.Notify(InterviewParticipant.Candidate, "candidate-key")
    )
    assertEquals(InterviewExecutionPolicy.admission(notifying, notification), InterviewExecutionAdmission.CheckBudget)
    assertEquals(
      InterviewExecutionPolicy.admission(notifying, notification.copy(revision = 3L)),
      InterviewExecutionAdmission.AlreadyHandled
    )
  }
}
