package com.example.graphQL.cats.service.application

import com.example.graphQL.cats.domain.workflow.InterviewWorkflow
import com.example.graphQL.cats.service.port.{InterviewWorkflowCommandRecord, InterviewWorkflowCommandState}
import java.time.Instant
import java.util.UUID

enum InterviewExecutionAdmission {
  case AlreadyHandled, InspectLease, CheckBudget
}

final case class InterviewExecutionLease(owner: String, token: UUID, until: Instant)

enum InterviewExecutionLeaseDisposition {
  case Busy, Reconcile
}

enum InterviewExecutionBudgetDisposition {
  case RequireRepair
  case Acquire(additionalAttempt: Int)
}

/** Pure staged decisions preserve which observations persistence needs to decode and read. */
object InterviewExecutionPolicy {
  def admission(workflow: InterviewWorkflow, stored: InterviewWorkflowCommandRecord): InterviewExecutionAdmission =
    if (stored.result.nonEmpty || !applicable(workflow, stored)) InterviewExecutionAdmission.AlreadyHandled
    else
      stored.state match {
        case InterviewWorkflowCommandState.Executing => InterviewExecutionAdmission.InspectLease
        case InterviewWorkflowCommandState.Pending | InterviewWorkflowCommandState.Claimed |
            InterviewWorkflowCommandState.Published =>
          InterviewExecutionAdmission.CheckBudget
        case _ => InterviewExecutionAdmission.AlreadyHandled
      }

  def lease(lease: InterviewExecutionLease, now: Instant): InterviewExecutionLeaseDisposition =
    if (lease.until.isAfter(now)) InterviewExecutionLeaseDisposition.Busy
    else InterviewExecutionLeaseDisposition.Reconcile

  def budget(
      stored: InterviewWorkflowCommandRecord,
      consumed: Long,
      maxAttempts: Int
  ): InterviewExecutionBudgetDisposition = {
    val additional = if (stored.executionAttempts == 0) 1 else 0
    if (consumed + additional.toLong > maxAttempts.toLong) InterviewExecutionBudgetDisposition.RequireRepair
    else InterviewExecutionBudgetDisposition.Acquire(additional)
  }

  private def applicable(workflow: InterviewWorkflow, command: InterviewWorkflowCommandRecord): Boolean =
    command.state != InterviewWorkflowCommandState.Superseded &&
      InterviewWorkflow.commandIsApplicable(workflow, command.revision, command.command)
}
