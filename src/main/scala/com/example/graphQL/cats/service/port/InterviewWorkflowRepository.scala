package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.UserRole
import com.example.graphQL.cats.domain.workflow.{
  InterviewWorkflow,
  InterviewWorkflowCommand,
  InterviewWorkflowId,
  InterviewAdvanceCause
}
import java.time.Instant
import java.util.UUID

final case class InterviewWorkflowAccess(actorId: UserId, role: UserRole)

enum InterviewWorkflowAdvanceResult {
  case Applied
  case Duplicate(existing: InterviewWorkflow)
  case StaleRevision
}

enum InterviewWorkflowCommandState {
  case Pending, Claimed, Published, Executing, ResultPending, ResultPublished, RepairRequired, Superseded
}

enum InterviewCommandResult { case Succeeded, Rejected, OutcomeUnknown, Found, Absent }

object InterviewCommandResult {
  def fromTransportResult(value: InterviewResult): InterviewCommandResult = value match {
    case InterviewResult.Succeeded      => Succeeded
    case InterviewResult.Rejected       => Rejected
    case InterviewResult.OutcomeUnknown => OutcomeUnknown
    case InterviewResult.Found          => Found
    case InterviewResult.Absent         => Absent
  }
}

final case class InterviewWorkflowCommandRecord(
    workflowId: InterviewWorkflowId,
    stepId: String,
    revision: Long,
    command: InterviewWorkflowCommand,
    state: InterviewWorkflowCommandState,
    publicationAttempts: Int,
    availableAt: Instant,
    occurredAt: Instant,
    result: Option[InterviewCommandResult] = None,
    executionAttempts: Int = 0
)

final case class ClaimedInterviewWorkflowCommand(
    record: InterviewWorkflowCommandRecord,
    owner: String,
    fencingToken: UUID,
    leaseUntil: Instant
)

enum InterviewExecutionClaimOutcome {
  case Acquired(claim: ClaimedInterviewWorkflowCommand)
  case Busy, AlreadyHandled, ReconciliationQueued, RepairRequired
}

enum InterviewPublicationResolution {
  case Repaired, Superseded, AlreadyHandled
}

/** Durable workflow state, inbox receipt, and emitted command intents share a transaction on `advance`. Implementations
  * must enforce workflow/step and inbox-message uniqueness, revision CAS, actor-scoped reads, and fencing-token checks
  * for claims.
  */
trait InterviewWorkflowRepository {
  def claimExecution(
      command: InterviewWorkflowCommandRecord,
      workerId: String,
      now: Instant,
      leaseUntil: Instant,
      maxAttempts: Int
  ): RepositoryIO[InterviewExecutionClaimOutcome]
  def authorizePublication(
      claim: ClaimedInterviewWorkflowCommand,
      generation: InterviewPublisherGeneration,
      now: Instant
  ): RepositoryIO[Boolean]
  def attemptCount(workflowId: InterviewWorkflowId, command: InterviewWorkflowCommand): RepositoryIO[Long]
  def quarantine(identity: String, now: Instant): RepositoryIO[Unit]
  def recordResult(
      claim: ClaimedInterviewWorkflowCommand,
      result: InterviewCommandResult,
      now: Instant
  ): RepositoryIO[Unit]
  def findRequest(
      recruiterId: UserId,
      requestKey: UUID,
      fingerprint: MutationReceiptFingerprint
  ): RepositoryIO[Option[InterviewWorkflow]]
  def findCommand(workflowId: InterviewWorkflowId, stepId: String): RepositoryIO[Option[InterviewWorkflowCommandRecord]]
  def repair(
      workflow: InterviewWorkflow,
      expectedRevision: Long,
      requestKey: UUID,
      now: Instant,
      actorId: UserId
  ): RepositoryIO[InterviewWorkflow]

  def commitHiring(
      workflow: InterviewWorkflow,
      now: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand] = None
  ): RepositoryIO[Unit]
  def hasHiringReceipt(workflowId: InterviewWorkflowId): RepositoryIO[Boolean]

  def create(
      workflow: InterviewWorkflow,
      initialCommand: InterviewWorkflowCommand,
      requestKey: UUID,
      fingerprint: MutationReceiptFingerprint,
      createdAt: Instant
  ): RepositoryIO[InterviewWorkflowAdvanceResult]

  def findForActor(
      workflowId: InterviewWorkflowId,
      access: InterviewWorkflowAccess
  ): RepositoryIO[Option[InterviewWorkflow]]

  def findForAdmin(workflowId: InterviewWorkflowId): RepositoryIO[Option[InterviewWorkflow]]

  def advance(
      workflow: InterviewWorkflow,
      expectedRevision: Long,
      cause: InterviewAdvanceCause,
      commands: List[InterviewWorkflowCommand],
      occurredAt: Instant,
      availableAt: Option[Instant] = None
  ): RepositoryIO[InterviewWorkflowAdvanceResult]

  def claimDueCommands(
      workerId: String,
      now: Instant,
      leaseUntil: Instant,
      limit: Int
  ): RepositoryIO[List[ClaimedInterviewWorkflowCommand]]

  def renewPublication(claim: ClaimedInterviewWorkflowCommand, now: Instant, leaseUntil: Instant): RepositoryIO[Boolean]

  def markPublished(claim: ClaimedInterviewWorkflowCommand, publishedAt: Instant): RepositoryIO[Unit]

  def retry(
      claim: ClaimedInterviewWorkflowCommand,
      now: Instant,
      availableAt: Instant,
      failureCode: String
  ): RepositoryIO[Unit]

  def requireRepair(
      claim: ClaimedInterviewWorkflowCommand,
      now: Instant,
      failureCode: String
  ): RepositoryIO[InterviewPublicationResolution]
}
