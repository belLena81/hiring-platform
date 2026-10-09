package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.{ApplicationStatus, UserRole}
import com.example.graphQL.cats.domain.workflow.{
  InterviewAdvanceCause,
  InterviewCommand,
  InterviewLifecycleEvent,
  InterviewWorkflow,
  InterviewWorkflowCommand,
  InterviewWorkflowError,
  InterviewWorkflowId
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

/** A durable scheduling, cancellation or rescheduling intent; one executor claims, runs and settles every family. */
final case class InterviewWorkflowCommandRecord(
    workflowId: InterviewWorkflowId,
    stepId: String,
    revision: Long,
    command: InterviewCommand,
    state: InterviewWorkflowCommandState,
    publicationAttempts: Int,
    availableAt: Instant,
    occurredAt: Instant,
    result: Option[InterviewCommandResult] = None,
    executionAttempts: Int = 0
)

/** Who drives a lifecycle transition. An actor request carries the trusted actor and its idempotent request identity;
  * an internal transition carries the receipt identity of its cause.
  */
enum InterviewLifecycleOrigin {
  case Actor(access: InterviewWorkflowAccess, requestKey: UUID, fingerprint: MutationReceiptFingerprint)
  case Internal(cause: InterviewAdvanceCause)
}

enum InterviewLifecycleOutcome {
  case Applied(workflow: InterviewWorkflow)

  /** The same request or cause was already applied; the stored workflow is returned unchanged. */
  case Duplicate(existing: InterviewWorkflow)

  /** The pure policy refused the event; nothing was written. */
  case Rejected(error: InterviewWorkflowError)

  /** A cancellation requires an `Interview` application; nothing was written. */
  case ApplicationNotInterview(status: ApplicationStatus)

  /** The workflow does not exist or the actor is not one of its participants; existence is not revealed. */
  case NotVisible
}

/** What happens to an informational notification whose delivery did not succeed. It never changes the workflow phase.
  */
enum InterviewNotificationSettlement {

  /** Deliver the same command again, not before `availableAt`. */
  case Retry(availableAt: Instant)

  /** The retry budget is spent: the command alone becomes `RepairRequired`, visible to Admin. */
  case Exhausted(failureCode: String)
}

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
  def attemptCount(workflowId: InterviewWorkflowId, command: InterviewCommand): RepositoryIO[Long]
  def quarantine(identity: String, now: Instant): RepositoryIO[Unit]
  def recordResult(
      claim: ClaimedInterviewWorkflowCommand,
      result: InterviewCommandResult,
      now: Instant
  ): RepositoryIO[Unit]
  def findRequest(
      actorId: UserId,
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

  /** The one canonical write path for cancel, reschedule request and proposal transitions.
    *
    * In one guarded transaction it fences the participants, resolves the request or cause receipt, loads the workflow
    * under the actor's participant predicate, runs the pure `InterviewLifecyclePolicy` against `expectedRevision`, and
    * persists the next workflow state, receipt and lifecycle command intents. A `Cancel` event additionally applies the
    * existing `Interview -> Rejected` application transition (history with the generated feedback and the
    * `statusChanged` operational event) in the same transaction, so a lost race writes nothing.
    */
  def applyLifecycle(
      workflowId: InterviewWorkflowId,
      expectedRevision: Long,
      event: InterviewLifecycleEvent,
      origin: InterviewLifecycleOrigin,
      now: Instant,
      availableAt: Option[Instant]
  ): RepositoryIO[InterviewLifecycleOutcome]

  /** The guarded part of the reschedule swap, run by the executor while the workflow is `RescheduleSwapPending`.
    *
    * In one transaction it fences the participants and the executing claim, and requires the application to still be
    * `Interview`, the replacement hold and the old hold to be live, and the old interval not to have started. It then
    * records the approval receipt for the replacement generation. The live interval moves only when the resulting
    * `SwapCommitted` event is applied through `applyLifecycle`. A refused guard is `RepositoryError.Conflict`.
    */
  def approveRescheduledInterval(
      workflow: InterviewWorkflow,
      now: Instant,
      execution: ClaimedInterviewWorkflowCommand
  ): RepositoryIO[Unit]

  def hasRescheduleApproval(workflowId: InterviewWorkflowId, generation: Int): RepositoryIO[Boolean]

  /** Settles a failed informational notification (retry later, or command-level repair) without touching the workflow.
    * Idempotent: `false` means the command was no longer in a failed-result state.
    */
  def settleNotificationResult(
      record: InterviewWorkflowCommandRecord,
      settlement: InterviewNotificationSettlement,
      now: Instant
  ): RepositoryIO[Boolean]

  /** Puts a proposal-expiry row back to `Pending`, available at `availableAt` (not before its own expiry time), because
    * its message arrived before that time (clock skew). Rows that are pending, published or still claimed by the
    * publisher qualify; a claiming publisher's later `markPublished` then fails its claim check harmlessly. `false`
    * means the row is in none of those states (already settled, superseded or gone).
    */
  def deferExpiry(record: InterviewWorkflowCommandRecord, availableAt: Instant): RepositoryIO[Boolean]

  /** Informational notification commands of a workflow that are in command-level `RepairRequired` (bounded). */
  def findNotificationRepairs(workflowId: InterviewWorkflowId): RepositoryIO[List[InterviewWorkflowCommandRecord]]

  /** Admin repair of command-level notification failures: requeues them with a fresh attempt budget and records the
    * actor. A repeated request key returns the earlier count. Returns the number of requeued commands.
    */
  def repairNotifications(
      workflowId: InterviewWorkflowId,
      requestKey: UUID,
      now: Instant,
      actorId: UserId
  ): RepositoryIO[Int]

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
