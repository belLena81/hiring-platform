package com.example.graphQL.cats.service.application

import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.*
import java.time.Instant
import java.util.UUID

private[application] class TestInterviewWorkflowRepository extends InterviewWorkflowRepository {
  private def unexpected[A]: RepositoryIO[A] =
    RepositoryIO.lift(IO.raiseError(new AssertionError("Unexpected repository operation")))
  override def claimDueCommands(workerId: String, now: Instant, leaseUntil: Instant, limit: Int) =
    RepositoryIO.fromEither[List[ClaimedInterviewWorkflowCommand]](Left(RepositoryError.Unavailable))
  override def claimExecution(
      command: InterviewWorkflowCommandRecord,
      workerId: String,
      now: Instant,
      leaseUntil: Instant,
      maxAttempts: Int
  ) =
    unexpected[InterviewExecutionClaimOutcome]
  override def authorizePublication(
      claim: ClaimedInterviewWorkflowCommand,
      generation: InterviewPublisherGeneration,
      now: Instant
  ) = unexpected[Boolean]
  override def renewPublication(claim: ClaimedInterviewWorkflowCommand, now: Instant, leaseUntil: Instant) =
    unexpected[Boolean]
  override def attemptCount(workflowId: InterviewWorkflowId, command: InterviewWorkflowCommand) = unexpected[Long]
  override def quarantine(identity: String, now: Instant) = unexpected[Unit]
  override def recordResult(claim: ClaimedInterviewWorkflowCommand, result: InterviewCommandResult, now: Instant) =
    unexpected[Unit]
  override def findRequest(recruiterId: UserId, requestKey: UUID, fingerprint: MutationReceiptFingerprint) =
    unexpected[Option[InterviewWorkflow]]
  override def findCommand(workflowId: InterviewWorkflowId, stepId: String) =
    unexpected[Option[InterviewWorkflowCommandRecord]]
  override def repair(
      workflow: InterviewWorkflow,
      expectedRevision: Long,
      requestKey: UUID,
      now: Instant,
      actorId: UserId
  ) = unexpected[InterviewWorkflow]
  override def commitHiring(
      workflow: InterviewWorkflow,
      now: Instant,
      execution: Option[ClaimedInterviewWorkflowCommand]
  ) = unexpected[Unit]
  override def hasHiringReceipt(workflowId: InterviewWorkflowId) = unexpected[Boolean]
  override def create(
      workflow: InterviewWorkflow,
      initialCommand: InterviewWorkflowCommand,
      requestKey: UUID,
      fingerprint: MutationReceiptFingerprint,
      createdAt: Instant
  ) = unexpected[InterviewWorkflowAdvanceResult]
  override def findForActor(workflowId: InterviewWorkflowId, access: InterviewWorkflowAccess) =
    unexpected[Option[InterviewWorkflow]]
  override def findForAdmin(workflowId: InterviewWorkflowId) = unexpected[Option[InterviewWorkflow]]
  override def advance(
      workflow: InterviewWorkflow,
      expectedRevision: Long,
      cause: InterviewAdvanceCause,
      commands: List[InterviewWorkflowCommand],
      occurredAt: Instant,
      availableAt: Option[Instant]
  ) = unexpected[InterviewWorkflowAdvanceResult]
  override def markPublished(claim: ClaimedInterviewWorkflowCommand, publishedAt: Instant) = unexpected[Unit]
  override def retry(
      claim: ClaimedInterviewWorkflowCommand,
      now: Instant,
      availableAt: Instant,
      failureCode: String
  ) = unexpected[Unit]
  override def requireRepair(claim: ClaimedInterviewWorkflowCommand, now: Instant, failureCode: String) =
    unexpected[InterviewPublicationResolution]
}
