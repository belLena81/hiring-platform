package com.example.graphQL.cats.service.application

import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import java.time.Instant
import scala.concurrent.duration.*
import java.util.UUID

private[cats] class TestInterviewWorkflowRepository extends InterviewWorkflowRepository[IO] {
  private def unexpected[A]: RepositoryF[IO, A] =
    RepositoryF.lift(IO.raiseError(new AssertionError("Unexpected repository operation")))
  override def claimDueCommands(workerId: String, now: Instant, leaseDuration: FiniteDuration, limit: Int) =
    RepositoryF.at[IO].fromEither[List[ClaimedInterviewWorkflowCommand]](Left(RepositoryError.Unavailable))
  override def claimExecution(
      command: InterviewWorkflowCommandRecord,
      workerId: String,
      now: Instant,
      leaseDuration: FiniteDuration,
      maxAttempts: Int
  ) =
    unexpected[InterviewExecutionClaimOutcome]
  override def authorizePublication(
      claim: ClaimedInterviewWorkflowCommand,
      generation: InterviewPublisherGeneration,
      now: Instant
  ) = unexpected[Boolean]
  override def renewPublication(claim: ClaimedInterviewWorkflowCommand, leaseDuration: FiniteDuration) =
    unexpected[Boolean]
  override def attemptCount(workflowId: InterviewWorkflowId, command: InterviewCommand) = unexpected[Long]
  override def quarantine(identity: String, now: Instant) = unexpected[Unit]
  override def recordResult(claim: ClaimedInterviewWorkflowCommand, result: InterviewResult, now: Instant) =
    unexpected[Unit]
  override def findCommand(workflowId: InterviewWorkflowId, stepId: String) =
    unexpected[Option[InterviewWorkflowCommandRecord]]
  override def applyLifecycle(
      workflowId: InterviewWorkflowId,
      expectedRevision: Long,
      event: InterviewLifecycleEvent,
      origin: InterviewLifecycleOrigin,
      now: Instant,
      availableAt: Option[Instant],
      context: MutationWriteContext
  ) = unexpected[InterviewLifecycleOutcome]
  override def approveRescheduledInterval(
      workflow: InterviewWorkflow,
      now: Instant,
      execution: ClaimedInterviewWorkflowCommand
  ) = unexpected[Unit]
  override def hasRescheduleApproval(workflowId: InterviewWorkflowId, generation: Int) = unexpected[Boolean]
  override def settleNotificationResult(
      record: InterviewWorkflowCommandRecord,
      settlement: InterviewNotificationSettlement,
      now: Instant
  ) = unexpected[Boolean]
  override def deferExpiry(record: InterviewWorkflowCommandRecord, availableAt: Instant) = unexpected[Boolean]
  override def findNotificationRepairs(workflowId: InterviewWorkflowId) =
    unexpected[List[InterviewWorkflowCommandRecord]]
  override def repairNotifications(workflowId: InterviewWorkflowId, requestKey: UUID, now: Instant, actorId: UserId) =
    unexpected[Int]
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
  override def isHiringCommitted(workflowId: InterviewWorkflowId) = unexpected[Boolean]
  override def create(
      workflow: InterviewWorkflow,
      initialCommand: InterviewWorkflowCommand,
      createdAt: Instant,
      context: MutationWriteContext
  ) = unexpected[Unit]
  override def findForActor(workflowId: InterviewWorkflowId, access: InterviewWorkflowAccess) =
    unexpected[Option[InterviewWorkflow]]
  override def findForAdmin(workflowId: InterviewWorkflowId) = unexpected[Option[InterviewWorkflow]]
  override def findByApplication(applicationId: ApplicationId, access: InterviewWorkflowAccess) =
    unexpected[Option[InterviewWorkflow]]
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
      availableAt: Instant,
      failureCode: InterviewFailureCode
  ) = unexpected[Unit]
  override def requireRepair(claim: ClaimedInterviewWorkflowCommand, now: Instant, failureCode: InterviewFailureCode) =
    unexpected[InterviewPublicationResolution]
}
