package com.example.graphQL.cats.service.port

import cats.effect.IO
import java.time.Instant
import java.util.UUID

enum InterviewStep {
  case Reserve, LookupReservation, CommitHiring, LookupCommit, Release
  case NotifyCandidate, NotifyRecruiter, LookupNotifyCandidate, LookupNotifyRecruiter, RequireRepair

  /** Cancellation and rescheduling coordination. Notification kinds ride on the stored command, not on the step. */
  case CancelSlot, LookupCancellation, HoldReplacement, LookupReplacementHold, CommitReschedule, LookupRescheduleCommit,
    ExpireProposal
}
enum InterviewResult {
  case Succeeded, Rejected, OutcomeUnknown, Found, Absent
}

enum InterviewPublisherRole(val transactionalIdPrefix: String) {
  case Orchestrator extends InterviewPublisherRole("hiring-interview-orchestrator-")
  case Worker extends InterviewPublisherRole("hiring-interview-worker-")
}

/** One initialized producer lifetime. A replacement receives a fresh identity and fresh authorization. */
final case class InterviewPublisherGeneration(role: InterviewPublisherRole, id: UUID) {
  def transactionalId: String = s"${role.transactionalIdPrefix}$id"
}

/** Fatal producer protocol failure, translated once by the Kafka adapter and handled by its resource owner. */
final case class InterviewProducerGenerationFenced(cause: Throwable)
    extends RuntimeException("interview producer generation was fenced", cause)

object InterviewResult {
  def fromCommandResult(value: InterviewCommandResult): InterviewResult = value match {
    case InterviewCommandResult.Succeeded      => Succeeded
    case InterviewCommandResult.Rejected       => Rejected
    case InterviewCommandResult.OutcomeUnknown => OutcomeUnknown
    case InterviewCommandResult.Found          => Found
    case InterviewCommandResult.Absent         => Absent
  }
}

/** A coordination envelope: participant identities and scheduling details stay in MongoDB. */
final case class InterviewMessage(
    messageId: UUID,
    workflowId: UUID,
    stepId: String,
    step: InterviewStep,
    revision: Long,
    causationId: UUID,
    deadline: Instant,
    result: Option[InterviewResult],
    occurredAt: Instant
)
trait InterviewTransport {

  /** This identity and publish must belong to the same immutable initialized producer resource. */
  def generationFor(message: InterviewMessage): InterviewPublisherGeneration

  /** Send completion means Kafka acknowledged publication; caller then durably marks its intent sent. */
  def publish(message: InterviewMessage): IO[Unit]
}
