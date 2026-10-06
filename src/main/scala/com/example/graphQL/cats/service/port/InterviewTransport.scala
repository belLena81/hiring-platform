package com.example.graphQL.cats.service.port

import cats.effect.IO
import java.time.Instant
import java.util.UUID

enum InterviewStep {
  case Reserve, LookupReservation, CommitHiring, LookupCommit, Release
  case NotifyCandidate, NotifyRecruiter, LookupNotifyCandidate, LookupNotifyRecruiter, RequireRepair
}
enum InterviewResult {
  case Succeeded, Rejected, OutcomeUnknown, Found, Absent
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

  /** Send completion means Kafka acknowledged publication; caller then durably marks its intent sent. */
  def publish(message: InterviewMessage): IO[Unit]
}
