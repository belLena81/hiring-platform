package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import java.time.Instant

final case class ClaimedOperationalEvent(
    event: OperationalEventEnvelope,
    envelopeBytes: Array[Byte],
    partitionKey: String,
    leaseToken: String,
    attempts: Int,
    subjectIds: List[String] = Nil
)

enum OperationalEventFailureCategory {
  case MalformedEnvelope, UnsupportedVersion, InvalidOrdering, ConsumerFailure
}

trait OperationalEventOutboxRepository[F[_]] {
  def claim(
      workerId: String,
      transactionalId: String,
      now: Instant,
      leaseUntil: Instant,
      limit: Int
  ): RepositoryF[F, List[ClaimedOperationalEvent]]
  def renewLease(
      eventId: java.util.UUID,
      leaseToken: String,
      subjectIds: List[String],
      leaseUntil: Instant
  ): RepositoryF[F, Unit]
  def markPublished(
      eventId: java.util.UUID,
      leaseToken: String,
      now: Instant,
      retentionExpiresAt: Instant
  ): RepositoryF[F, Unit]
  def releaseForRetry(
      eventId: java.util.UUID,
      leaseToken: String,
      now: Instant,
      availableAt: Instant
  ): RepositoryF[F, Unit]
  def markFailed(
      eventId: java.util.UUID,
      leaseToken: String,
      now: Instant,
      reason: String
  ): RepositoryF[F, Unit]
}

trait ConsumerReceiptRepository[F[_]] {
  def exists(consumerGroup: String, eventId: java.util.UUID): RepositoryF[F, Boolean]
  def record(
      consumerGroup: String,
      event: OperationalEventEnvelope,
      now: Instant,
      expiresAt: Instant
  ): RepositoryF[F, Boolean]
}

final case class EventQuarantineRecord(
    topic: String,
    partition: Int,
    offset: Long,
    category: OperationalEventFailureCategory,
    reason: String,
    rawBytes: Array[Byte],
    occurredAt: Instant,
    expiresAt: Instant
)

trait EventQuarantineRepository[F[_]] {
  def save(record: EventQuarantineRecord): RepositoryF[F, Unit]
}
