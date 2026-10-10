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

trait OperationalEventOutboxRepository {
  def claim(
      workerId: String,
      transactionalId: String,
      now: Instant,
      leaseUntil: Instant,
      limit: Int
  ): RepositoryIO[List[ClaimedOperationalEvent]]
  def renewLease(
      eventId: java.util.UUID,
      leaseToken: String,
      subjectIds: List[String],
      leaseUntil: Instant
  ): RepositoryIO[Unit]
  def markPublished(
      eventId: java.util.UUID,
      leaseToken: String,
      now: Instant,
      retentionExpiresAt: Instant
  ): RepositoryIO[Unit]
  def releaseForRetry(
      eventId: java.util.UUID,
      leaseToken: String,
      now: Instant,
      availableAt: Instant
  ): RepositoryIO[Unit]
  def markFailed(
      eventId: java.util.UUID,
      leaseToken: String,
      now: Instant,
      reason: String
  ): RepositoryIO[Unit]
}

trait ConsumerReceiptRepository {
  def exists(consumerGroup: String, eventId: java.util.UUID): RepositoryIO[Boolean]
  def record(
      consumerGroup: String,
      event: OperationalEventEnvelope,
      now: Instant,
      expiresAt: Instant
  ): RepositoryIO[Boolean]
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

trait EventQuarantineRepository {
  def save(record: EventQuarantineRecord): RepositoryIO[Unit]
}
