package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import java.time.Instant
import scala.concurrent.duration.FiniteDuration

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

  /** Claims due events; every lease ends `leaseDuration` after the database time (never a caller instant). */
  def claim(
      workerId: String,
      transactionalId: String,
      now: Instant,
      leaseDuration: FiniteDuration,
      limit: Int
  ): RepositoryF[F, List[ClaimedOperationalEvent]]

  /** Extends the event and subject leases to `leaseDuration` after database time; `Conflict` when the lease is lost. */
  def renewLease(
      eventId: java.util.UUID,
      leaseToken: String,
      subjectIds: List[String],
      leaseDuration: FiniteDuration
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

/** Deduplicates consumed events in the store: a unique receipt id per consumer group and event decides which delivery
  * is first. There is deliberately no existence check, because a check followed by an action races with a second
  * delivery.
  */
trait ConsumerReceiptRepository[F[_]] {

  /** Inserts the receipt; `false` means another delivery of the same event already recorded it. */
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
