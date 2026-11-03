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
  *
  * The receipt is the whole consumer effect today, so a duplicate key means "already handled" only while the receipt
  * outlives every message Kafka can still deliver (`ConsumerReceiptWindow`, enforced when the consumer starts).
  *
  * Effect rule: an effect keyed by the event id is written in the same transaction as the receipt insert. Recording
  * first loses the event on a crash; applying first applies it twice.
  *
  * Revisit trigger: the first consumer effect that is not the receipt. That change introduces, with the effect, the
  * transaction (`MongoTransactionRunner.sessions`) and a crash/duplicate test of the atomic write. If a closed type is
  * wanted then, it is `enum ConsumedEventEffect { case ReceiptOnly }` passed to `record` and matched without a wildcard
  * so `-Werror` rejects an unhandled case.
  */
trait ConsumerReceiptRepository[F[_]] {

  /** Inserts the receipt; `false` means another delivery of the same event already recorded it.
    *
    * A duplicate delivery extends the stored `expiresAt` to the later of the stored and given value (DDC-02.13). An
    * outbox row whose lease expires after a successful send is sent again as a new Kafka record with the same event id,
    * possibly much later; its receipt must cover that later copy, so expiry is measured from the latest delivery and is
    * never shortened. A failure of that extension is `Unavailable`, so the offset is not committed and the record is
    * delivered again.
    */
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
