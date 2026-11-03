package com.example.graphQL.cats.domain.policy

import scala.concurrent.duration.*

/** Why the consumer-receipt window cannot guarantee that a receipt outlives every deliverable message. */
enum ConsumerReceiptWindowRefusal {

  /** Retention is infinite (`-1`) or otherwise not a bounded duration. */
  case RetentionUnbounded

  /** The receipt expires before the oldest message Kafka may still deliver. */
  case WindowExceedsReceiptTtl
}

/** A duplicate receipt key means "already handled" only while the receipt outlives every message Kafka can still
  * deliver. Time retention deletes a segment once its newest record is older than `retention.ms`, the active segment
  * included (the broker rolls an empty one first), so a record stays readable for at most `retention.ms + segment.ms`
  * even on a quiet topic; the margin covers the retention check interval and producer clock skew. Mongo's TTL monitor
  * only delays deletion, so it needs no margin.
  *
  * Consumer-group offset expiry (`offsets.retention.minutes`) and any offset reset only cause re-reads from the
  * earliest retained offset. Every re-read record is deduplicated by the same inequality, so the window does not depend
  * on offset retention and the gate does not read that broker setting (it would need cluster `DESCRIBE_CONFIGS`). It
  * does depend on `auto.offset.reset=earliest`: `latest` would lose events rather than duplicate them.
  */
object ConsumerReceiptWindow {
  val Margin: FiniteDuration = 1.hour

  def check(
      receiptTtl: FiniteDuration,
      retentionMs: Long,
      segmentMs: Long
  ): Either[ConsumerReceiptWindowRefusal, Unit] =
    if (retentionMs < 0L || segmentMs < 0L) Left(ConsumerReceiptWindowRefusal.RetentionUnbounded)
    else {
      val deliverableMs = BigInt(retentionMs) + BigInt(segmentMs) + BigInt(Margin.toMillis)
      Either.cond(
        BigInt(receiptTtl.toMillis) >= deliverableMs,
        (),
        ConsumerReceiptWindowRefusal.WindowExceedsReceiptTtl
      )
    }
}
