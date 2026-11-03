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
  * deliver. Kafka deletes closed segments only, so a record stays readable for `retention.ms + segment.ms`; the margin
  * covers the retention check interval and clock skew. Mongo's TTL monitor only delays deletion, so it needs no margin.
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
