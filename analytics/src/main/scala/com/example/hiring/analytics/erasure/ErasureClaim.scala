package com.example.hiring.analytics.erasure

import java.time.Instant

final case class ErasureClaim(
    requestId: String,
    leaseToken: String,
    leaseUntil: Instant,
    phase: ErasurePhase,
    progress: Int,
    progressKey: Long
)

/** Ordered durable stages; the worker owns the meaning and idempotent action of each stage. */
enum ErasurePhase {
  case Requested
  case PublisherDrained
  case OutboxPurged
  case DeltaPurged
  case GoldRebuilt
  case ReadyToPublish
  case ReportPublished

  def precedes(other: ErasurePhase): Boolean = ordinal < other.ordinal
}

object ErasurePhase {
  val ProgressPerPhase: Int = 1000000

  def fromString(value: String): Option[ErasurePhase] = values.find(_.toString == value)
}
