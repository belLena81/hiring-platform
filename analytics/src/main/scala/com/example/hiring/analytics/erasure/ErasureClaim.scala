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
enum ErasurePhase(val persistedName: String) {
  case Requested extends ErasurePhase("Requested")
  case PublisherDrained extends ErasurePhase("PublisherDrained")
  case OutboxPurged extends ErasurePhase("OutboxPurged")
  case DeltaPurged extends ErasurePhase("DeltaPurged")
  case GoldRebuilt extends ErasurePhase("GoldRebuilt")
  case ReadyToPublish extends ErasurePhase("ReadyToPublish")
  case ReportPublished extends ErasurePhase("ReportPublished")

  def precedes(other: ErasurePhase): Boolean = ordinal < other.ordinal
}

object ErasurePhase {
  val ProgressPerPhase: Int = 1000000

  def fromString(value: String): Option[ErasurePhase] = values.find(_.persistedName == value)
}
