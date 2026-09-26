package com.example.hiring.analytics.erasure

import java.time.Instant

final case class ErasureClaim(
    requestId: String,
    leaseToken: String,
    leaseUntil: Instant,
    phase: ErasurePhase,
    progress: Int,
    progressKey: Long
) {
  def advanceTo(nextPhase: ErasurePhase): Either[String, ErasureClaim] =
    phase.next match {
      case Some(expected) if expected == nextPhase =>
        Right(
          copy(phase = nextPhase, progress = 0, progressKey = nextPhase.ordinal.toLong * ErasurePhase.ProgressPerPhase)
        )
      case _ => Left(s"${phase.persistedName} cannot advance to ${nextPhase.persistedName}")
    }
}

/** Ordered durable stages; the worker owns the meaning and idempotent action of each stage. */
enum ErasurePhase(val persistedName: String) {
  case Requested extends ErasurePhase("Requested")
  case PublisherDrained extends ErasurePhase("PublisherDrained")
  case OutboxPurged extends ErasurePhase("OutboxPurged")
  case DeltaPurged extends ErasurePhase("DeltaPurged")
  case GoldRebuilt extends ErasurePhase("GoldRebuilt")
  case ReadyToPublish extends ErasurePhase("ReadyToPublish")
  case ReportPublished extends ErasurePhase("ReportPublished")

  def next: Option[ErasurePhase] = ErasurePhase.values.lift(ordinal + 1)
}

object ErasurePhase {
  val ProgressPerPhase: Int = 1000000

  def fromString(value: String): Option[ErasurePhase] = values.find(_.persistedName == value)
}
