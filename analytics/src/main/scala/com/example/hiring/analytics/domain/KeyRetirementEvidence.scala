package com.example.hiring.analytics.domain

import java.time.Instant

/** Operator-attested evidence for the read-only key-retirement audit. Blank references mean "not provided". */
enum WriterDisposition {
  case Stopped
  case AccessRevoked
  case Active
  case Unknown
}

final case class WriterRecord(
    identity: String,
    disposition: WriterDisposition,
    evidenceReference: String
)

/** Inventory references must point to operator evidence covering deployments, jobs, and unmanaged writers. */
final case class WriterInventory(
    observedAt: Option[Instant],
    coverageReference: String,
    managed: Vector[WriterRecord],
    unmanaged: Vector[WriterRecord]
)

final case class KafkaRetentionEvidence(
    barrierOffset: Option[Long],
    earliestAvailableOffset: Option[Long],
    evidenceReference: String,
    partitions: Vector[(Int, Long, Long)] = Vector.empty
)

final case class RetentionHorizon(retainedUntil: Option[Instant], evidenceReference: String)

final case class RetentionEvidence(
    kafka: KafkaRetentionEvidence,
    deltaData: RetentionHorizon,
    deltaLogs: RetentionHorizon,
    reports: RetentionHorizon
)
