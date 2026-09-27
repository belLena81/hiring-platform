package com.example.hiring.analytics.erasure

import cats.effect.IO

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** Persistence operations required by the resumable erasure lifecycle. */
trait AnalyticsErasureStore {
  def claim(now: Instant, leaseUntil: Instant, limit: Int): IO[Vector[ErasureClaim]]
  def publisherDrainReady(subjectId: String, now: Instant, deliveryTimeout: FiniteDuration): IO[Boolean]
  def transactionalIds(requestId: String): IO[Vector[String]]
  def purgeOutbox(subjectId: String, now: Instant, deliveryTimeout: FiniteDuration): IO[Boolean]
  def persistBarrier(claim: ErasureClaim, barrier: KafkaRetentionBarrier, now: Instant): IO[Boolean]
  def readBarrier(requestId: String): IO[Option[KafkaRetentionBarrier]]
  def persistDeltaPurgedAt(claim: ErasureClaim, at: Instant, now: Instant): IO[Boolean]
  def persistDeltaGeneration(claim: ErasureClaim, generation: Long, now: Instant): IO[Boolean]
  def persistAffectedRows(claim: ErasureClaim, affectedRows: Long, now: Instant): IO[Boolean]
  def persistDeltaFiles(claim: ErasureClaim, files: Vector[String], now: Instant): IO[Boolean]
  def readDeltaFiles(requestId: String): IO[Vector[String]]
  def readAffectedRows(requestId: String): IO[Long]
  def readDeltaGeneration(requestId: String): IO[Option[Long]]
  def readDeltaPurgedAt(requestId: String): IO[Option[Instant]]
  def releaseForOtherRequests(claim: ErasureClaim, now: Instant): IO[Boolean]
  def recordFailure(
      claim: ErasureClaim,
      category: ErasureFailureCategory,
      attempt: Int,
      retryAt: Option[Instant],
      now: Instant
  ): IO[Boolean]
  def defer(claim: ErasureClaim, resumeAt: Instant, now: Instant): IO[Boolean]
  def hasNonReadyOtherRequests(requestId: String): IO[Boolean]
  def renew(claim: ErasureClaim, now: Instant, leaseUntil: Instant): IO[Boolean]
  def advance(claim: ErasureClaim, phase: ErasurePhase, progress: Int, now: Instant): IO[Boolean]
  def heartbeat(now: Instant, leaseUntil: Instant): IO[Unit]
  def preflight: IO[Unit]
}

/** Fixed, non-sensitive operator-facing failure labels. Never persist exception messages. */
enum ErasureFailureCategory(val persistedName: String) {
  case TransientStorage extends ErasureFailureCategory("TRANSIENT_STORAGE")
  case TransientSource extends ErasureFailureCategory("TRANSIENT_SOURCE")
  case InvalidState extends ErasureFailureCategory("INVALID_STATE")
  case Unknown extends ErasureFailureCategory("UNKNOWN")
}
