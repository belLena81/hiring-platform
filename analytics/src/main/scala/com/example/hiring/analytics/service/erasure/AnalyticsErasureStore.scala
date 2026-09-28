package com.example.hiring.analytics.service.erasure

import com.example.hiring.analytics.domain.AccountSubjectId

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** Queueing, publisher-drain, and worker-liveness operations for erasure requests. */
trait ErasureQueue[F[_]] {
  def claim(now: Instant, leaseUntil: Instant, limit: Int): F[Vector[ErasureClaim]]
  def publisherDrainReady(subjectId: AccountSubjectId, now: Instant, deliveryTimeout: FiniteDuration): F[Boolean]
  def transactionalIds(requestId: AccountSubjectId): F[Vector[String]]
  def purgeOutbox(subjectId: AccountSubjectId, now: Instant, deliveryTimeout: FiniteDuration): F[Boolean]
  def hasNonReadyOtherRequests(requestId: AccountSubjectId): F[Boolean]
  def heartbeat(now: Instant, leaseUntil: Instant): F[Unit]
  def preflight: F[Unit]
}

/** Durable barriers and erasure completion evidence. */
trait ErasureBarrier[F[_]] {
  def persistBarrier(claim: ErasureClaim, barrier: KafkaRetentionBarrier, now: Instant): F[ErasureUpdate]
  def readBarrier(requestId: AccountSubjectId): F[Option[KafkaRetentionBarrier]]
}

/** Lease-owned lifecycle progress. A failed match means the claim no longer owns the request. */
trait ErasureProgress[F[_]] {
  def persistDeltaPurgedAt(claim: ErasureClaim, at: Instant, now: Instant): F[ErasureUpdate]
  def persistDeltaGeneration(claim: ErasureClaim, generation: Long, now: Instant): F[ErasureUpdate]
  def persistAffectedRows(claim: ErasureClaim, affectedRows: Long, now: Instant): F[ErasureUpdate]
  def persistDeltaFiles(claim: ErasureClaim, files: Vector[String], now: Instant): F[ErasureUpdate]
  def readDeltaFiles(requestId: AccountSubjectId): F[Vector[String]]
  def readAffectedRows(requestId: AccountSubjectId): F[Long]
  def readDeltaGeneration(requestId: AccountSubjectId): F[Option[Long]]
  def readDeltaPurgedAt(requestId: AccountSubjectId): F[Option[Instant]]
  def releaseForOtherRequests(claim: ErasureClaim, now: Instant): F[ErasureUpdate]
  def recordFailure(
      claim: ErasureClaim,
      category: ErasureFailureCategory,
      attempt: Int,
      retryAt: Option[Instant],
      now: Instant
  ): F[ErasureUpdate]
  def defer(claim: ErasureClaim, resumeAt: Instant, now: Instant): F[ErasureUpdate]
  def renew(claim: ErasureClaim, now: Instant, leaseUntil: Instant): F[ErasureUpdate]
  def advance(claim: ErasureClaim, phase: ErasurePhase, progress: Int, now: Instant): F[ErasureUpdate]
}

enum ErasureUpdate {
  case Applied
  case LeaseLost
}

/** Persisted lifecycle state for an account-erasure request. */
enum ErasureRequestState(val persistedName: String) {
  case Pending extends ErasureRequestState("Pending")
  case Processing extends ErasureRequestState("Processing")
  case Complete extends ErasureRequestState("Complete")
}

object ErasureRequestState {
  def fromString(value: String): Option[ErasureRequestState] =
    values.find(_.persistedName == value)
}

/** Fixed, non-sensitive operator-facing failure labels. Never persist exception messages. */
enum ErasureFailureCategory(val persistedName: String) {
  case TransientStorage extends ErasureFailureCategory("TRANSIENT_STORAGE")
  case TransientSource extends ErasureFailureCategory("TRANSIENT_SOURCE")
  case InvalidState extends ErasureFailureCategory("INVALID_STATE")
  case Unknown extends ErasureFailureCategory("UNKNOWN")
}
