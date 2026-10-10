package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.model.AccountDeletionStatus
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.{AnalyticsReportSnapshot, RepositoryError}
import java.time.Instant

trait AnalyticsErasureRequestRepository {

  /** Confirms an erasure worker is live before a new account deletion can mutate Mongo. */
  def workerReady(now: Instant): RepositoryIO[Unit]

  /** Records the request in the caller's mutation transaction. Repeating the same request is intentionally idempotent
    * so receipt replay cannot create work twice.
    */
  def enqueue(userId: UserId, now: Instant, context: MutationWriteContext): RepositoryIO[String]

  /** Returns lifecycle state only when the receipt belongs to the supplied subject. */
  def statusForSubject(
      userId: UserId,
      receiptId: String
  ): RepositoryIO[AccountDeletionStatus]

  /** Removes durable outbox rows attributed to the subject after producer drain has been proven. */
  def purgeSubjectOutbox(userId: UserId): RepositoryIO[Unit]

  /** Retains a completed tombstone through the replay horizon before TTL cleanup. */
  def markComplete(userId: UserId, now: Instant): RepositoryIO[Unit]
}

object AnalyticsErasureRequestRepository {
  val unavailable: AnalyticsErasureRequestRepository = new AnalyticsErasureRequestRepository {
    override def workerReady(now: Instant): RepositoryIO[Unit] =
      RepositoryIO.raise(RepositoryError.Unavailable)

    override def enqueue(
        userId: UserId,
        now: Instant,
        context: MutationWriteContext
    ): RepositoryIO[String] =
      RepositoryIO.raise(RepositoryError.Unavailable)

    override def statusForSubject(
        userId: UserId,
        receiptId: String
    ): RepositoryIO[AccountDeletionStatus] =
      RepositoryIO.raise(RepositoryError.Unavailable)

    override def purgeSubjectOutbox(userId: UserId): RepositoryIO[Unit] =
      RepositoryIO.raise(RepositoryError.Unavailable)

    override def markComplete(userId: UserId, now: Instant): RepositoryIO[Unit] =
      RepositoryIO.raise(RepositoryError.Unavailable)
  }
}

trait AnalyticsReportRepository {
  def latest: RepositoryIO[Option[AnalyticsReportSnapshot]]
}
