package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.model.AccountDeletionStatus
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.{AnalyticsReportSnapshot, RepositoryError}
import cats.Applicative
import java.time.Instant

trait AnalyticsErasureRequestRepository[F[_]] {

  /** Confirms an erasure worker is live before a new account deletion can mutate Mongo. */
  def workerReady(now: Instant): RepositoryF[F, Unit]

  /** Records the request in the caller's mutation transaction. Repeating the same request is intentionally idempotent
    * so receipt replay cannot create work twice.
    */
  def enqueue(userId: UserId, now: Instant, context: MutationWriteContext): RepositoryF[F, String]

  /** Returns lifecycle state only when the receipt belongs to the supplied subject. */
  def statusForSubject(
      userId: UserId,
      receiptId: String
  ): RepositoryF[F, AccountDeletionStatus]

  /** Removes durable outbox rows attributed to the subject after producer drain has been proven. */
  def purgeSubjectOutbox(userId: UserId): RepositoryF[F, Unit]

  /** Retains a completed tombstone through the replay horizon before TTL cleanup. */
  def markComplete(userId: UserId, now: Instant): RepositoryF[F, Unit]
}

object AnalyticsErasureRequestRepository {
  def unavailable[F[_]: Applicative]: AnalyticsErasureRequestRepository[F] = new AnalyticsErasureRequestRepository[F] {
    override def workerReady(now: Instant): RepositoryF[F, Unit] =
      RepositoryF.raise(RepositoryError.Unavailable)

    override def enqueue(
        userId: UserId,
        now: Instant,
        context: MutationWriteContext
    ): RepositoryF[F, String] =
      RepositoryF.raise(RepositoryError.Unavailable)

    override def statusForSubject(
        userId: UserId,
        receiptId: String
    ): RepositoryF[F, AccountDeletionStatus] =
      RepositoryF.raise(RepositoryError.Unavailable)

    override def purgeSubjectOutbox(userId: UserId): RepositoryF[F, Unit] =
      RepositoryF.raise(RepositoryError.Unavailable)

    override def markComplete(userId: UserId, now: Instant): RepositoryF[F, Unit] =
      RepositoryF.raise(RepositoryError.Unavailable)
  }
}

trait AnalyticsReportRepository[F[_]] {
  def latest: RepositoryF[F, Option[AnalyticsReportSnapshot]]
}
