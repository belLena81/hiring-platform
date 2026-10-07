package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.InterviewSubjectCleanup
import java.time.Instant

/** Internal adapter token. It carries ordering coordinates, never authority to perform subject cleanup. */
opaque type InterviewCleanupCursor = String

object InterviewCleanupCursor {
  private[cats] def fromEncoded(value: String): InterviewCleanupCursor = value
  extension (value: InterviewCleanupCursor) private[cats] def encoded: String = value
}

final case class InterviewCleanupPage(
    entries: Vector[Either[RepositoryError, InterviewSubjectCleanup]],
    next: Option[InterviewCleanupCursor]
)

final case class InterviewCleanupProgress(
    next: Option[InterviewCleanupCursor],
    firstFailure: Option[RepositoryError]
)

type InterviewRetentionBarrier = com.example.graphQL.cats.domain.workflow.InterviewRetentionBarrier
val InterviewRetentionBarrier = com.example.graphQL.cats.domain.workflow.InterviewRetentionBarrier

trait InterviewPublisherFencer {

  /** Success confirms broker fencing for every captured generation, including an otherwise idle producer. */
  def fence(transactionalIds: Vector[String]): RepositoryIO[Unit]
}

enum InterviewCleanupUpdate {
  case Applied, StaleRevision
}

trait InterviewSubjectCleanupRepository {
  def producerBatch(subject: UserId): RepositoryIO[Vector[String]]
  def markProducersFenced(subject: UserId, ids: Vector[String], now: Instant): RepositoryIO[Unit]
  def pendingPage(cursor: Option[InterviewCleanupCursor], observedAt: Instant): RepositoryIO[InterviewCleanupPage]
  def find(subject: UserId): RepositoryIO[Option[InterviewSubjectCleanup]]
  def transition(expected: InterviewSubjectCleanup, next: InterviewSubjectCleanup): RepositoryIO[InterviewCleanupUpdate]
  def purge(subject: UserId): RepositoryIO[Unit]
  def absent(subject: UserId): RepositoryIO[Boolean]
}
