package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.RepositoryError
import cats.Applicative
import cats.data.EitherT
import com.example.graphQL.cats.domain.workflow.{InterviewSubjectCleanup, InterviewWorkflowId}
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

trait InterviewPublisherFencer[F[_]] {

  /** Success confirms broker fencing for every captured generation, including an otherwise idle producer. */
  def fence(transactionalIds: Vector[String]): RepositoryF[F, Unit]
}

enum InterviewCleanupUpdate {
  case Applied, StaleRevision
}

trait InterviewSubjectCleanupRepository[F[_]] {
  def producerBatch(subject: UserId): RepositoryF[F, Vector[String]]
  def markProducersFenced(subject: UserId, ids: Vector[String], now: Instant): RepositoryF[F, Unit]
  def pendingPage(cursor: Option[InterviewCleanupCursor], observedAt: Instant): RepositoryF[F, InterviewCleanupPage]
  def find(subject: UserId): RepositoryF[F, Option[InterviewSubjectCleanup]]
  def transition(
      expected: InterviewSubjectCleanup,
      next: InterviewSubjectCleanup
  ): RepositoryF[F, InterviewCleanupUpdate]
  def purge(subject: UserId): RepositoryF[F, Unit]
  def absent(subject: UserId): RepositoryF[F, Boolean]
}

/** A provider reservation that is still held for a workflow of a deleted subject. */
final case class InterviewLiveHold(workflowId: InterviewWorkflowId, reserveKey: String, cancelKey: String)

/** Confirms the cancellation of a live hold at whichever calendar provider owns it. Local evidence of a hold is purged
  * only after this answers with a confirmed cancellation, so deleting an account never leaks a provider slot. The
  * answer is idempotent per cancel key: a retry after a crash between the provider and the local write is safe.
  */
trait InterviewHoldCanceller[F[_]] {
  def cancel(hold: InterviewLiveHold, now: Instant): InterviewProviderF[F, InterviewCalendarCancellation]
}

object InterviewHoldCanceller {

  /** The platform's own calendar ledger is the provider: the cleanup writes the confirmed release into that ledger
    * itself, so there is no other system to ask. A deleted subject is fenced out of the ledger's normal cancel path.
    */
  def ledgerOwned[F[_]: Applicative]: InterviewHoldCanceller[F] = new InterviewHoldCanceller[F] {
    override def cancel(hold: InterviewLiveHold, now: Instant): InterviewProviderF[F, InterviewCalendarCancellation] =
      EitherT.rightT[F, InterviewProviderError](InterviewCalendarCancellation.Cancelled(now))
  }

  /** An external calendar provider: the same-key confirmed cancel, with no workflow execution to fence. */
  def external[F[_]](calendar: InterviewCalendarProvider[F]): InterviewHoldCanceller[F] =
    new InterviewHoldCanceller[F] {
      override def cancel(hold: InterviewLiveHold, now: Instant): InterviewProviderF[F, InterviewCalendarCancellation] =
        calendar.cancel(hold.workflowId, hold.cancelKey, now)
    }
}
