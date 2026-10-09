package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.model.Identifiers.UserId
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

/** A provider reservation that is still held for a workflow of a deleted subject. */
final case class InterviewLiveHold(workflowId: InterviewWorkflowId, reserveKey: String, cancelKey: String)

/** Confirms the cancellation of a live hold at whichever calendar provider owns it. Local evidence of a hold is purged
  * only after this answers with a confirmed cancellation, so deleting an account never leaks a provider slot. The
  * answer is idempotent per cancel key: a retry after a crash between the provider and the local write is safe.
  */
trait InterviewHoldCanceller {
  def cancel(hold: InterviewLiveHold, now: Instant): InterviewProviderIO[InterviewCalendarCancellation]
}

object InterviewHoldCanceller {

  /** The platform's own calendar ledger is the provider: the cleanup writes the confirmed release into that ledger
    * itself, so there is no other system to ask. A deleted subject is fenced out of the ledger's normal cancel path.
    */
  val ledgerOwned: InterviewHoldCanceller = new InterviewHoldCanceller {
    override def cancel(hold: InterviewLiveHold, now: Instant): InterviewProviderIO[InterviewCalendarCancellation] =
      EitherT.rightT(InterviewCalendarCancellation.Cancelled(now))
  }

  /** An external calendar provider: the same-key confirmed cancel, with no workflow execution to fence. */
  def external(calendar: InterviewCalendarProvider): InterviewHoldCanceller = new InterviewHoldCanceller {
    override def cancel(hold: InterviewLiveHold, now: Instant): InterviewProviderIO[InterviewCalendarCancellation] =
      calendar.cancel(hold.workflowId, hold.cancelKey, now)
  }
}
