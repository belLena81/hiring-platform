package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.events.SearchSession

import java.time.Instant
import scala.concurrent.duration.FiniteDuration
import java.util.UUID

/** The durable work row holds only the session; its `SearchPerformed` event is derived when the work completes. */
final case class PendingSearchSessionWork(session: SearchSession)

enum SearchSessionWorkState {
  case Ready, Processing, Retry, Failed
}

enum SearchSessionWorkFailure {
  case RetryExhausted, InvalidWork
}

final case class ClaimedSearchSessionWork(work: PendingSearchSessionWork, attempts: Int, leaseToken: String)

enum SearchSessionLookup {
  case Materialized(session: SearchSession)
  case Pending
  case Failed
}

trait SearchSessionWorkRepository[F[_]] {
  def enqueue(work: PendingSearchSessionWork, now: Instant): RepositoryF[F, Unit]
  def findForActor(actorId: UserId, searchId: UUID): RepositoryF[F, Option[SearchSessionLookup]]

  /** Claims due work; the lease ends `leaseDuration` after the database time of the claim (never a caller instant). */
  def claim(
      workerId: String,
      now: Instant,
      leaseDuration: FiniteDuration
  ): RepositoryF[F, Option[ClaimedSearchSessionWork]]
  def complete(claim: ClaimedSearchSessionWork, now: Instant): RepositoryF[F, Unit]
  def retry(claim: ClaimedSearchSessionWork, availableAt: Instant): RepositoryF[F, Unit]
  def fail(
      claim: ClaimedSearchSessionWork,
      failure: SearchSessionWorkFailure,
      now: Instant
  ): RepositoryF[F, Unit]
}
