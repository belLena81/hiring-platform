package com.example.graphQL.cats.repository.protocol

import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.shared.events.{OperationalAggregateType, OperationalEventEnvelope, OperationalEventType, SearchSession}

import java.time.Instant
import java.util.UUID

final case class PendingSearchSessionWork(session: SearchSession, event: OperationalEventEnvelope) {
  require(
    event.eventType == OperationalEventType.SEARCH_PERFORMED &&
      event.aggregateType == OperationalAggregateType.Search &&
      event.actorId == session.actorId &&
      scala.util.Try(UUID.fromString(event.aggregateId)).toOption.contains(session.id),
    "search session work event must address its search session"
  )
}

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

trait SearchSessionWorkRepository {
  def enqueue(work: PendingSearchSessionWork, now: Instant): IO[Either[RepositoryError, Unit]]
  def findForActor(actorId: UserId, searchId: UUID): IO[Either[RepositoryError, Option[SearchSessionLookup]]]
  def claim(workerId: String, now: Instant, leaseUntil: Instant): IO[Either[RepositoryError, Option[ClaimedSearchSessionWork]]]
  def complete(claim: ClaimedSearchSessionWork, now: Instant): IO[Either[RepositoryError, Unit]]
  def retry(claim: ClaimedSearchSessionWork, availableAt: Instant): IO[Either[RepositoryError, Unit]]
  def fail(claim: ClaimedSearchSessionWork, failure: SearchSessionWorkFailure, now: Instant): IO[Either[RepositoryError, Unit]]
}

object SearchSessionWorkRepository {
  val noop: SearchSessionWorkRepository = new SearchSessionWorkRepository {
    def enqueue(work: PendingSearchSessionWork, now: Instant): IO[Either[RepositoryError, Unit]] = IO.pure(Right(()))
    def findForActor(actorId: UserId, searchId: UUID): IO[Either[RepositoryError, Option[SearchSessionLookup]]] = IO.pure(Right(None))
    def claim(workerId: String, now: Instant, leaseUntil: Instant): IO[Either[RepositoryError, Option[ClaimedSearchSessionWork]]] = IO.pure(Right(None))
    def complete(claim: ClaimedSearchSessionWork, now: Instant): IO[Either[RepositoryError, Unit]] = IO.pure(Right(()))
    def retry(claim: ClaimedSearchSessionWork, availableAt: Instant): IO[Either[RepositoryError, Unit]] = IO.pure(Right(()))
    def fail(claim: ClaimedSearchSessionWork, failure: SearchSessionWorkFailure, now: Instant): IO[Either[RepositoryError, Unit]] = IO.pure(Right(()))
  }
}
