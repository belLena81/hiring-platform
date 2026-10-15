package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.events.{
  OperationalAggregateType,
  OperationalEventEnvelope,
  OperationalEventType,
  SearchSession
}
import com.example.graphQL.cats.shared.Parsing

import java.time.Instant
import java.util.UUID

final case class PendingSearchSessionWork(session: SearchSession, event: OperationalEventEnvelope) {
  def addressesSession: Boolean =
    event.eventType == OperationalEventType.SEARCH_PERFORMED &&
      event.aggregateType == OperationalAggregateType.Search &&
      event.actorId == session.actorId &&
      Parsing.parseUuid(event.aggregateId).toOption.contains(session.id)
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

trait SearchSessionWorkRepository[F[_]] {
  def enqueue(work: PendingSearchSessionWork, now: Instant): RepositoryF[F, Unit]
  def findForActor(actorId: UserId, searchId: UUID): RepositoryF[F, Option[SearchSessionLookup]]
  def claim(
      workerId: String,
      now: Instant,
      leaseUntil: Instant
  ): RepositoryF[F, Option[ClaimedSearchSessionWork]]
  def complete(claim: ClaimedSearchSessionWork, now: Instant): RepositoryF[F, Unit]
  def retry(claim: ClaimedSearchSessionWork, availableAt: Instant): RepositoryF[F, Unit]
  def fail(
      claim: ClaimedSearchSessionWork,
      failure: SearchSessionWorkFailure,
      now: Instant
  ): RepositoryF[F, Unit]
}
