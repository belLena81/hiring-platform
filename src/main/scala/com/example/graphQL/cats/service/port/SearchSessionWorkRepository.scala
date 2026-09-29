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
  require(
    event.eventType == OperationalEventType.SEARCH_PERFORMED &&
      event.aggregateType == OperationalAggregateType.Search &&
      event.actorId == session.actorId &&
      Parsing.parseUuid(event.aggregateId).toOption.contains(session.id),
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
  def enqueue(work: PendingSearchSessionWork, now: Instant): RepositoryIO[Unit]
  def findForActor(actorId: UserId, searchId: UUID): RepositoryIO[Option[SearchSessionLookup]]
  def claim(
      workerId: String,
      now: Instant,
      leaseUntil: Instant
  ): RepositoryIO[Option[ClaimedSearchSessionWork]]
  def complete(claim: ClaimedSearchSessionWork, now: Instant): RepositoryIO[Unit]
  def retry(claim: ClaimedSearchSessionWork, availableAt: Instant): RepositoryIO[Unit]
  def fail(
      claim: ClaimedSearchSessionWork,
      failure: SearchSessionWorkFailure,
      now: Instant
  ): RepositoryIO[Unit]
}
