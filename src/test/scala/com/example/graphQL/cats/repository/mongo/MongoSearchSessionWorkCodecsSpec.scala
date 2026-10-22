package com.example.graphQL.cats.repository.mongo

import cats.data.NonEmptyList
import com.example.graphQL.cats.repository.mongo.MongoStoredDocumentDecoding.toStored
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.events.OperationalEventPayload.SearchKind
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.port.PendingSearchSessionWork
import com.example.graphQL.cats.service.events.{
  OperationalEvents,
  SearchFilterSnapshot,
  SearchSession,
  SearchSessionResult
}
import munit.FunSuite

import java.time.Instant
import java.util.UUID

class MongoSearchSessionWorkCodecsSpec extends FunSuite {
  private val actorId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000801"))
  private val searchId = UUID.fromString("00000000-0000-0000-0000-000000000802")
  private val now = Instant.parse("2026-09-22T08:00:00Z")

  private def session(query: Option[String], results: List[SearchSessionResult] = Nil) = SearchSession(
    searchId,
    actorId,
    SearchKind.SemanticJobSearch,
    query,
    SearchFilterSnapshot.Unfiltered,
    None,
    results,
    now,
    now.plusSeconds(3600)
  )

  test("AC-05.1 work documents retain a query-free session and no copy of the event") {
    val withQuery = session(
      Some("private candidate query"),
      List(SearchSessionResult("00000000-0000-0000-0000-000000000804", 1, 0.9d))
    )
    val stored = MongoSearchSessionWorkCodecs.work(PendingSearchSessionWork(withQuery), now)

    assert(!stored.toJson.contains("private candidate query"))
    assert(!stored.containsKey("event"))
    assertEquals(MongoSearchSessionWorkCodecs.readWork(stored).toEither.map(_.session.query), Right(None))
    assertEquals(
      MongoSearchSessionWorkCodecs.readWork(stored).toEither.map(_.session.results),
      Right(withQuery.results)
    )
  }

  test("AC-05.1 the event is a function of the stored session, so rebuilding it twice gives the same envelope") {
    val value = session(None, List(SearchSessionResult("00000000-0000-0000-0000-000000000804", 1, 0.9d)))
    val stored = MongoSearchSessionWorkCodecs.work(PendingSearchSessionWork(value), now)
    val read = MongoSearchSessionWorkCodecs.readWork(stored).toEither.map(_.session)
    assertEquals(read, Right(value))
    val first = read.flatMap(OperationalEvents.searchPerformed(_).left.map(_.toString))
    assertEquals(first, OperationalEvents.searchPerformed(value).left.map(_.toString))
    assertEquals(first.map(_.eventId), Right(OperationalEvents.searchPerformedEventId(searchId)))
  }

  test("work attempt counts require BSON int32 values") {
    val stored = MongoSearchSessionWorkCodecs.work(PendingSearchSessionWork(session(None)), now)
    stored.put(MongoFields.Attempts, Long.box(0L))
    stored.put(MongoFields.LeaseToken, "lease-token")

    assertEquals(
      MongoSearchSessionWorkCodecs.readClaim(stored).toEither,
      Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InvalidField(MongoFields.Attempts)))
    )
  }

  test("a stored session that still carries its query returns a typed inconsistency") {
    val stored = MongoSearchSessionWorkCodecs.work(PendingSearchSessionWork(session(None)), now)
    stored.put(MongoFields.Session, MongoHiringCodecs.searchSession(session(Some("private candidate query"))))
    assertEquals(
      MongoSearchSessionWorkCodecs.readWork(stored).toEither,
      Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InconsistentDocument))
    )
  }

  test("AC-06.3 a claimed work document reports the problems of session, attempts and lease together") {
    val stored = MongoSearchSessionWorkCodecs.work(PendingSearchSessionWork(session(None)), now)
    stored.put(MongoFields.Session, new org.bson.Document())
    stored.put(MongoFields.Attempts, Long.box(0L))

    val errors = MongoSearchSessionWorkCodecs.readClaim(stored).toEither.swap.toOption.get
    assert(errors.size >= 3, clue(errors))
    assertEquals(MongoSearchSessionWorkCodecs.readClaim(stored).toStored, Left(RepositoryError.InvalidStoredData))
  }
}
