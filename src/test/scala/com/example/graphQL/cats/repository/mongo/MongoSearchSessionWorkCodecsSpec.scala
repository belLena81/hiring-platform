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
import com.example.graphQL.cats.infrastructure.events.OperationalEventWireCodec
import munit.FunSuite

import java.time.Instant
import java.util.UUID

class MongoSearchSessionWorkCodecsSpec extends FunSuite {
  private val actorId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000801"))
  private val searchId = UUID.fromString("00000000-0000-0000-0000-000000000802")
  private val now = Instant.parse("2026-09-22T08:00:00Z")

  test("work documents retain a query-free session and event payload") {
    val session = SearchSession(
      searchId,
      actorId,
      SearchKind.SemanticJobSearch,
      Some("private candidate query"),
      SearchFilterSnapshot.JobCriteria(Some("Nicosia"), Nil, None),
      Some("test-model"),
      List(SearchSessionResult("00000000-0000-0000-0000-000000000804", 1, 0.9d)),
      now,
      now.plusSeconds(3600)
    )
    val stored = MongoSearchSessionWorkCodecs.work(
      PendingSearchSessionWork(
        session,
        OperationalEvents
          .searchPerformed(UUID.fromString("00000000-0000-0000-0000-000000000803"), session)
          .fold(error => fail(error.toString), identity)
      ),
      now
    )

    assert(!stored.toJson.contains("private candidate query"))
    assertEquals(MongoSearchSessionWorkCodecs.readWork(stored).toEither.map(_.session.query), Right(None))
    assertEquals(
      MongoSearchSessionWorkCodecs
        .readWork(stored)
        .toEither
        .map(event => OperationalEventWireCodec.payloadJson(event.event.payload).hcursor.get[Option[String]]("query")),
      Right(Right(None))
    )
  }

  test("work attempt counts require BSON int32 values") {
    val session = SearchSession(
      searchId,
      actorId,
      SearchKind.SemanticJobSearch,
      None,
      SearchFilterSnapshot.Unfiltered,
      None,
      Nil,
      now,
      now.plusSeconds(3600)
    )
    val stored = MongoSearchSessionWorkCodecs.work(
      PendingSearchSessionWork(
        session,
        OperationalEvents
          .searchPerformed(UUID.fromString("00000000-0000-0000-0000-000000000803"), session)
          .fold(error => fail(error.toString), identity)
      ),
      now
    )
    stored.put(MongoFields.Attempts, Long.box(0L))
    stored.put(MongoFields.LeaseToken, "lease-token")

    assertEquals(
      MongoSearchSessionWorkCodecs.readClaim(stored).toEither,
      Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InvalidField(MongoFields.Attempts)))
    )
  }

  test("a stored event for another actor returns a typed inconsistency") {
    val session =
      SearchSession(
        searchId,
        actorId,
        SearchKind.Jobs,
        None,
        SearchFilterSnapshot.Unfiltered,
        None,
        Nil,
        now,
        now.plusSeconds(3600)
      )
    val event =
      OperationalEvents.searchPerformed(UUID.randomUUID(), session).fold(error => fail(error.toString), identity)
    val invalid = PendingSearchSessionWork(session, event.copy(actorId = UserId(UUID.randomUUID())))
    assert(!invalid.addressesSession)
    val stored = MongoSearchSessionWorkCodecs.work(invalid, now)
    assertEquals(
      MongoSearchSessionWorkCodecs.readWork(stored).toEither,
      Left(NonEmptyList.one(MongoHiringCodecs.StoredDocumentError.InconsistentDocument))
    )
  }

  test("AC-06.3 a work document with a corrupt session and a corrupt event reports the problems of both") {
    val session = SearchSession(
      searchId,
      actorId,
      SearchKind.Jobs,
      None,
      SearchFilterSnapshot.Unfiltered,
      None,
      Nil,
      now,
      now.plusSeconds(3600)
    )
    val event =
      OperationalEvents.searchPerformed(UUID.randomUUID(), session).fold(error => fail(error.toString), identity)
    def stored = MongoSearchSessionWorkCodecs.work(PendingSearchSessionWork(session, event), now)
    def errors(document: org.bson.Document) =
      MongoSearchSessionWorkCodecs.readWork(document).toEither.swap.toOption.get

    val badSession = stored
    badSession.put(MongoFields.Session, new org.bson.Document())
    val badEvent = stored
    badEvent.put(MongoFields.Event, new org.bson.Document())
    val both = stored
    both.put(MongoFields.Session, new org.bson.Document())
    both.put(MongoFields.Event, new org.bson.Document())

    assert(errors(badSession).size > 0 && errors(badEvent).size > 0)
    assertEquals(errors(both).size, errors(badSession).size + errors(badEvent).size)
    assertEquals(
      MongoSearchSessionWorkCodecs.readWork(both).toStored,
      Left(RepositoryError.InvalidStoredData)
    )
  }
}
