package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.repository.protocol.PendingSearchSessionWork
import com.example.graphQL.cats.shared.events.{OperationalEvents, SearchSession, SearchSessionResult}
import io.circe.Json
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
      "semanticJobSearch",
      Some("private candidate query"),
      Json.obj("city" -> Json.fromString("Nicosia")),
      Some("test-model"),
      List(SearchSessionResult("job-1", 1, 0.9d)),
      now,
      now.plusSeconds(3600)
    )
    val stored = MongoSearchSessionWorkCodecs.work(
      PendingSearchSessionWork(session, OperationalEvents.searchPerformed(UUID.fromString("00000000-0000-0000-0000-000000000803"), session)),
      now
    )

    assert(!stored.toJson.contains("private candidate query"))
    assertEquals(MongoSearchSessionWorkCodecs.readWork(stored).map(_.session.query), Right(None))
    assertEquals(MongoSearchSessionWorkCodecs.readWork(stored).map(_.event.payload.hcursor.get[Option[String]]("query")), Right(Right(None)))
  }
}
