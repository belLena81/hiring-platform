package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.service.events.OperationalEvents
import munit.CatsEffectSuite
import org.bson.Document

import java.time.Instant
import java.util.UUID

final class MongoSearchClickMigrationSpec extends CatsEffectSuite {
  test("retained search session lookup failure is reported without changing the migration result") {
    val actor = UserId(UUID.fromString("00000000-0000-0000-0000-000000000001"))
    val searchId = UUID.fromString("00000000-0000-0000-0000-000000000002")
    val resultId = UUID.fromString("00000000-0000-0000-0000-000000000003").toString
    val click = OperationalEvents
      .searchResultClicked(
        UUID.fromString("00000000-0000-0000-0000-000000000004"),
        searchId,
        resultId,
        "candidateMatches",
        actor,
        1,
        Instant.parse("2026-09-22T12:00:00Z")
      )
      .fold(error => fail(error.toString), identity)

    for {
      lookedUp <- Ref.of[IO, Option[UUID]](None)
      observed <- Ref.of[IO, List[(LogEvent, Map[LogField, String])]](Nil)
      diagnostics = new Diagnostics {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          observed.update(_ :+ (event -> fields))
      }
      result <- MongoHiringMigrations.verifyRetainedSearchSessionForClick(
        click,
        id =>
          lookedUp.set(Some(id)) *> IO.raiseError[Option[Document]](new IllegalStateException("private Mongo detail")),
        diagnostics
      )
      attemptedId <- lookedUp.get
      events <- observed.get
    } yield {
      assertEquals(attemptedId, Some(searchId))
      assertEquals(result, Left("an unavailable retained search session"))
      assertEquals(events.map(_._1), List(LogEvent.SearchSessionVerificationFailed))
      assertEquals(events.headOption.flatMap(_._2.get(LogField.ErrorType)), Some("java.lang.IllegalStateException"))
      assertEquals(events.headOption.map(_._2.keySet), Some(LogFields.failure(new IllegalStateException()).keySet))
      assert(!events.exists(_._2.values.exists(_.contains("private Mongo detail"))))
    }
  }
}
