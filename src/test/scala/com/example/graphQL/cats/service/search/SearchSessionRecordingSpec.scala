package com.example.graphQL.cats.service.search

import cats.effect.{IO, Ref}
import cats.effect.std.UUIDGen
import com.example.graphQL.cats.FixedTestClock
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.events.OperationalEventPayload.SearchKind
import com.example.graphQL.cats.service.events.{OperationalEventEnvelope, SearchSession, SearchSessionHandoff}
import io.circe.Json
import munit.CatsEffectSuite

import java.time.Instant
import java.util.UUID

final class SearchSessionRecordingSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-10-01T10:00:00Z")
  private val actorId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000a01"))
  private val generated = UUID.fromString("00000000-0000-0000-0000-000000000a02")
  private val supplied = UUID.fromString("00000000-0000-0000-0000-000000000a03")

  private val generator = new UUIDGen[IO] { def randomUUID: IO[UUID] = IO.pure(generated) }

  private def recording(sink: Ref[IO, Vector[(SearchSession, OperationalEventEnvelope)]]) =
    SearchSessionRecording(
      new SearchSessionHandoff {
        def enqueue(session: SearchSession, event: OperationalEventEnvelope): IO[Unit] =
          sink.update(_ :+ (session -> event))
      },
      FixedTestClock.at(now),
      generator
    )

  test("a client-supplied search id is reused and an absent one is generated") {
    for {
      sink <- Ref.of[IO, Vector[(SearchSession, OperationalEventEnvelope)]](Vector.empty)
      service = recording(sink)
      reused <- service.searchId(Some(supplied))
      fresh <- service.searchId(None)
    } yield {
      assertEquals(reused, supplied)
      assertEquals(fresh, generated)
    }
  }

  test("recording ranks results, applies seven-day retention and derives a deterministic event identity") {
    for {
      sink <- Ref.of[IO, Vector[(SearchSession, OperationalEventEnvelope)]](Vector.empty)
      service = recording(sink)
      _ <- service.record(
        actorId,
        SearchKind.Jobs,
        supplied,
        Json.obj("city" -> Json.Null),
        None,
        List(
          SearchSessionEntry("00000000-0000-0000-0000-000000000c01", 0.5d),
          SearchSessionEntry("00000000-0000-0000-0000-000000000c02", 0.25d)
        )
      )
      recorded <- sink.get
    } yield {
      val (session, event) = recorded.head
      assertEquals(session.id, supplied)
      assertEquals(session.actorId, actorId)
      assertEquals(session.searchKind, "jobs")
      assertEquals(session.occurredAt, now)
      assertEquals(session.expiresAt, now.plusSeconds(7L * 24L * 3600L))
      assertEquals(
        session.results.map(r => (r.resultId, r.rank, r.score)),
        List(("00000000-0000-0000-0000-000000000c01", 1, 0.5d), ("00000000-0000-0000-0000-000000000c02", 2, 0.25d))
      )
      assertEquals(event.eventId, SearchSessionRecording.eventId(supplied))
      assertEquals(
        SearchSessionRecording.eventId(supplied),
        UUID.nameUUIDFromBytes(s"search-performed:$supplied".getBytes("UTF-8"))
      )
    }
  }

  test("an empty result list with a model records an empty ranked session") {
    for {
      sink <- Ref.of[IO, Vector[(SearchSession, OperationalEventEnvelope)]](Vector.empty)
      _ <- recording(sink).record(
        actorId,
        SearchKind.SemanticJobSearch,
        supplied,
        Json.obj(),
        Some("voyage-4-lite"),
        Nil
      )
      recorded <- sink.get
    } yield {
      assertEquals(recorded.size, 1)
      assertEquals(recorded.head._1.results, Nil)
      assertEquals(recorded.head._1.model, Some("voyage-4-lite"))
      assertEquals(recorded.head._1.searchKind, "semanticJobSearch")
    }
  }
}
