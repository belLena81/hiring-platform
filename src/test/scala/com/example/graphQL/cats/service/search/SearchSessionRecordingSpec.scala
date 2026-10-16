package com.example.graphQL.cats.service.search

import cats.effect.{IO, Ref}
import cats.effect.std.UUIDGen
import com.example.graphQL.cats.FixedTestClock
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.events.OperationalEventPayload.SearchKind
import com.example.graphQL.cats.service.events.{
  OperationalEventEnvelope,
  OperationalEventPayload,
  SearchFilterSnapshot,
  SearchSession,
  SearchSessionHandoff
}
import com.example.graphQL.cats.service.port.{RepositoryF, SearchRecordingFailureRepository}
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
      new SearchSessionHandoff[IO] {
        def enqueue(session: SearchSession, event: OperationalEventEnvelope): IO[Unit] =
          sink.update(_ :+ (session -> event))
      },
      new SearchRecordingFailureRepository[IO] {
        def record(event: OperationalEventEnvelope) = RepositoryF.at[IO].fromEither(Right(true))
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
      _ <- service
        .record(
          actorId,
          SearchKind.Jobs,
          supplied,
          SearchFilterSnapshot.JobCriteria(None, Nil, None),
          None,
          List(
            SearchSessionEntry("00000000-0000-0000-0000-000000000c01", 0.5d),
            SearchSessionEntry("00000000-0000-0000-0000-000000000c02", 0.25d)
          )
        )
        .value
      recorded <- sink.get
    } yield {
      val (session, event) = recorded.head
      assertEquals(session.id, supplied)
      assertEquals(session.actorId, actorId)
      assertEquals(session.searchKind, SearchKind.Jobs)
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

  test("AIR-04.3 a recording failure is handed off as one deterministic event with the kind and failure code only") {
    for {
      stored <- Ref.of[IO, Vector[OperationalEventEnvelope]](Vector.empty)
      service = SearchSessionRecording(
        new SearchSessionHandoff[IO] { def enqueue(session: SearchSession, event: OperationalEventEnvelope) = IO.unit },
        new SearchRecordingFailureRepository[IO] {
          def record(event: OperationalEventEnvelope) =
            RepositoryF.lift(stored.update(_ :+ event).as(true))
        },
        FixedTestClock.at(now),
        generator
      )
      _ <- service.recordFailure(actorId, SearchKind.Jobs, supplied, "UNAVAILABLE")
      events <- stored.get
    } yield {
      val event = events.head
      assertEquals(event.eventId, SearchSessionRecording.failureEventId(supplied))
      assertEquals(event.occurredAt, now)
      assertEquals(event.actorId, actorId)
      assertEquals(
        event.payload,
        OperationalEventPayload.SearchRecordingFailed(supplied, SearchKind.Jobs, "UNAVAILABLE")
      )
    }
  }

  test("an empty result list with a model records an empty ranked session") {
    for {
      sink <- Ref.of[IO, Vector[(SearchSession, OperationalEventEnvelope)]](Vector.empty)
      _ <- recording(sink)
        .record(
          actorId,
          SearchKind.SemanticJobSearch,
          supplied,
          SearchFilterSnapshot.Unfiltered,
          Some("voyage-4-lite"),
          Nil
        )
        .value
      recorded <- sink.get
    } yield {
      assertEquals(recorded.size, 1)
      assertEquals(recorded.head._1.results, Nil)
      assertEquals(recorded.head._1.model, Some("voyage-4-lite"))
      assertEquals(recorded.head._1.searchKind, SearchKind.SemanticJobSearch)
    }
  }
}
