package com.example.graphQL.cats.service.search

import cats.effect.IO
import cats.effect.kernel.Clock
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.events.{
  OperationalEventPayload,
  OperationalEvents,
  SearchSession,
  SearchSessionHandoff,
  SearchSessionResult
}
import io.circe.Json

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.concurrent.duration.*

/** One ranked search result as shown to the actor; rank is the one-based position in the list. */
final case class SearchSessionEntry(resultId: String, score: Double)

/** Service-owned search session capture: id generation, clock, retention, event identity and durable handoff. */
trait SearchSessionRecording {

  /** Reuses a client-supplied search id so retries stay correlated; otherwise generates one. */
  def searchId(clientSupplied: Option[UUID]): IO[UUID]

  def record(
      actorId: UserId,
      kind: OperationalEventPayload.SearchKind,
      searchId: UUID,
      filter: Json,
      model: Option[String],
      results: List[SearchSessionEntry]
  ): IO[Unit]
}

object SearchSessionRecording {
  private[search] val Retention: FiniteDuration = 7.days

  def eventId(searchId: UUID): UUID =
    UUID.nameUUIDFromBytes(s"search-performed:$searchId".getBytes(StandardCharsets.UTF_8))

  def apply(
      handoff: SearchSessionHandoff,
      clock: Clock[IO] = Clock[IO],
      uuids: UUIDGen[IO] = UUIDGen[IO]
  ): SearchSessionRecording = new SearchSessionRecording {
    def searchId(clientSupplied: Option[UUID]): IO[UUID] = clientSupplied.fold(uuids.randomUUID)(IO.pure)

    def record(
        actorId: UserId,
        kind: OperationalEventPayload.SearchKind,
        searchId: UUID,
        filter: Json,
        model: Option[String],
        results: List[SearchSessionEntry]
    ): IO[Unit] =
      clock.realTimeInstant.flatMap { now =>
        val session = SearchSession(
          searchId,
          actorId,
          kind.wire,
          None,
          filter,
          model,
          results.zipWithIndex.map { case (entry, index) =>
            SearchSessionResult(entry.resultId, index + 1, entry.score)
          },
          now,
          now.plusSeconds(Retention.toSeconds)
        )
        IO.fromEither(
          OperationalEvents
            .searchPerformed(eventId(searchId), session)
            .leftMap(_ => new IllegalStateException("Invalid search event contract"))
        ).flatMap(event => handoff.enqueue(session, event))
      }
  }
}
