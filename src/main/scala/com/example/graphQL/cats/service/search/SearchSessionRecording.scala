package com.example.graphQL.cats.service.search

import cats.data.EitherT
import cats.Monad
import cats.effect.kernel.Clock
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.events.{
  OperationalEventPayload,
  OperationalEvents,
  SearchFilterSnapshot,
  SearchSession,
  SearchSessionHandoff,
  SearchSessionResult
}
import com.example.graphQL.cats.service.protocol.UseCaseF
import com.example.graphQL.cats.service.{RepositoryError, UseCaseError}
import com.example.graphQL.cats.shared.StableIds
import java.util.UUID
import scala.concurrent.duration.*

/** One ranked search result as shown to the actor; rank is the one-based position in the list. */
final case class SearchSessionEntry(resultId: String, score: Double)

/** Service-owned search session capture: id generation, clock, retention, event identity and durable handoff. */
trait SearchSessionRecording[F[_]] {

  /** Reuses a client-supplied search id so retries stay correlated; otherwise generates one. */
  def searchId(clientSupplied: Option[UUID]): F[UUID]

  def record(
      actorId: UserId,
      kind: OperationalEventPayload.SearchKind,
      searchId: UUID,
      filter: SearchFilterSnapshot,
      model: Option[String],
      results: List[SearchSessionEntry]
  ): UseCaseF[F, Unit]
}

object SearchSessionRecording {
  private[search] val Retention: FiniteDuration = 7.days

  def eventId(searchId: UUID): UUID =
    StableIds.uuid(s"search-performed:$searchId")

  def apply[F[_]: Monad: Clock: UUIDGen](handoff: SearchSessionHandoff[F]): SearchSessionRecording[F] =
    apply(handoff, Clock[F], UUIDGen[F])

  def apply[F[_]: Monad](
      handoff: SearchSessionHandoff[F],
      clock: Clock[F],
      uuids: UUIDGen[F]
  ): SearchSessionRecording[F] = new SearchSessionRecording[F] {
    def searchId(clientSupplied: Option[UUID]): F[UUID] = clientSupplied.fold(uuids.randomUUID)(_.pure[F])

    def record(
        actorId: UserId,
        kind: OperationalEventPayload.SearchKind,
        searchId: UUID,
        filter: SearchFilterSnapshot,
        model: Option[String],
        results: List[SearchSessionEntry]
    ): UseCaseF[F, Unit] =
      EitherT.liftF[F, UseCaseError, java.time.Instant](clock.realTimeInstant).flatMap { now =>
        val session = SearchSession(
          searchId,
          actorId,
          kind,
          None,
          filter,
          model,
          results.zipWithIndex.map { case (entry, index) =>
            SearchSessionResult(entry.resultId, index + 1, entry.score)
          },
          now,
          now.plusSeconds(Retention.toSeconds)
        )
        EitherT
          .fromEither[F](
            OperationalEvents
              .searchPerformed(eventId(searchId), session)
              .leftMap(_ => UseCaseError.Repository(RepositoryError.InvalidEvent))
          )
          .flatMap(event => EitherT.liftF[F, UseCaseError, Unit](handoff.enqueue(session, event)))
      }
  }
}
