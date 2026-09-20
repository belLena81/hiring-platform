package com.example.graphQL.cats.service.events

import cats.Monad
import cats.data.EitherT
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.shared.events.OperationalEvents
import com.example.graphQL.cats.repository.protocol.{JobRepository, SearchSessionRepository, UserRepository}
import com.example.graphQL.cats.service.UseCaseError.*
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.protocol.InteractionUseCases
import java.time.Instant
import java.util.UUID

final class OperationalTelemetryService[F[_]: Monad](
    users: UserRepository[F],
    jobs: JobRepository[F],
    searchSessions: SearchSessionRepository[F]
) extends InteractionUseCases[F] {
  private val authorization = ActorAuthorization(users)

  override def recordJobView(
      actor: ActorContext,
      eventId: UUID,
      jobId: JobId,
      searchId: Option[UUID],
      now: Instant
  ): F[Either[UseCaseError, Unit]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      job <- EitherT(jobs.find(jobId).map(_.widenUseCase)).subflatMap(_.toRight(UseCaseError.domain(DomainError.NotFound("job"))))
      _ <- EitherT.cond[F](authorization.canView(user, job), (), UseCaseError.domain(DomainError.Forbidden))
      rank <- EitherT(searchId match {
        case Some(id) => verifiedSearchResult(actor, id, jobId.value.toString).map(_.map(Some(_)))
        case None => Monad[F].pure(Right(None))
      })
      event = OperationalEvents.jobViewed(eventId, jobId, actor.userId, searchId, rank, now)
      _ <- EitherT(searchSessions.recordInteraction(event).map(_.widenUseCase.void))
    } yield ()).value

  override def recordSearchResultClick(
      actor: ActorContext,
      eventId: UUID,
      searchId: UUID,
      resultId: String,
      now: Instant
  ): F[Either[UseCaseError, Unit]] =
    (for {
      rank <- EitherT(verifiedSearchResult(actor, searchId, resultId))
      event = OperationalEvents.searchResultClicked(eventId, searchId, resultId, actor.userId, rank, now)
      _ <- EitherT(searchSessions.recordInteraction(event).map(_.widenUseCase.void))
    } yield ()).value

  private def verifiedSearchResult(
      actor: ActorContext,
      searchId: UUID,
      resultId: String
  ): F[Either[UseCaseError, Int]] =
    searchSessions.find(searchId).map(_.widenUseCase.flatMap {
      case None => UseCaseError.domain(DomainError.NotFound("search session")).asLeft[Int]
      case Some(session) if session.actorId != actor.userId => UseCaseError.domain(DomainError.Forbidden).asLeft[Int]
      case Some(session) =>
        session.results.find(_.resultId == resultId)
          .map(result => result.rank.asRight[UseCaseError])
          .getOrElse(UseCaseError.domain(DomainError.Forbidden).asLeft[Int])
    })
}

object OperationalTelemetryService {
  def apply[F[_]: Monad](
      users: UserRepository[F],
      jobs: JobRepository[F],
      searchSessions: SearchSessionRepository[F]
  ): OperationalTelemetryService[F] =
    new OperationalTelemetryService(users, jobs, searchSessions)
}
