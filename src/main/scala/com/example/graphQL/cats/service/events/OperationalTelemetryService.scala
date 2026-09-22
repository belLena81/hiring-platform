package com.example.graphQL.cats.service.events

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.shared.events.OperationalEvents
import com.example.graphQL.cats.repository.protocol.{JobRepository, MutationWriteContext, SearchSessionLookup, SearchSessionRepository, SearchSessionWorkRepository, UserRepository}
import com.example.graphQL.cats.service.UseCaseError.*
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.protocol.InteractionUseCases
import java.time.Instant
import java.util.UUID

final class OperationalTelemetryService(
    users: UserRepository,
    jobs: JobRepository,
    searchSessions: SearchSessionRepository,
    searchSessionWork: SearchSessionWorkRepository
) extends InteractionUseCases {
  private val authorization = ActorAuthorization(users)

  override def recordJobView(
      actor: ActorContext,
      eventId: UUID,
      jobId: JobId,
      searchId: Option[UUID],
      now: Instant
  ): IO[Either[UseCaseError, Unit]] =
    recordJobView(actor, eventId, jobId, searchId, now, MutationWriteContext.noop)

  override def recordJobView(
      actor: ActorContext,
      eventId: UUID,
      jobId: JobId,
      searchId: Option[UUID],
      now: Instant,
      context: MutationWriteContext
  ): IO[Either[UseCaseError, Unit]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      job <- EitherT(jobs.find(jobId).map(_.widenUseCase)).subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
      _ <- EitherT.cond[IO](authorization.canView(user, job), (), UseCaseError.Domain(DomainError.Forbidden))
      rank <- EitherT(searchId match {
        case Some(id) => verifiedSearchResult(actor, id, jobId.value.toString).map(_.map(Some(_)))
        case None => IO.pure(Right(None))
      })
      event = OperationalEvents.jobViewed(eventId, jobId, actor.userId, searchId, rank, now)
      _ <- EitherT(searchSessions.recordInteraction(event, context).map(_.widenUseCase.void))
    } yield ()).value

  override def recordSearchResultClick(
      actor: ActorContext,
      eventId: UUID,
      searchId: UUID,
      resultId: String,
      now: Instant
  ): IO[Either[UseCaseError, Unit]] =
    recordSearchResultClick(actor, eventId, searchId, resultId, now, MutationWriteContext.noop)

  override def recordSearchResultClick(
      actor: ActorContext,
      eventId: UUID,
      searchId: UUID,
      resultId: String,
      now: Instant,
      context: MutationWriteContext
  ): IO[Either[UseCaseError, Unit]] =
    (for {
      rank <- EitherT(verifiedSearchResult(actor, searchId, resultId))
      event = OperationalEvents.searchResultClicked(eventId, searchId, resultId, actor.userId, rank, now)
      _ <- EitherT(searchSessions.recordInteraction(event, context).map(_.widenUseCase.void))
    } yield ()).value

  private def verifiedSearchResult(
      actor: ActorContext,
      searchId: UUID,
      resultId: String
  ): IO[Either[UseCaseError, Int]] =
    searchSessions.find(searchId).flatMap(_.widenUseCase.fold(
      error => IO.pure(error.asLeft[Int]),
      session => session match {
      case None => UseCaseError.Domain(DomainError.NotFound("search session")).asLeft[Int]
      case Some(session) if session.actorId != actor.userId => UseCaseError.Domain(DomainError.Forbidden).asLeft[Int]
      case Some(session) =>
        session.results.find(_.resultId == resultId)
          .map(result => result.rank.asRight[UseCaseError])
          .getOrElse(UseCaseError.Domain(DomainError.Forbidden).asLeft[Int])
      } match {
        case value @ Right(_) => IO.pure(value)
        case Left(UseCaseError.Domain(DomainError.NotFound("search session"))) =>
          searchSessionWork.findForActor(actor.userId, searchId).map(_.widenUseCase.flatMap {
            case Some(SearchSessionLookup.Pending) => UseCaseError.Domain(DomainError.SearchSessionPending).asLeft[Int]
            case Some(SearchSessionLookup.Failed) => UseCaseError.Domain(DomainError.SearchSessionUnavailable).asLeft[Int]
            case _ => UseCaseError.Domain(DomainError.NotFound("search session")).asLeft[Int]
          })
        case value => IO.pure(value)
      }
    ))
}

object OperationalTelemetryService {
  def apply(
      users: UserRepository,
      jobs: JobRepository,
      searchSessions: SearchSessionRepository
  ): OperationalTelemetryService =
    new OperationalTelemetryService(users, jobs, searchSessions, SearchSessionWorkRepository.noop)

  def apply(
      users: UserRepository,
      jobs: JobRepository,
      searchSessions: SearchSessionRepository,
      searchSessionWork: SearchSessionWorkRepository
  ): OperationalTelemetryService =
    new OperationalTelemetryService(users, jobs, searchSessions, searchSessionWork)
}
