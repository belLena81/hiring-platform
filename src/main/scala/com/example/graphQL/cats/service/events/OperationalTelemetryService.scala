package com.example.graphQL.cats.service.events

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.shared.events.OperationalEvents
import com.example.graphQL.cats.repository.protocol.{
  JobRepository,
  SearchSessionLookup,
  SearchSessionRepository,
  SearchSessionWorkRepository,
  UserRepository
}
import com.example.graphQL.cats.service.UseCaseError.*
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.{
  IdempotencyRequest,
  InteractionUseCases,
  UseCaseIO,
  UseCaseIO as UseCase
}
import java.time.Instant
import java.util.UUID

final class OperationalTelemetryService(
    users: UserRepository,
    jobs: JobRepository,
    searchSessions: SearchSessionRepository,
    searchSessionWork: SearchSessionWorkRepository,
    idempotent: Idempotent = Idempotent.noop,
    currentTime: IO[Instant] = IO.realTimeInstant
) extends InteractionUseCases {
  private val authorization = ActorAuthorization(users)

  override def recordJobView(
      request: IdempotencyRequest,
      actor: ActorContext,
      eventId: UUID,
      jobId: JobId,
      searchId: Option[UUID]
  ): UseCaseIO[Unit] =
    idempotent.execute[Unit](
      "recordJobView",
      Idempotent.actorScope(actor),
      request,
      _ => interactionReference(eventId),
      replayInteraction(actor, eventId)
    ) { context =>
      for {
        user <- authorization.resolve(actor)
        job <- UseCase
          .repository(jobs.find(jobId))
          .subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
        _ <- UseCase.fromEither(
          Either.cond(authorization.canView(user, job), (), UseCaseError.Domain(DomainError.Forbidden))
        )
        rank <- searchId.fold(UseCase.pure(Option.empty[Int]))(id =>
          verifiedSearchResult(actor, id, jobId.value.toString).map(Some(_))
        )
        now <- UseCase.liftIO(currentTime)
        event = OperationalEvents.jobViewed(eventId, jobId, actor.userId, searchId, rank, now)
        _ <- UseCase.repository(searchSessions.recordInteraction(event, context))
      } yield ()
    }

  override def recordSearchResultClick(
      request: IdempotencyRequest,
      actor: ActorContext,
      eventId: UUID,
      searchId: UUID,
      resultId: String
  ): UseCaseIO[Unit] =
    idempotent.execute[Unit](
      "recordSearchResultClick",
      Idempotent.actorScope(actor),
      request,
      _ => interactionReference(eventId),
      replayInteraction(actor, eventId)
    ) { context =>
      for {
        rank <- verifiedSearchResult(actor, searchId, resultId)
        now <- UseCase.liftIO(currentTime)
        event = OperationalEvents.searchResultClicked(eventId, searchId, resultId, actor.userId, rank, now)
        _ <- UseCase.repository(searchSessions.recordInteraction(event, context))
      } yield ()
    }

  private def verifiedSearchResult(
      actor: ActorContext,
      searchId: UUID,
      resultId: String
  ): UseCaseIO[Int] =
    UseCase.fromIO(
      searchSessions
        .find(searchId)
        .flatMap(
          _.widenUseCase.fold(
            error => IO.pure(error.asLeft[Int]),
            session =>
              session match {
                case None => UseCaseError.Domain(DomainError.NotFound("search session")).asLeft[Int]
                case Some(session) if session.actorId != actor.userId =>
                  UseCaseError.Domain(DomainError.Forbidden).asLeft[Int]
                case Some(session) =>
                  session.results
                    .find(_.resultId == resultId)
                    .map(result => result.rank.asRight[UseCaseError])
                    .getOrElse(UseCaseError.Domain(DomainError.Forbidden).asLeft[Int])
              } match {
                case value @ Right(_)                                                  => IO.pure(value)
                case Left(UseCaseError.Domain(DomainError.NotFound("search session"))) =>
                  searchSessionWork
                    .findForActor(actor.userId, searchId)
                    .map(_.widenUseCase.flatMap {
                      case Some(SearchSessionLookup.Pending) =>
                        UseCaseError.Domain(DomainError.SearchSessionPending).asLeft[Int]
                      case Some(SearchSessionLookup.Failed) =>
                        UseCaseError.Domain(DomainError.SearchSessionUnavailable).asLeft[Int]
                      case _ => UseCaseError.Domain(DomainError.NotFound("search session")).asLeft[Int]
                    })
                case value => IO.pure(value)
              }
          )
        )
    )

  private def interactionReference(
      eventId: UUID
  ): com.example.graphQL.cats.repository.protocol.MutationEntityReference =
    com.example.graphQL.cats.repository.protocol.MutationEntityReference("interaction", eventId.toString)

  private def replayInteraction(actor: ActorContext, eventId: UUID)(
      reference: com.example.graphQL.cats.repository.protocol.MutationEntityReference
  ): UseCaseIO[Unit] =
    if (reference == interactionReference(eventId)) authorization.resolve(actor).void
    else UseCase.left(UseCaseError.Repository(com.example.graphQL.cats.repository.protocol.RepositoryError.Unavailable))
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

  def live(
      users: UserRepository,
      jobs: JobRepository,
      searchSessions: SearchSessionRepository,
      searchSessionWork: SearchSessionWorkRepository,
      idempotent: Idempotent
  ): OperationalTelemetryService =
    new OperationalTelemetryService(users, jobs, searchSessions, searchSessionWork, idempotent)
}
