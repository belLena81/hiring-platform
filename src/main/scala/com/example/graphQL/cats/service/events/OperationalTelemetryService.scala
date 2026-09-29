package com.example.graphQL.cats.service.events

import cats.effect.{Clock, IO}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.service.events.OperationalEvents
import com.example.graphQL.cats.service.port.{
  JobRepository,
  SearchSessionLookup,
  SearchSessionRepository,
  SearchSessionWorkRepository,
  UserRepository
}
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.{
  IdempotencyRequest,
  InteractionUseCases,
  UseCaseIO,
  UseCaseIO as UseCase
}
import java.util.UUID

final class OperationalTelemetryService(
    users: UserRepository,
    jobs: JobRepository,
    searchSessions: SearchSessionRepository,
    searchSessionWork: SearchSessionWorkRepository,
    idempotent: Idempotent,
    clock: Clock[IO] = Clock[IO]
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
          verifiedSearchResult(actor, id, jobId.value.toString).map(value => Some(value._1))
        )
        now <- UseCase.liftIO(clock.realTimeInstant)
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
        verified <- verifiedSearchResult(actor, searchId, resultId)
        now <- UseCase.liftIO(clock.realTimeInstant)
        event = OperationalEvents.searchResultClicked(
          eventId,
          searchId,
          resultId,
          verified._2,
          actor.userId,
          verified._1,
          now
        )
        _ <- UseCase.repository(searchSessions.recordInteraction(event, context))
      } yield ()
    }

  private def verifiedSearchResult(
      actor: ActorContext,
      searchId: UUID,
      resultId: String
  ): UseCaseIO[(Int, String)] =
    UseCase.repository(searchSessions.find(searchId)).flatMap {
      case None =>
        UseCase.repository(searchSessionWork.findForActor(actor.userId, searchId)).subflatMap {
          case Some(SearchSessionLookup.Pending) =>
            Left(UseCaseError.Domain(DomainError.SearchSessionPending))
          case Some(SearchSessionLookup.Failed) =>
            Left(UseCaseError.Domain(DomainError.SearchSessionUnavailable))
          case _ => Left(UseCaseError.Domain(DomainError.NotFound("search session")))
        }
      case Some(session) if session.actorId != actor.userId =>
        UseCase.left(UseCaseError.Domain(DomainError.Forbidden))
      case Some(session) =>
        UseCase.fromEither(
          session.results
            .find(_.resultId == resultId)
            .map(result => (result.rank, session.searchKind))
            .toRight(UseCaseError.Domain(DomainError.Forbidden))
        )
    }

  private def interactionReference(
      eventId: UUID
  ): com.example.graphQL.cats.service.port.MutationEntityReference =
    com.example.graphQL.cats.service.port.MutationEntityReference("interaction", eventId.toString)

  private def replayInteraction(actor: ActorContext, eventId: UUID)(
      reference: com.example.graphQL.cats.service.port.MutationEntityReference
  ): UseCaseIO[Unit] =
    if (reference == interactionReference(eventId)) authorization.resolve(actor).void
    else UseCase.left(UseCaseError.Repository(com.example.graphQL.cats.service.RepositoryError.Unavailable))
}

object OperationalTelemetryService {
  def live(
      users: UserRepository,
      jobs: JobRepository,
      searchSessions: SearchSessionRepository,
      searchSessionWork: SearchSessionWorkRepository,
      idempotent: Idempotent
  ): OperationalTelemetryService =
    new OperationalTelemetryService(users, jobs, searchSessions, searchSessionWork, idempotent)
}
