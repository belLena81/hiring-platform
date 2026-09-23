package com.example.graphQL.cats.api.graphql

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.admission.AuthRateLimiter
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.{ActorContext, AvailabilityError, ProbeResult, SearchError, UseCaseError}
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, UseCaseIO}
import com.example.graphQL.cats.shared.events.{OperationalEvents, SearchSession, SearchSessionResult}
import com.example.graphQL.cats.shared.pagination.*
import com.example.graphQL.cats.shared.search.JobSearchFilter
import io.circe.Json
import sangria.schema.Context

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.concurrent.duration.*

private[graphql] object HiringGraphQLResolverSupport {
  def raiseOnUseCaseError[A](value: UseCaseIO[A]): IO[A] =
    value.value.map(_.leftMap(RequestContext.ReadFailure(_))).rethrow

  def inputResult[A](value: Either[GraphQLFailure, A]): IO[A] =
    IO.fromEither(value.leftMap(error => RequestContext.FieldFailure(error.code, error.message)))

  def mutationResult[A](value: UseCaseIO[A]): IO[MutationOutcome[A]] =
    value.value.flatMap {
      case Right(result)                               => IO.pure(result)
      case Left(UseCaseError.ValidationFailed(errors)) => IO.pure(validationError(errors))
      case Left(error)                                 =>
        val failure = toGraphQLFailure(error)
        if (failure.exceptional) liftUseCase(error)
        else IO.pure(DomainError(failure.code, failure.message))
    }

  def idempotencyRequest(idempotencyKey: UUID, canonicalInput: Json): IdempotencyRequest =
    IdempotencyRequest.fromCanonicalInput(idempotencyKey, canonicalInput.noSpaces)

  def searchEventId(searchId: UUID): UUID =
    UUID.nameUUIDFromBytes(s"search-performed:$searchId".getBytes(StandardCharsets.UTF_8))

  def jobFilter(value: Option[JobFilterGraphQLInput]): JobSearchFilter =
    value match {
      case None         => JobSearchFilter(None, Set.empty, None)
      case Some(filter) =>
        JobSearchFilter(filter.city, filter.skills.fold(Set.empty[String])(_.toSet), filter.createdAfter)
    }

  def filterJson(value: JobSearchFilter): Json =
    Json.obj(
      "city" -> value.city.fold(Json.Null)(Json.fromString),
      "skills" -> Json.arr(value.skills.toList.sorted.map(Json.fromString)*),
      "createdAfter" -> value.createdAfter.fold(Json.Null)(instant => Json.fromString(instant.toString))
    )

  def saveSearchSession[A](
      hiring: HiringGraphQLServices,
      actorId: UserId,
      kind: String,
      searchId: UUID,
      filter: Json,
      model: Option[String] = None
  )(results: List[A])(idOf: A => String, scoreOf: A => Double): IO[Unit] =
    IO.realTimeInstant.flatMap { now =>
      val session = SearchSession(
        searchId,
        actorId,
        kind,
        None,
        filter,
        model,
        results.zipWithIndex.map { case (result, index) =>
          SearchSessionResult(idOf(result), index + 1, scoreOf(result))
        },
        now,
        now.plusSeconds(7.days.toSeconds)
      )
      hiring.searchSessionHandoff.enqueue(session, OperationalEvents.searchPerformed(searchEventId(searchId), session))
    }

  def authenticated[A](context: Context[RequestContext, Unit])(
      action: (ActorContext, HiringGraphQLServices) => IO[A]
  ): IO[A] = authenticated(context).flatMap(action.tupled)

  def publicMutation[A](context: Context[RequestContext, Unit])(
      action: HiringGraphQLServices => IO[A]
  ): IO[A] =
    context.ctx.hiringAvailable.flatMap {
      case ProbeResult.Ready => action(context.ctx.hiring)
      case _ => IO.raiseError(RequestContext.ReadFailure(UseCaseError.Availability(AvailabilityError.ServiceNotReady)))
    }

  def rateLimited(context: Context[RequestContext, Unit], operation: AuthRateLimiter.Operation): IO[Unit] =
    context.ctx.rateLimited(operation)

  def authenticatedSearch(
      context: Context[RequestContext, Unit]
  ): IO[(ActorContext, HiringGraphQLServices, com.example.graphQL.cats.service.protocol.SearchUseCases)] =
    authenticated(context).flatMap { case (actor, hiring) =>
      hiring.semanticSearchService match {
        case Some(service) => IO.pure((actor, hiring, service))
        case None => IO.raiseError(RequestContext.ReadFailure(UseCaseError.Search(SearchError.VectorSearchUnavailable)))
      }
    }

  def page(
      first: Int,
      after: Option[String],
      decode: String => Either[CursorCodec.CursorError, JobCursor]
  ): Either[GraphQLFailure, (JobPageRequest, Int)] =
    cursorPage(first, after, decode)((cursor, size) => JobPageRequest(None, cursor, size))

  def pageEvent(
      first: Int,
      after: Option[String],
      decode: String => Either[CursorCodec.CursorError, ApplicationEventCursor]
  ): Either[GraphQLFailure, (ApplicationEventPageRequest, Int)] =
    cursorPage(first, after, decode)((cursor, size) => ApplicationEventPageRequest(cursor, size))

  def applicationPage(
      first: Int,
      after: Option[String],
      status: Option[ApplicationStatus],
      decode: String => Either[CursorCodec.CursorError, ApplicationCursor]
  ): Either[GraphQLFailure, (ApplicationPageRequest, Int)] =
    cursorPage(first, after, decode)((cursor, size) => ApplicationPageRequest(status, cursor, size))

  def userPage(
      first: Int,
      after: Option[String],
      status: AccountStatus,
      role: Option[UserRole],
      decode: String => Either[CursorCodec.CursorError, UserCursor]
  ): Either[GraphQLFailure, (UserPageRequest, Int)] =
    cursorPage(first, after, decode)((cursor, size) => UserPageRequest(status, role, cursor, size))

  def connection[A](values: List[A], requested: Int)(cursor: A => String): Connection[A] = {
    val nodes = values.take(requested)
    val edges = nodes.map(value => Edge(value, cursor(value)))
    Connection(edges, PageInfo(values.size > requested, edges.lastOption.map(_.cursor)))
  }

  def toGraphQLFailure(error: UseCaseError): GraphQLFailure =
    GraphQLFailureCatalog.classify(error)

  private def validationError(errors: NonEmptyList[DomainValidationError]): ValidationError = {
    val failure = toGraphQLFailure(UseCaseError.ValidationFailed(errors))
    ValidationError(failure.code, failure.message)
  }

  private def liftUseCase[A](error: UseCaseError): IO[A] =
    IO.raiseError(RequestContext.ReadFailure(error))

  private def authenticated(context: Context[RequestContext, Unit]): IO[(ActorContext, HiringGraphQLServices)] =
    context.ctx.hiringAvailable.flatMap {
      case ProbeResult.Ready => context.ctx.authenticatedActor.map(_ -> context.ctx.hiring)
      case _ => IO.raiseError(RequestContext.ReadFailure(UseCaseError.Availability(AvailabilityError.ServiceNotReady)))
    }

  private def cursorPage[A, B](
      first: Int,
      after: Option[String],
      decode: String => Either[CursorCodec.CursorError, A]
  )(build: (Option[A], PageSize) => B): Either[GraphQLFailure, (B, Int)] =
    pageSize(first).flatMap { size =>
      after
        .traverse(decode)
        .leftMap {
          case CursorCodec.CursorError.WrongKind(_) =>
            GraphQLFailure("WRONG_CURSOR_KIND", "Cursor belongs to a different connection", exceptional = false)
          case CursorCodec.CursorError.Malformed(_) =>
            GraphQLFailure("INVALID_CURSOR", "Invalid cursor", exceptional = false)
        }
        .map(cursor => build(cursor, PageSize.next(size)) -> size.value)
    }

  def pageSize(first: Int): Either[GraphQLFailure, PageSize] =
    PageSize.fromInt(first).toEither.leftMap(errors => toGraphQLFailure(UseCaseError.ValidationFailed(errors)))

}
