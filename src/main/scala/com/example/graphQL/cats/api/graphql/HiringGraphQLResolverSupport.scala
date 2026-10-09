package com.example.graphQL.cats.api.graphql

import cats.data.{EitherT, NonEmptyList}
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.admission.AuthRateLimiter
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.given
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.service.{ActorContext, AvailabilityError, SearchError, UseCaseError}
import com.example.graphQL.cats.service.ProbeResult
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, UseCaseIO}
import com.example.graphQL.cats.service.events.OperationalEventPayload
import com.example.graphQL.cats.domain.pagination.*
import com.example.graphQL.cats.service.search.{JobSearchFilter, SearchSessionEntry}
import io.circe.{Json, Printer}
import io.circe.syntax.*
import sangria.schema.Context

import java.time.Instant
import java.util.{Locale, UUID}

private[graphql] object HiringGraphQLResolverSupport {
  private val CanonicalJsonPrinter = Printer.noSpaces.copy(sortKeys = true)

  def raiseOnUseCaseError[A](value: UseCaseIO[A]): HiringGraphQLResult[A] =
    EitherT(value.value.map(_.leftMap(HiringGraphQLFailure.UseCase(_))))

  def inputResult[A](value: Either[GraphQLFailure, A]): HiringGraphQLResult[A] =
    EitherT.fromEither[IO](value.leftMap(HiringGraphQLFailure.Input(_)))

  def mutationResult[A](value: UseCaseIO[A]): HiringGraphQLResult[MutationOutcome[A]] =
    EitherT(value.value.flatMap {
      case Right(result)                               => IO.pure(Right(result))
      case Left(UseCaseError.ValidationFailed(errors)) => IO.pure(Right(validationError(errors)))
      case Left(error)                                 =>
        val failure = toGraphQLFailure(error)
        if (failure.exceptional) IO.pure(Left(HiringGraphQLFailure.UseCase(error)))
        else IO.pure(Right(DomainError(failure.code, failure.message)))
    })

  def idempotencyRequest(idempotencyKey: UUID, payload: Json): IdempotencyRequest =
    IdempotencyRequest.fromCanonicalInput(idempotencyKey, payload.printWith(CanonicalJsonPrinter))

  def applicationStatusFingerprintInput(
      applicationId: ApplicationId,
      status: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String]
  ): Json =
    Json.obj(
      "applicationId" -> applicationId.asJson,
      "status" -> Json.fromString(status.toString.toUpperCase(Locale.ROOT)),
      "feedback" -> feedback.asJson,
      "reason" -> reason.asJson
    )

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

  /** Resolves the client-supplied search id or lets the service generate one. */
  def searchIdFor(hiring: HiringGraphQLServices, supplied: Option[UUID]): HiringGraphQLResult[UUID] =
    EitherT.liftF[IO, HiringGraphQLFailure, UUID](hiring.searchSessions.searchId(supplied))

  def recordSearch[A](
      hiring: HiringGraphQLServices,
      actorId: UserId,
      kind: OperationalEventPayload.SearchKind,
      searchId: UUID,
      filter: Json,
      model: Option[String] = None
  )(results: List[A])(idOf: A => String, scoreOf: A => Double): HiringGraphQLResult[Unit] =
    EitherT.liftF[IO, HiringGraphQLFailure, Unit](
      hiring.searchSessions.record(
        actorId,
        kind,
        searchId,
        filter,
        model,
        results.map(result => SearchSessionEntry(idOf(result), scoreOf(result)))
      )
    )

  def authenticated[A](context: Context[RequestContext, Unit])(
      action: (ActorContext, HiringGraphQLServices) => HiringGraphQLResult[A]
  ): HiringGraphQLResult[A] = authenticated(context).flatMap(action.tupled)

  /** Runs `action` only while the hiring services are available. */
  def withHiring[A](context: Context[RequestContext, Unit])(
      action: HiringGraphQLServices => HiringGraphQLResult[A]
  ): HiringGraphQLResult[A] =
    EitherT.liftF[IO, HiringGraphQLFailure, ProbeResult](context.ctx.hiringAvailable).flatMap {
      case ProbeResult.Ready => action(context.ctx.hiring)
      case _                 => EitherT.leftT(availabilityFailure)
    }

  def rateLimited(
      context: Context[RequestContext, Unit],
      operation: AuthRateLimiter.Operation
  ): HiringGraphQLResult[Unit] =
    context.ctx.rateLimited(operation)

  def authenticatedSearch(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[
    (ActorContext, HiringGraphQLServices, com.example.graphQL.cats.service.protocol.SearchUseCases)
  ] =
    authenticated(context).flatMap { case (actor, hiring) =>
      hiring.semanticSearchService match {
        case Some(service) => EitherT.pure((actor, hiring, service))
        case None          =>
          EitherT.leftT(HiringGraphQLFailure.UseCase(UseCaseError.Search(SearchError.VectorSearchUnavailable)))
      }
    }

  /** One bounded cursor page: decodes `after`, fetches with the request built from it, and signs the cursors. */
  def paged[C, R, A](hiring: HiringGraphQLServices, first: Int, after: Option[String])(
      build: (Option[C], PageSize) => R
  )(fetch: R => HiringGraphQLResult[List[A]])(cursorOf: A => C)(using
      CursorCodec.Keyed[C]
  ): HiringGraphQLResult[Connection[A]] = {
    given CursorCodec.CursorKey = hiring.cursorKey
    for {
      now <- EitherT.liftF[IO, HiringGraphQLFailure, Instant](IO.realTimeInstant)
      (request, size) <- inputResult(cursorPage(first, after, CursorCodec.decode[C](_, now))(build))
      values <- fetch(request)
    } yield connection(values, size)(value => CursorCodec.encode(cursorOf(value), now))
  }

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

  private def authenticated(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[(ActorContext, HiringGraphQLServices)] =
    withHiring(context)(hiring => context.ctx.authenticatedActor.map(_ -> hiring))

  private def availabilityFailure: HiringGraphQLFailure =
    HiringGraphQLFailure.UseCase(UseCaseError.Availability(AvailabilityError.ServiceNotReady))

  private def cursorPage[A, B](
      first: Int,
      after: Option[String],
      decode: String => Either[CursorCodec.CursorError, A]
  )(build: (Option[A], PageSize) => B): Either[GraphQLFailure, (B, Int)] =
    pageSize(first).flatMap { size =>
      after
        .traverse(decode)
        .leftMap(GraphQLFailureCatalog.classifyCursor)
        .map(cursor => build(cursor, PageSize.next(size)) -> size.value)
    }

  def pageSize(first: Int): Either[GraphQLFailure, PageSize] =
    PageSize.fromInt(first).toEither.leftMap(errors => toGraphQLFailure(UseCaseError.ValidationFailed(errors)))

}
