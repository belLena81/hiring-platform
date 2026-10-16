package com.example.graphQL.cats.api.graphql

import cats.data.{EitherT, NonEmptyList}
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.given
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.service.{ActorContext, AvailabilityError, SearchError, UseCaseError}
import com.example.graphQL.cats.service.ProbeResult
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, UseCaseF}
import com.example.graphQL.cats.service.events.{OperationalEventPayload, SearchFilterSnapshot}
import com.example.graphQL.cats.domain.pagination.*
import com.example.graphQL.cats.service.search.{JobDiscoveryValidation, JobSearchFilter, SearchSessionEntry}
import io.circe.{Json, Printer}
import io.circe.syntax.*
import sangria.schema.Context

import java.time.Instant
import java.util.{Locale, UUID}

private[graphql] object HiringGraphQLResolverSupport {
  private val CanonicalJsonPrinter = Printer.noSpaces.copy(sortKeys = true)

  def raiseOnUseCaseError[A](value: UseCaseF[IO, A]): HiringGraphQLResult[A] =
    EitherT(value.value.map(_.leftMap(HiringGraphQLFailure.UseCase(_))))

  def inputResult[A](value: Either[GraphQLFailure, A]): HiringGraphQLResult[A] =
    EitherT.fromEither[IO](value.leftMap(HiringGraphQLFailure.Input(_)))

  def mutationResult[A](
      value: UseCaseF[IO, A],
      fieldPath: InputFieldPaths.Path = InputFieldPaths.none
  ): HiringGraphQLResult[MutationOutcome[A]] =
    EitherT(value.value.flatMap {
      case Right(result)                               => IO.pure(Right(result))
      case Left(UseCaseError.ValidationFailed(errors)) => IO.pure(Right(validationError(errors, fieldPath)))
      case Left(error)                                 =>
        val failure = GraphQLFailureCatalog.classify(error)
        if (failure.exceptional || GraphQLFailureCatalog.isAuthenticationFailure(error))
          IO.pure(Left(HiringGraphQLFailure.UseCase(error)))
        else IO.pure(Right(DomainError(failure.code, failure.message)))
    })

  /** A rate-limit refusal as data: the code and message come from the failure, and the delay is carried (whole seconds,
    * clamped to `Int`) so the client knows when to retry.
    */
  def rateLimitedRefusal(failure: GraphQLFailure, retryAfterSeconds: Long): DomainError =
    DomainError(failure.code, failure.message, Some(math.min(retryAfterSeconds, Int.MaxValue.toLong).toInt))

  /** Mirrors `mutationResult` for the AI search roots: an unavailable search, an invalid input, a rate-limit refusal or
    * a non-exceptional business refusal becomes data, so sibling fields survive; authentication, authorization,
    * readiness and repository failures stay top-level and null only this field. Search roots decide data versus
    * top-level from the failure's search kind first, so the embedding and provider failures that are exceptional for
    * mutations are still data here. The kind comes from the failure catalog.
    */
  def searchResult[A](
      value: HiringGraphQLResult[A],
      fieldPath: InputFieldPaths.Path
  ): HiringGraphQLResult[SearchOutcome[A]] =
    EitherT(value.value.map {
      case Right(result)                                   => Right(result)
      case Left(HiringGraphQLFailure.RateLimited(seconds)) =>
        Right(rateLimitedRefusal(GraphQLFailureCatalog.requestRateLimited, seconds))
      case Left(failure) =>
        searchRefusal(failure).flatMap { found =>
          val refusal: Option[SearchUnavailable | ValidationError | DomainError] =
            dataRefusal(found, fieldPath).orElse(
              Option.when(!found.exceptional)(DomainError(found.code, found.message))
            )
          refusal.toRight(failure)
        }
    })

  /** The facets root has no business-rule member, so only unavailability and invalid input become data. */
  def facetsResult[A](
      value: HiringGraphQLResult[A],
      fieldPath: InputFieldPaths.Path
  ): HiringGraphQLResult[FacetsOutcome[A]] =
    EitherT(value.value.map {
      case Right(result) => Right(result)
      case Left(failure) => searchRefusal(failure).flatMap(dataRefusal(_, fieldPath).toRight(failure))
    })

  private def searchRefusal(failure: HiringGraphQLFailure): Either[HiringGraphQLFailure, GraphQLFailure] =
    failure match {
      case HiringGraphQLFailure.UseCase(error) if GraphQLFailureCatalog.isAuthenticationFailure(error) => Left(failure)
      case HiringGraphQLFailure.UseCase(error) => Right(GraphQLFailureCatalog.classify(error))
      case HiringGraphQLFailure.Input(error)   => Right(error)
      case other                               => Left(other)
    }

  private def dataRefusal(
      found: GraphQLFailure,
      fieldPath: InputFieldPaths.Path
  ): Option[SearchUnavailable | ValidationError] =
    found.search match {
      case SearchRefusal.Unavailable  => Some(SearchUnavailable(found.code, found.message, retryable = true))
      case SearchRefusal.InvalidInput =>
        Some(ValidationError(found.code, found.message, found.field.flatMap(fieldPath)))
      case SearchRefusal.Other => None
    }

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

  /** The filter after the shared discovery validation, so every job discovery operation rejects the same values. */
  def discoveryFilter(value: Option[JobFilterGraphQLInput]): Either[GraphQLFailure, JobSearchFilter] =
    JobDiscoveryValidation
      .filter(jobFilter(value))
      .toEither
      .leftMap(errors => GraphQLFailureCatalog.classify(UseCaseError.Search(SearchError.accumulated(errors))))

  def filterSnapshot(value: JobSearchFilter): SearchFilterSnapshot =
    SearchFilterSnapshot.jobCriteria(value.city, value.skills, value.createdAfter)

  /** Resolves the client-supplied search id or lets the service generate one. */
  def searchIdFor(hiring: HiringGraphQLServices, supplied: Option[UUID]): HiringGraphQLResult[UUID] =
    EitherT.liftF[IO, HiringGraphQLFailure, UUID](hiring.searchSessions.searchId(supplied))

  /** Recording is best-effort: the caller already holds the results, so a failed recording is logged once and the
    * results and `searchId` are still returned. Cancellation is not an error and still propagates; there is no retry.
    */
  def recordSearch[A](
      requestContext: RequestContext,
      hiring: HiringGraphQLServices,
      actorId: UserId,
      kind: OperationalEventPayload.SearchKind,
      searchId: UUID,
      filter: SearchFilterSnapshot,
      model: Option[String] = None
  )(results: List[A])(idOf: A => String, scoreOf: A => Double): HiringGraphQLResult[Unit] =
    EitherT.liftF[IO, HiringGraphQLFailure, Unit](
      hiring.searchSessions
        .record(
          actorId,
          kind,
          searchId,
          filter,
          model,
          results.map(result => SearchSessionEntry(idOf(result), scoreOf(result)))
        )
        .value
        .attempt
        .flatMap {
          case Right(Right(()))   => IO.unit
          case Right(Left(error)) =>
            reportFailure(
              requestContext,
              hiring,
              actorId,
              kind,
              searchId,
              Right(GraphQLFailureCatalog.classify(error).code)
            )
          case Left(error) => reportFailure(requestContext, hiring, actorId, kind, searchId, Left(error))
        }
    )

  /** Logs the failed recording and publishes its operational event. Publishing is best-effort too: a failure to emit it
    * is dropped (the log entry already exists) and never fails the search; cancellation still propagates.
    */
  private def reportFailure(
      requestContext: RequestContext,
      hiring: HiringGraphQLServices,
      actorId: UserId,
      kind: OperationalEventPayload.SearchKind,
      searchId: UUID,
      cause: Either[Throwable, String]
  ): IO[Unit] =
    requestContext.reportSearchRecordingFailure(kind, cause) *>
      hiring.searchSessions
        .recordFailure(actorId, kind, searchId, cause.fold(_.getClass.getName, identity))
        .attempt
        .void

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

  def authenticatedSearch(
      context: Context[RequestContext, Unit]
  ): HiringGraphQLResult[
    (ActorContext, HiringGraphQLServices, com.example.graphQL.cats.service.protocol.SearchUseCases[IO])
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

  private def validationError(
      errors: NonEmptyList[DomainValidationError],
      fieldPath: InputFieldPaths.Path
  ): ValidationError = {
    val failure = GraphQLFailureCatalog.classify(UseCaseError.ValidationFailed(errors))
    ValidationError(failure.code, failure.message, failure.field.flatMap(fieldPath))
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
    PageSize
      .fromInt(first)
      .toEither
      .leftMap(errors => GraphQLFailureCatalog.classify(UseCaseError.ValidationFailed(errors)))

}
