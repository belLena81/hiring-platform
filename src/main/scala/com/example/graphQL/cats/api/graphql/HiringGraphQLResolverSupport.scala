package com.example.graphQL.cats.api.graphql

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.admission.AuthRateLimiter
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.domain.error.{DomainError as DomainFailure, DomainValidationError}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.service.{AccountError, ActorContext, AuthenticationError, AvailabilityError, ProbeResult, SearchError, UseCaseError}
import com.example.graphQL.cats.shared.events.{OperationalEvents, SearchSession, SearchSessionResult}
import com.example.graphQL.cats.shared.pagination.*
import com.example.graphQL.cats.shared.search.JobSearchFilter
import io.circe.Json
import sangria.schema.Context

import java.time.Instant
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.concurrent.duration.*

private[graphql] object HiringGraphQLResolverSupport {
  def raiseOnUseCaseError[A](value: IO[Either[UseCaseError, A]]): IO[A] =
    value.map(_.leftMap(RequestContext.ReadFailure(_))).rethrow

  def inputResult[A](value: Either[GraphQLFailure, A]): IO[A] =
    IO.fromEither(value.leftMap(error => RequestContext.FieldFailure(error.code, error.message)))

  def mutationResult[A](value: IO[Either[UseCaseError, A]]): IO[MutationOutcome[A]] =
    value.flatMap {
      case Right(result) => IO.pure(result)
      case Left(UseCaseError.ValidationFailed(errors)) => IO.pure(validationError(errors))
      case Left(error) =>
        val failure = toGraphQLFailure(error)
        if (failure.exceptional) liftUseCase(error)
        else IO.pure(DomainError(failure.code, failure.message))
    }

  def mutationResult[A](value: Either[UseCaseError, A]): IO[MutationOutcome[A]] =
    mutationResult(IO.pure(value))

  def executeMutation[A](
      hiring: HiringGraphQLServices,
      operation: String,
      actorScope: String,
      idempotencyKey: UUID,
      canonicalInput: Json,
      entity: A => MutationEntityReference,
      replay: MutationEntityReference => IO[Either[UseCaseError, A]]
  )(write: MutationWriteContext => IO[Either[UseCaseError, A]]): IO[Either[UseCaseError, A]] =
    (IO.realTimeInstant, IO.realTimeInstant).flatMapN { (now, expiresBase) =>
      val key = MutationReceiptKey(operation, actorScope, idempotencyKey)
      val fingerprint = MutationReceiptFingerprint.fromCanonicalInput(canonicalInput.noSpaces)
      hiring.mutationReceipts.execute(key, fingerprint, now, expiresBase.plusSeconds(7.days.toSeconds)) { context =>
        write(context).map {
          case Left(error) => Right(Left(error))
          case Right(value) => Right(Right(MutationReceiptWrite(value, entity(value))))
        }
      }.flatMap {
        case Left(error) => IO.pure(Left(UseCaseError.Repository(error)))
        case Right(MutationReceiptExecution.Applied(value, _)) => IO.pure(Right(value))
        case Right(MutationReceiptExecution.Replay(reference)) => replay(reference)
        case Right(MutationReceiptExecution.Rejected(error)) => IO.pure(Left(error))
        case Right(MutationReceiptExecution.FingerprintMismatch) => IO.pure(Left(UseCaseError.Repository(RepositoryError.Conflict)))
        case Right(MutationReceiptExecution.InProgress) => IO.pure(Left(UseCaseError.Repository(RepositoryError.Unavailable)))
      }
    }

  def actorScope(actor: ActorContext): String = actor.userId.value.toString

  def publicActorScope(name: String): String = s"public:${name.trim.toLowerCase(java.util.Locale.ROOT)}"

  def timestamped[A](f: (Instant, UUID) => IO[A]): IO[A] =
    (IO.realTimeInstant, IO.randomUUID).flatMapN(f)

  def searchEventId(searchId: UUID): UUID =
    UUID.nameUUIDFromBytes(s"search-performed:$searchId".getBytes(StandardCharsets.UTF_8))

  def jobFilter(value: Option[JobFilterGraphQLInput]): JobSearchFilter =
    value match {
      case None => JobSearchFilter(None, Set.empty, None)
      case Some(filter) => JobSearchFilter(filter.city, filter.skills.fold(Set.empty[String])(_.toSet), filter.createdAfter)
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
    error match {
      case UseCaseError.Authentication(AuthenticationError.Unauthorized) => GraphQLFailure("UNAUTHORIZED", "Authentication required", exceptional = true)
      case UseCaseError.Authentication(AuthenticationError.SingletonAdminViolation) => GraphQLFailure("FORBIDDEN", "Forbidden", exceptional = true)
      case UseCaseError.Account(AccountError.BootstrapRequired) => GraphQLFailure("ADMIN_BOOTSTRAP_REQUIRED", "The first Admin must be bootstrapped", exceptional = false)
      case UseCaseError.Account(AccountError.AlreadyBootstrapped) => GraphQLFailure("ADMIN_ALREADY_BOOTSTRAPPED", "Admin bootstrap is already complete", exceptional = false)
      case UseCaseError.Account(AccountError.NameTaken) => GraphQLFailure("REGISTRATION_FAILED", "Registration failed", exceptional = false)
      case UseCaseError.Account(AccountError.InvalidCredentials) => GraphQLFailure("INVALID_CREDENTIALS", "Invalid credentials", exceptional = false)
      case UseCaseError.Account(AccountError.DeletedAccount) => GraphQLFailure("UNAUTHORIZED", "Authentication required", exceptional = false)
      case UseCaseError.Account(AccountError.ProfileRoleMismatch) => GraphQLFailure("PROFILE_ROLE_MISMATCH", "Profile does not match the selected role", exceptional = false)
      case UseCaseError.Account(AccountError.ProfileUnsupportedForRole) => GraphQLFailure("PROFILE_UNSUPPORTED_FOR_ROLE", "This role does not support a profile", exceptional = false)
      case UseCaseError.Account(AccountError.PasswordPolicyViolation) => GraphQLFailure("INVALID_PASSWORD", "Password does not meet policy", exceptional = false)
      case UseCaseError.Account(AccountError.AccountAlreadyDeleted) => GraphQLFailure("ACCOUNT_ALREADY_DELETED", "Account is already deleted", exceptional = false)
      case UseCaseError.Account(AccountError.AdminSignupForbidden) => GraphQLFailure("ADMIN_BOOTSTRAP_ONLY", "Admin accounts can only be created through bootstrap", exceptional = false)
      case UseCaseError.Availability(AvailabilityError.ServiceNotReady) => GraphQLFailure("SERVICE_NOT_READY", "Service not ready", exceptional = true)
      case UseCaseError.Domain(DomainFailure.Forbidden) => GraphQLFailure("FORBIDDEN", "Forbidden", exceptional = true)
      case UseCaseError.Domain(DomainFailure.NotFound(entity)) => GraphQLFailure("NOT_FOUND", s"$entity not found", exceptional = false)
      case UseCaseError.Domain(DomainFailure.DuplicateApplication) => GraphQLFailure("DUPLICATE_APPLICATION", "Application already exists", exceptional = false)
      case UseCaseError.Domain(DomainFailure.SearchSessionPending) => GraphQLFailure("SEARCH_SESSION_PENDING", "Search session is being prepared; retry shortly", exceptional = false)
      case UseCaseError.Domain(DomainFailure.SearchSessionUnavailable) => GraphQLFailure("SEARCH_SESSION_UNAVAILABLE", "Search session is unavailable", exceptional = false)
      case UseCaseError.Domain(DomainFailure.JobMustBeOpen) => GraphQLFailure("JOB_MUST_BE_OPEN", "Job must be open", exceptional = false)
      case UseCaseError.Domain(DomainFailure.CandidateRequired) => GraphQLFailure("CANDIDATE_REQUIRED", "Candidate role required", exceptional = false)
      case UseCaseError.Domain(DomainFailure.RecruiterRequired) => GraphQLFailure("RECRUITER_REQUIRED", "Recruiter role required", exceptional = false)
      case UseCaseError.Domain(DomainFailure.InvalidJobTransition(_, _)) => GraphQLFailure("INVALID_JOB_TRANSITION", "Invalid job transition", exceptional = false)
      case UseCaseError.Domain(DomainFailure.InvalidInitialJobStatus(_)) => GraphQLFailure("INVALID_INITIAL_JOB_STATUS", "New jobs must be open", exceptional = false)
      case UseCaseError.Domain(DomainFailure.InvalidStatusTransition(_, _)) => GraphQLFailure("INVALID_STATUS_TRANSITION", "Invalid application status transition", exceptional = false)
      case UseCaseError.Domain(DomainFailure.RejectionFeedbackRequired) => GraphQLFailure("REJECTION_FEEDBACK_REQUIRED", "Rejection feedback is required", exceptional = false)
      case UseCaseError.Domain(DomainFailure.DeclineReasonRequired) => GraphQLFailure("DECLINE_REASON_REQUIRED", "Decline reason is required", exceptional = false)
      case UseCaseError.Repository(RepositoryError.DuplicateApplication) => GraphQLFailure("DUPLICATE_APPLICATION", "Application already exists", exceptional = false)
      case UseCaseError.Repository(RepositoryError.Conflict) => GraphQLFailure("CONFLICT", "Conflict", exceptional = false)
      case UseCaseError.Repository(RepositoryError.Unavailable) => GraphQLFailure("UNAVAILABLE", "Repository unavailable", exceptional = true)
      case UseCaseError.Search(SearchError.MissingEmbedding(entity)) => GraphQLFailure("MISSING_EMBEDDING", s"$entity embedding is missing", exceptional = true)
      case UseCaseError.Search(SearchError.StaleEmbedding(entity)) => GraphQLFailure("STALE_EMBEDDING", s"$entity embedding is stale", exceptional = true)
      case UseCaseError.Search(SearchError.InputTooLarge(field, maximum)) => GraphQLFailure("INPUT_TOO_LARGE", s"$field must be at most $maximum characters", exceptional = true)
      case UseCaseError.Search(SearchError.ProviderUnavailable) => GraphQLFailure("PROVIDER_UNAVAILABLE", "Embedding provider unavailable", exceptional = true)
      case UseCaseError.Search(SearchError.VectorSearchUnavailable) => GraphQLFailure("VECTOR_SEARCH_UNAVAILABLE", "Vector search unavailable", exceptional = true)
      case UseCaseError.ValidationFailed(errors) =>
        val fields = errors.toList.map(validationErrorMessage).mkString(", ")
        GraphQLFailure("VALIDATION_FAILED", if (fields.isEmpty) "Validation failed" else fields, exceptional = false)
    }

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
      after.traverse(decode)
        .leftMap {
          case CursorCodec.CursorError.WrongKind(_) => GraphQLFailure("WRONG_CURSOR_KIND", "Cursor belongs to a different connection", exceptional = false)
          case CursorCodec.CursorError.Malformed(_) => GraphQLFailure("INVALID_CURSOR", "Invalid cursor", exceptional = false)
        }
        .map(cursor => build(cursor, PageSize.next(size)) -> size.value)
    }

  def pageSize(first: Int): Either[GraphQLFailure, PageSize] =
    PageSize.fromInt(first).toEither.leftMap(errors => toGraphQLFailure(UseCaseError.ValidationFailed(errors)))

  private def validationErrorMessage(error: DomainValidationError): String =
    error match {
      case DomainValidationError.BlankField(field) => s"$field is required"
      case DomainValidationError.EmptyCollection(field) => s"$field must not be empty"
      case DomainValidationError.InvalidNumber(field, minimum, maximum, _) => s"$field must be between $minimum and $maximum"
      case DomainValidationError.TextTooLong(field, maximum, _) => s"$field must be at most $maximum characters"
      case DomainValidationError.ByteLengthExceeded(field, maximum, _) => s"$field must be at most $maximum bytes"
      case DomainValidationError.TooManyValues(field, maximum, _) => s"$field must contain at most $maximum values"
    }
}
