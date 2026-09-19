package com.example.graphQL.cats.api.graphql

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.*
import com.example.graphQL.cats.domain.error.{DomainError, DomainValidationError}
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.service.{AccountError, ActorContext, AuthenticationError, AvailabilityError, ProbeResult, RepositoryError, SearchError, UseCaseError}
import com.example.graphQL.cats.shared.pagination.*
import sangria.schema.Context

import java.time.Instant
import java.util.UUID

private[graphql] object HiringGraphQLResolverSupport {
  def liftUseCase[A](value: IO[Either[UseCaseError, A]]): GraphQLStep[A] =
    EitherT(value.map(_.leftMap(toGraphQLError)))

  def timestamped[A](f: (Instant, UUID) => IO[A]): IO[A] =
    (IO.realTimeInstant, IO.randomUUID).mapN(f).flatten

  def complete[A](value: GraphQLStep[A], onError: GraphQLError => A): IO[A] =
    value.value.map(_.fold(onError, identity))

  def authenticatedStep[A](
      context: Context[RequestContext, Unit]
  )(action: (ActorContext, HiringGraphQLServices) => GraphQLStep[A]): GraphQLStep[A] =
    EitherT(authenticated(context).map(_.leftMap(toGraphQLError))).flatMap(action.tupled)

  def authenticatedPayload[A](
      unauthenticated: List[GraphQLError] => A
  )(context: Context[RequestContext, Unit])(
      action: (ActorContext, HiringGraphQLServices) => IO[A]
  ): IO[A] =
    authenticated(context).flatMap(_.fold(error => IO.pure(unauthenticated(List(toGraphQLError(error)))), action.tupled))

  def publicAccountPayload[A](
      unavailable: List[GraphQLError] => A
  )(context: Context[RequestContext, Unit])(
      action: HiringGraphQLServices => IO[A]
  ): IO[A] =
    context.ctx.hiringAvailable.flatMap {
      case ProbeResult.Ready => action(context.ctx.hiring)
      case _ => IO.pure(unavailable(List(toGraphQLError(UseCaseError.availability(AvailabilityError.ServiceNotReady)))))
    }

  def authenticatedSearch(
      context: Context[RequestContext, Unit]
  ): IO[Either[UseCaseError, (ActorContext, HiringGraphQLServices, com.example.graphQL.cats.service.protocol.SearchUseCases[IO])]] =
    authenticated(context).map(_.flatMap { case (actor, hiring) =>
      hiring.semanticSearchService.map(service => (actor, hiring, service)).toRight(UseCaseError.search(SearchError.VectorSearchUnavailable))
    })

  def page(
      first: Int,
      after: Option[String],
      decode: String => Either[CursorCodec.CursorError, JobCursor]
  ): IO[Either[GraphQLError, (JobPageRequest, Int)]] =
    cursorPage(first, after, decode)((cursor, size) => JobPageRequest(None, cursor, size))

  def pageEvent(
      first: Int,
      after: Option[String],
      cursorCodec: CursorCodec[ApplicationEventCursor]
  ): IO[Either[GraphQLError, (ApplicationEventPageRequest, Int)]] =
    cursorPage(first, after, cursorCodec.decode)((cursor, size) => ApplicationEventPageRequest(cursor, size))

  def applicationPage(
      first: Int,
      after: Option[String],
      status: Option[ApplicationStatus],
      cursorCodec: CursorCodec[ApplicationCursor]
  ): IO[Either[GraphQLError, (ApplicationPageRequest, Int)]] =
    cursorPage(first, after, cursorCodec.decode)((cursor, size) => ApplicationPageRequest(status, cursor, size))

  def userPage(
      first: Int,
      after: Option[String],
      status: AccountStatus,
      role: Option[UserRole],
      cursorCodec: CursorCodec[UserCursor]
  ): IO[Either[GraphQLError, (UserPageRequest, Int)]] =
    cursorPage(first, after, cursorCodec.decode)((cursor, size) => UserPageRequest(status, role, cursor, size))

  def connection[A](values: List[A], requested: Int)(cursor: A => String): Connection[A] = {
    val nodes = values.take(requested)
    Connection(nodes.map(value => Edge(value, cursor(value))), PageInfo(values.size > requested, nodes.lastOption.map(cursor)))
  }

  def graphQLErrorConnection[A](error: GraphQLError): Connection[A] =
    Connection(Nil, PageInfo(false, None), List(error))

  def toGraphQLError(error: UseCaseError): GraphQLError =
    error match {
      case UseCaseError.Authentication(AuthenticationError.Unauthorized) => GraphQLError("UNAUTHORIZED", "Authentication required")
      case UseCaseError.Authentication(AuthenticationError.SingletonAdminViolation) => GraphQLError("FORBIDDEN", "Forbidden")
      case UseCaseError.Account(AccountError.BootstrapRequired) => GraphQLError("ADMIN_BOOTSTRAP_REQUIRED", "The first Admin must be bootstrapped")
      case UseCaseError.Account(AccountError.AlreadyBootstrapped) => GraphQLError("ADMIN_ALREADY_BOOTSTRAPPED", "Admin bootstrap is already complete")
      case UseCaseError.Account(AccountError.NameTaken) => GraphQLError("NAME_TAKEN", "Name is already in use")
      case UseCaseError.Account(AccountError.InvalidCredentials) => GraphQLError("INVALID_CREDENTIALS", "Invalid credentials")
      case UseCaseError.Account(AccountError.DeletedAccount) => GraphQLError("UNAUTHORIZED", "Authentication required")
      case UseCaseError.Account(AccountError.ProfileRoleMismatch) => GraphQLError("PROFILE_ROLE_MISMATCH", "Profile does not match the selected role")
      case UseCaseError.Account(AccountError.ProfileUnsupportedForRole) => GraphQLError("PROFILE_UNSUPPORTED_FOR_ROLE", "This role does not support a profile")
      case UseCaseError.Account(AccountError.PasswordPolicyViolation) => GraphQLError("INVALID_PASSWORD", "Password does not meet policy")
      case UseCaseError.Account(AccountError.AccountAlreadyDeleted) => GraphQLError("ACCOUNT_ALREADY_DELETED", "Account is already deleted")
      case UseCaseError.Account(AccountError.AdminSignupForbidden) => GraphQLError("ADMIN_BOOTSTRAP_ONLY", "Admin accounts can only be created through bootstrap")
      case UseCaseError.Availability(AvailabilityError.ServiceNotReady) => GraphQLError("SERVICE_NOT_READY", "Service not ready")
      case UseCaseError.Domain(DomainError.Forbidden) => GraphQLError("FORBIDDEN", "Forbidden")
      case UseCaseError.Domain(DomainError.NotFound(entity)) => GraphQLError("NOT_FOUND", s"$entity not found")
      case UseCaseError.Domain(DomainError.DuplicateApplication) => GraphQLError("DUPLICATE_APPLICATION", "Application already exists")
      case UseCaseError.Domain(DomainError.JobMustBeOpen) => GraphQLError("JOB_MUST_BE_OPEN", "Job must be open")
      case UseCaseError.Domain(DomainError.CandidateRequired) => GraphQLError("CANDIDATE_REQUIRED", "Candidate role required")
      case UseCaseError.Domain(DomainError.RecruiterRequired) => GraphQLError("RECRUITER_REQUIRED", "Recruiter role required")
      case UseCaseError.Domain(DomainError.InvalidJobTransition(_, _)) => GraphQLError("INVALID_JOB_TRANSITION", "Invalid job transition")
      case UseCaseError.Domain(DomainError.InvalidInitialJobStatus(_)) => GraphQLError("INVALID_INITIAL_JOB_STATUS", "New jobs must be open")
      case UseCaseError.Domain(DomainError.InvalidStatusTransition(_, _)) => GraphQLError("INVALID_STATUS_TRANSITION", "Invalid application status transition")
      case UseCaseError.Domain(DomainError.RejectionFeedbackRequired) => GraphQLError("REJECTION_FEEDBACK_REQUIRED", "Rejection feedback is required")
      case UseCaseError.Domain(DomainError.DeclineReasonRequired) => GraphQLError("DECLINE_REASON_REQUIRED", "Decline reason is required")
      case UseCaseError.Repository(RepositoryError.DuplicateApplication) => GraphQLError("DUPLICATE_APPLICATION", "Application already exists")
      case UseCaseError.Repository(RepositoryError.Conflict) => GraphQLError("CONFLICT", "Conflict")
      case UseCaseError.Repository(RepositoryError.Unavailable) => GraphQLError("UNAVAILABLE", "Repository unavailable")
      case UseCaseError.Search(SearchError.MissingEmbedding(entity)) => GraphQLError("MISSING_EMBEDDING", s"$entity embedding is missing")
      case UseCaseError.Search(SearchError.StaleEmbedding(entity)) => GraphQLError("STALE_EMBEDDING", s"$entity embedding is stale")
      case UseCaseError.Search(SearchError.InputTooLarge(field, maximum)) => GraphQLError("INPUT_TOO_LARGE", s"$field must be at most $maximum characters")
      case UseCaseError.Search(SearchError.ProviderUnavailable) => GraphQLError("PROVIDER_UNAVAILABLE", "Embedding provider unavailable")
      case UseCaseError.Search(SearchError.VectorSearchUnavailable) => GraphQLError("VECTOR_SEARCH_UNAVAILABLE", "Vector search unavailable")
      case UseCaseError.ValidationFailed(errors) =>
        val fields = errors.toList.map(validationErrorMessage).mkString(", ")
        GraphQLError("VALIDATION_FAILED", if (fields.isEmpty) "Validation failed" else fields)
    }

  private def authenticated(context: Context[RequestContext, Unit]): IO[Either[UseCaseError, (ActorContext, HiringGraphQLServices)]] =
    context.ctx.actor match {
      case Some(actor) =>
        val hiring = context.ctx.hiring
        context.ctx.hiringAvailable.map {
          case ProbeResult.Ready => Right((actor, hiring))
          case _ => Left(UseCaseError.availability(AvailabilityError.ServiceNotReady))
        }
      case None => IO.pure(Left(UseCaseError.authentication(AuthenticationError.Unauthorized)))
    }

  private def cursorPage[A, B](
      first: Int,
      after: Option[String],
      decode: String => Either[CursorCodec.CursorError, A]
  )(build: (Option[A], PageSize) => B): IO[Either[GraphQLError, (B, Int)]] =
    pageSize(first).map(_.flatMap { size =>
      after.traverse(decode)
        .leftMap {
          case CursorCodec.CursorError.WrongKind(_) => GraphQLError("WRONG_CURSOR_KIND", "Cursor belongs to a different connection")
          case CursorCodec.CursorError.Malformed(_) => GraphQLError("INVALID_CURSOR", "Invalid cursor")
        }
        .map(cursor => build(cursor, PageSize.next(size)) -> size.value)
    })

  def pageSize(first: Int): IO[Either[GraphQLError, PageSize]] =
    IO.pure(PageSize.fromInt(first).toEither.leftMap(errors => toGraphQLError(UseCaseError.ValidationFailed(errors))))

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
