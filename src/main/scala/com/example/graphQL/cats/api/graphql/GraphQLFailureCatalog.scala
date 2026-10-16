package com.example.graphQL.cats.api.graphql

import cats.data.NonEmptyList
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.{GraphQLFailure, SearchRefusal}
import com.example.graphQL.cats.domain.error.{DomainError, DomainValidationError}
import com.example.graphQL.cats.domain.workflow.InterviewWorkflowError
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.{
  AccountError,
  AnalyticsError,
  AuthenticationError,
  AvailabilityError,
  SearchError,
  UseCaseError
}

private[graphql] object GraphQLFailureCatalog {
  // Several entries share FORBIDDEN and UNAUTHORIZED; they use one generic description each, so the public
  // ErrorCode description does not depend on catalog order.
  private val GenericForbidden = "The caller may not perform this operation on this resource."
  private val GenericUnauthorized = "Authentication is required, or the credentials or account are no longer valid."

  private enum FailureMetadata(
      val code: String,
      val exceptional: Boolean,
      val description: String,
      val search: SearchRefusal = SearchRefusal.Other
  ) {
    case AuthenticationRequired
        extends FailureMetadata(
          "UNAUTHORIZED",
          exceptional = true,
          GenericUnauthorized
        )
    case SingletonAdminForbidden extends FailureMetadata("FORBIDDEN", exceptional = true, GenericForbidden)
    case AdminBootstrapRequired
        extends FailureMetadata(
          "ADMIN_BOOTSTRAP_REQUIRED",
          exceptional = false,
          "The first Admin has not been bootstrapped yet."
        )
    case AdminAlreadyBootstrapped
        extends FailureMetadata(
          "ADMIN_ALREADY_BOOTSTRAPPED",
          exceptional = false,
          "The Admin bootstrap has already completed."
        )
    case RegistrationFailed
        extends FailureMetadata(
          "REGISTRATION_FAILED",
          exceptional = false,
          "Registration was refused; the reason is deliberately not disclosed."
        )
    case InvalidCredentials
        extends FailureMetadata("INVALID_CREDENTIALS", exceptional = false, "The name or password is wrong.")
    case DeletedAccountAuthenticationRequired
        extends FailureMetadata("UNAUTHORIZED", exceptional = false, GenericUnauthorized)
    case ProfileRoleMismatch
        extends FailureMetadata(
          "PROFILE_ROLE_MISMATCH",
          exceptional = false,
          "The profile variant does not match the role of the account."
        )
    case ProfileUnsupportedForRole
        extends FailureMetadata(
          "PROFILE_UNSUPPORTED_FOR_ROLE",
          exceptional = false,
          "The role has no profile, so none can be set."
        )
    case InvalidPassword
        extends FailureMetadata(
          "INVALID_PASSWORD",
          exceptional = false,
          "The password is shorter than the minimum length."
        )
    case AccountAlreadyDeleted
        extends FailureMetadata("ACCOUNT_ALREADY_DELETED", exceptional = false, "The account is already deleted.")
    case AdminBootstrapOnly
        extends FailureMetadata(
          "ADMIN_BOOTSTRAP_ONLY",
          exceptional = false,
          "Admin accounts can only be created through the bootstrap configuration."
        )
    case ServiceNotReady
        extends FailureMetadata(
          "SERVICE_NOT_READY",
          exceptional = true,
          "The service is not ready to handle the request; retry later."
        )
    case Forbidden
        extends FailureMetadata(
          "FORBIDDEN",
          exceptional = true,
          GenericForbidden
        )
    case SchemaExposureDisabled
        extends FailureMetadata(
          "FORBIDDEN",
          exceptional = false,
          GenericForbidden
        )
    case NotFound
        extends FailureMetadata(
          "NOT_FOUND",
          exceptional = false,
          "The resource does not exist or is not visible to the caller."
        )
    case DuplicateApplication
        extends FailureMetadata(
          "DUPLICATE_APPLICATION",
          exceptional = false,
          "The candidate has already applied to this job."
        )
    case SearchSessionPending
        extends FailureMetadata(
          "SEARCH_SESSION_PENDING",
          exceptional = false,
          "The search session is still being prepared; retry shortly."
        )
    case SearchSessionUnavailable
        extends FailureMetadata(
          "SEARCH_SESSION_UNAVAILABLE",
          exceptional = false,
          "The search session is not available."
        )
    case JobMustBeOpen
        extends FailureMetadata("JOB_MUST_BE_OPEN", exceptional = false, "The job must be open for this operation.")
    case CandidateRequired
        extends FailureMetadata(
          "CANDIDATE_REQUIRED",
          exceptional = false,
          "Only a Candidate may perform this operation."
        )
    case RecruiterRequired
        extends FailureMetadata(
          "RECRUITER_REQUIRED",
          exceptional = false,
          "Only a Recruiter may perform this operation."
        )
    case InvalidJobTransition
        extends FailureMetadata(
          "INVALID_JOB_TRANSITION",
          exceptional = false,
          "The job cannot move to the requested status."
        )
    case InvalidInitialJobStatus
        extends FailureMetadata("INVALID_INITIAL_JOB_STATUS", exceptional = false, "New jobs must start open.")
    case InvalidStatusTransition
        extends FailureMetadata(
          "INVALID_STATUS_TRANSITION",
          exceptional = false,
          "The application or interview cannot move to the requested state from its current state."
        )
    case RejectionFeedbackRequired
        extends FailureMetadata(
          "REJECTION_FEEDBACK_REQUIRED",
          exceptional = false,
          "Rejecting an application requires feedback."
        )
    case DeclineReasonRequired
        extends FailureMetadata(
          "DECLINE_REASON_REQUIRED",
          exceptional = false,
          "Declining an application requires a reason."
        )
    case Conflict
        extends FailureMetadata(
          "CONFLICT",
          exceptional = false,
          "A concurrent change conflicted with this operation; reload and retry."
        )
    case RepositoryUnavailable
        extends FailureMetadata("UNAVAILABLE", exceptional = true, "The data store is temporarily unavailable.")
    case MissingEmbedding
        extends FailureMetadata(
          "MISSING_EMBEDDING",
          exceptional = true,
          "A required embedding has not been computed yet.",
          SearchRefusal.Unavailable
        )
    case StaleEmbedding
        extends FailureMetadata(
          "STALE_EMBEDDING",
          exceptional = true,
          "A required embedding is out of date.",
          SearchRefusal.Unavailable
        )
    case InputTooLarge
        extends FailureMetadata(
          "INPUT_TOO_LARGE",
          exceptional = true,
          "An input text exceeds its maximum length.",
          SearchRefusal.InvalidInput
        )
    case InvalidSearchFilter
        extends FailureMetadata(
          "INVALID_SEARCH_FILTER",
          exceptional = false,
          "A search filter value is invalid.",
          SearchRefusal.InvalidInput
        )
    case ProviderUnavailable
        extends FailureMetadata(
          "PROVIDER_UNAVAILABLE",
          exceptional = true,
          "The embedding provider is unavailable.",
          SearchRefusal.Unavailable
        )
    case VectorSearchUnavailable
        extends FailureMetadata(
          "VECTOR_SEARCH_UNAVAILABLE",
          exceptional = true,
          "Semantic search is unavailable.",
          SearchRefusal.Unavailable
        )
    case FacetsUnavailable
        extends FailureMetadata(
          "FACETS_UNAVAILABLE",
          exceptional = true,
          "Discovery facets are unavailable.",
          SearchRefusal.Unavailable
        )
    case ValidationFailed
        extends FailureMetadata(
          "VALIDATION_FAILED",
          exceptional = false,
          "One or more input fields are invalid.",
          SearchRefusal.InvalidInput
        )
    case AnalyticsUnavailable
        extends FailureMetadata("ANALYTICS_UNAVAILABLE", exceptional = true, "Analytics reports are unavailable.")
    case AnalyticsInvalidPeriod
        extends FailureMetadata(
          "INVALID_ANALYTICS_PERIOD",
          exceptional = false,
          "The analytics period must be ordered and at most 30 days."
        )
    case AnalyticsContextRequired
        extends FailureMetadata(
          "ANALYTICS_CONTEXT_REQUIRED",
          exceptional = true,
          "Account deletion needs analytics erasure context that is not available."
        )
    case InvalidCursor
        extends FailureMetadata(
          "INVALID_CURSOR",
          exceptional = false,
          "The pagination cursor is malformed, expired or does not match the query."
        )
    case WrongCursorKind
        extends FailureMetadata(
          "WRONG_CURSOR_KIND",
          exceptional = false,
          "The cursor belongs to a different connection."
        )
    case InvalidFilter
        extends FailureMetadata(
          "INVALID_FILTER",
          exceptional = false,
          "The combination of arguments is inconsistent.",
          SearchRefusal.InvalidInput
        )
    case RateLimited
        extends FailureMetadata(
          "RATE_LIMITED",
          exceptional = false,
          "Too many requests; retry after the indicated time."
        )
    case StaleRevision
        extends FailureMetadata(
          "STALE_REVISION",
          exceptional = false,
          "The workflow changed since the supplied revision was read."
        )
    case InterviewAlreadyStarted
        extends FailureMetadata("INTERVIEW_ALREADY_STARTED", exceptional = false, "The interview has already started.")
    case InterviewAlreadyCancelled
        extends FailureMetadata(
          "INTERVIEW_ALREADY_CANCELLED",
          exceptional = false,
          "The interview is already cancelled."
        )
    case InterviewNotSettled
        extends FailureMetadata(
          "INTERVIEW_NOT_SETTLED",
          exceptional = false,
          "The interview is still being updated; retry later."
        )
    case InvalidInterviewInterval
        extends FailureMetadata(
          "INVALID_INTERVIEW_INTERVAL",
          exceptional = false,
          "The interview must start in the future and end after it starts."
        )
    case RescheduleIntervalUnchanged
        extends FailureMetadata(
          "RESCHEDULE_INTERVAL_UNCHANGED",
          exceptional = false,
          "The proposed time equals the current interview time."
        )
    case ProposalAlreadyOpen
        extends FailureMetadata("PROPOSAL_ALREADY_OPEN", exceptional = false, "A reschedule proposal is already open.")
    case NoOpenProposal
        extends FailureMetadata("NO_OPEN_PROPOSAL", exceptional = false, "There is no open reschedule proposal.")
    case ProposalExpired
        extends FailureMetadata("PROPOSAL_EXPIRED", exceptional = false, "The reschedule proposal has expired.")
    case NoRescheduleRequest
        extends FailureMetadata("NO_RESCHEDULE_REQUEST", exceptional = false, "There is no reschedule request.")
  }

  /** Every code a failure can carry, with its one description. This is the single source of the public `ErrorCode`
    * enum, so the enum and the codes the API emits cannot drift apart.
    */
  def errorCodes: List[(String, String)] =
    FailureMetadata.values.toList.map(metadata => metadata.code -> metadata.description).distinct

  /** Codes whose catalog entries disagree on the description; empty when every code has exactly one. */
  private[graphql] def codesWithConflictingDescriptions: List[String] =
    errorCodes.groupBy(_._1).collect { case (code, entries) if entries.size > 1 => code }.toList.sorted

  def classify(error: UseCaseError): GraphQLFailure =
    error match {
      case UseCaseError.Authentication(value)    => classifyAuthentication(value)
      case UseCaseError.Account(value)           => classifyAccount(value)
      case UseCaseError.Availability(value)      => classifyAvailability(value)
      case UseCaseError.Analytics(value)         => classifyAnalytics(value)
      case UseCaseError.InterviewRule(value)     => classifyInterviewRule(value)
      case UseCaseError.Domain(value)            => classifyDomain(value)
      case UseCaseError.Repository(value)        => classifyRepository(value)
      case UseCaseError.Search(value)            => classifySearch(value)
      case UseCaseError.ValidationFailed(errors) => classifyValidation(errors)
    }

  /** Cursor decoding and binding failures raised by connection and nearby-search arguments. */
  def classifyCursor(error: CursorCodec.CursorError): GraphQLFailure =
    error match {
      case CursorCodec.CursorError.Malformed(_) => failure(FailureMetadata.InvalidCursor, "Invalid cursor")
      case CursorCodec.CursorError.WrongKind(_) =>
        failure(FailureMetadata.WrongCursorKind, "Cursor belongs to a different connection")
      case CursorCodec.CursorError.CriteriaMismatch =>
        failure(FailureMetadata.InvalidCursor, "Nearby cursor does not match criteria")
    }

  /** An interview action refused by the per-actor allowance; it carries no limit, window or counter. */
  def interviewActionRateLimited: GraphQLFailure =
    failure(FailureMetadata.RateLimited, "Too many interview actions; retry later")

  /** An introspection operation refused while schema exposure is off; it names neither the schema nor its types. */
  def schemaExposureDisabled: GraphQLFailure =
    failure(FailureMetadata.SchemaExposureDisabled, "Schema introspection is not available")

  /** A request refused by a per-caller allowance; it carries no limit, window or counter. */
  def requestRateLimited: GraphQLFailure =
    failure(FailureMetadata.RateLimited, "Too many requests; retry later")

  /** Authentication failures stay top-level GraphQL errors on every root, including the non-exceptional deleted-account
    * `UNAUTHORIZED`, so a client never reads them as a business outcome.
    */
  def isAuthenticationFailure(error: UseCaseError): Boolean =
    error match {
      case UseCaseError.Authentication(_) | UseCaseError.Account(AccountError.DeletedAccount) => true
      case _                                                                                  => false
    }

  /** A query refused because its caller's cost budget for the window is spent; it carries no budget or counter. */
  def costBudgetExhausted: GraphQLFailure =
    failure(FailureMetadata.RateLimited, "Query cost budget exhausted; retry later")

  /** A role or ownership refusal of a mutation is a typed payload for interview actions, so one refused alias does not
    * discard the results of the others in a batched request.
    */
  def isAuthorizationRefusal(error: UseCaseError): Boolean =
    error match {
      case UseCaseError.Domain(DomainError.Forbidden) | UseCaseError.Repository(RepositoryError.AuthorityRevoked) =>
        true
      case _ => false
    }

  /** The interview rules refuse with stable codes that name no workflow, user, time or text. */
  def classifyInterviewRule(error: InterviewWorkflowError): GraphQLFailure =
    error match {
      case InterviewWorkflowError.StaleRevision => failure(FailureMetadata.StaleRevision, "Stale revision")
      case InterviewWorkflowError.InvalidTransition | InterviewWorkflowError.ApplicationMustBeAccepted =>
        failure(FailureMetadata.InvalidStatusTransition, "This action is not allowed in the current state")
      case InterviewWorkflowError.InterviewAlreadyStarted =>
        failure(FailureMetadata.InterviewAlreadyStarted, "The interview has already started")
      case InterviewWorkflowError.InterviewAlreadyCancelled =>
        failure(FailureMetadata.InterviewAlreadyCancelled, "The interview is already cancelled")
      case InterviewWorkflowError.InterviewNotSettled =>
        failure(FailureMetadata.InterviewNotSettled, "The interview is still being updated; retry later")
      case InterviewWorkflowError.StartMustBeInFuture | InterviewWorkflowError.EndMustFollowStart =>
        failure(
          FailureMetadata.InvalidInterviewInterval,
          "The interval must start in the future and end after it starts"
        )
      case InterviewWorkflowError.RescheduleIntervalUnchanged =>
        failure(FailureMetadata.RescheduleIntervalUnchanged, "The proposed time equals the current interview time")
      case InterviewWorkflowError.ProposalAlreadyOpen =>
        failure(FailureMetadata.ProposalAlreadyOpen, "A reschedule proposal is already open")
      case InterviewWorkflowError.NoOpenProposal =>
        failure(FailureMetadata.NoOpenProposal, "There is no open reschedule proposal")
      case InterviewWorkflowError.ProposalExpired =>
        failure(FailureMetadata.ProposalExpired, "The reschedule proposal has expired")
      case InterviewWorkflowError.NoRescheduleRequest =>
        failure(FailureMetadata.NoRescheduleRequest, "There is no reschedule request")
      // A proposal lifetime outside its bounds is rejected when the service starts; it cannot be caused by a caller.
      case InterviewWorkflowError.ProposalTtlOutOfRange =>
        failure(FailureMetadata.RepositoryUnavailable, "Service unavailable")
    }

  /** Request arguments that are individually valid but inconsistent together. */
  def inconsistentArguments(message: String): GraphQLFailure = failure(FailureMetadata.InvalidFilter, message)

  private def classifyAuthentication(error: AuthenticationError): GraphQLFailure =
    error match {
      case AuthenticationError.Unauthorized =>
        failure(FailureMetadata.AuthenticationRequired, "Authentication required")
      case AuthenticationError.SingletonAdminViolation => failure(FailureMetadata.SingletonAdminForbidden, "Forbidden")
    }

  private def classifyAccount(error: AccountError): GraphQLFailure =
    error match {
      case AccountError.BootstrapRequired =>
        failure(FailureMetadata.AdminBootstrapRequired, "The first Admin must be bootstrapped")
      case AccountError.AlreadyBootstrapped =>
        failure(FailureMetadata.AdminAlreadyBootstrapped, "Admin bootstrap is already complete")
      case AccountError.NameTaken          => failure(FailureMetadata.RegistrationFailed, "Registration failed")
      case AccountError.InvalidCredentials => failure(FailureMetadata.InvalidCredentials, "Invalid credentials")
      case AccountError.DeletedAccount     =>
        failure(FailureMetadata.DeletedAccountAuthenticationRequired, "Authentication required")
      case AccountError.ProfileRoleMismatch =>
        failure(FailureMetadata.ProfileRoleMismatch, "Profile does not match the selected role")
      case AccountError.ProfileUnsupportedForRole =>
        failure(FailureMetadata.ProfileUnsupportedForRole, "This role does not support a profile")
      case AccountError.AccountAlreadyDeleted =>
        failure(FailureMetadata.AccountAlreadyDeleted, "Account is already deleted")
      case AccountError.AdminSignupForbidden =>
        failure(FailureMetadata.AdminBootstrapOnly, "Admin accounts can only be created through bootstrap")
    }

  private def classifyAvailability(error: AvailabilityError): GraphQLFailure =
    error match {
      case AvailabilityError.ServiceNotReady => failure(FailureMetadata.ServiceNotReady, "Service not ready")
    }

  private def classifyAnalytics(error: AnalyticsError): GraphQLFailure =
    error match {
      case AnalyticsError.ReportsUnavailable =>
        failure(FailureMetadata.AnalyticsUnavailable, "Analytics reports are unavailable")
      case AnalyticsError.InvalidPeriod =>
        failure(FailureMetadata.AnalyticsInvalidPeriod, "Analytics period must be ordered and at most 30 days")
      case AnalyticsError.ErasureContextRequired =>
        failure(FailureMetadata.AnalyticsContextRequired, "Account deletion is unavailable")
      case AnalyticsError.ErasureWorkerUnavailable | AnalyticsError.ErasureNotCompleted =>
        failure(FailureMetadata.AnalyticsUnavailable, "Account deletion is not available right now")
    }

  private def classifyDomain(error: DomainError): GraphQLFailure =
    error match {
      case DomainError.Forbidden            => failure(FailureMetadata.Forbidden, "Forbidden")
      case DomainError.NotFound(entity)     => failure(FailureMetadata.NotFound, s"$entity not found")
      case DomainError.SearchSessionPending =>
        failure(FailureMetadata.SearchSessionPending, "Search session is being prepared; retry shortly")
      case DomainError.SearchSessionUnavailable =>
        failure(FailureMetadata.SearchSessionUnavailable, "Search session is unavailable")
      case DomainError.JobMustBeOpen     => failure(FailureMetadata.JobMustBeOpen, "Job must be open")
      case DomainError.CandidateRequired => failure(FailureMetadata.CandidateRequired, "Candidate role required")
      case DomainError.RecruiterRequired => failure(FailureMetadata.RecruiterRequired, "Recruiter role required")
      case DomainError.InvalidJobTransition(_, _) =>
        failure(FailureMetadata.InvalidJobTransition, "Invalid job transition")
      case DomainError.InvalidInitialJobStatus(_) =>
        failure(FailureMetadata.InvalidInitialJobStatus, "New jobs must be open")
      case DomainError.InvalidStatusTransition(_, _) =>
        failure(FailureMetadata.InvalidStatusTransition, "Invalid application status transition")
      case DomainError.RejectionFeedbackRequired =>
        failure(FailureMetadata.RejectionFeedbackRequired, "Rejection feedback is required")
      case DomainError.DeclineReasonRequired =>
        failure(FailureMetadata.DeclineReasonRequired, "Decline reason is required")
    }

  private def classifyRepository(error: RepositoryError): GraphQLFailure =
    error match {
      case RepositoryError.DuplicateApplication =>
        failure(FailureMetadata.DuplicateApplication, "Application already exists")
      case RepositoryError.AuthorityRevoked => failure(FailureMetadata.Forbidden, "Forbidden")
      case RepositoryError.Conflict         => failure(FailureMetadata.Conflict, "Conflict")
      case RepositoryError.InvalidEvent | RepositoryError.InvalidStoredData | RepositoryError.MissingStoredResult =>
        failure(FailureMetadata.RepositoryUnavailable, "Repository unavailable")
      case RepositoryError.Unavailable | RepositoryError.Overloaded | RepositoryError.VectorSearchUnavailable =>
        failure(FailureMetadata.RepositoryUnavailable, "Repository unavailable")
    }

  private def classifySearch(error: SearchError): GraphQLFailure =
    error match {
      case SearchError.MissingEmbedding(entity) =>
        failure(FailureMetadata.MissingEmbedding, s"$entity embedding is missing")
      case SearchError.StaleEmbedding(entity) => failure(FailureMetadata.StaleEmbedding, s"$entity embedding is stale")
      case SearchError.InputTooLarge(field, maximum) =>
        failure(FailureMetadata.InputTooLarge, s"$field must be at most $maximum characters").copy(field = Some(field))
      case SearchError.InvalidFilter(field) =>
        failure(FailureMetadata.InvalidSearchFilter, s"Invalid $field filter").copy(field = Some(field))
      case SearchError.InvalidFilters(fields) =>
        failure(FailureMetadata.InvalidSearchFilter, s"Invalid ${fields.toList.mkString(", ")} filters")
          .copy(field = Some(fields.head))
      case SearchError.ProviderUnavailable =>
        failure(FailureMetadata.ProviderUnavailable, "Embedding provider unavailable")
      case SearchError.VectorSearchUnavailable =>
        failure(FailureMetadata.VectorSearchUnavailable, "Vector search unavailable")
      case SearchError.FacetsUnavailable =>
        GraphQLFailure(
          FailureMetadata.FacetsUnavailable.code,
          "Facets unavailable",
          FailureMetadata.FacetsUnavailable.exceptional,
          Some("SEARCH_UNAVAILABLE"),
          search = FailureMetadata.FacetsUnavailable.search
        )
    }

  /** A short password as the only violation is `INVALID_PASSWORD`; combined with other violations the failure stays
    * `VALIDATION_FAILED` and lists every message.
    */
  private def classifyValidation(errors: NonEmptyList[DomainValidationError]): GraphQLFailure = {
    val fields = errors.toList.map(validationMessage).mkString(", ")
    val metadata = errors match {
      case NonEmptyList(DomainValidationError.PasswordTooShort(_, _), Nil) => FailureMetadata.InvalidPassword
      case _                                                               => FailureMetadata.ValidationFailed
    }
    failure(metadata, if (fields.isEmpty) "Validation failed" else fields)
      .copy(field = errors.head match {
        case DomainValidationError.BlankField(field)               => Some(field)
        case DomainValidationError.EmptyCollection(field)          => Some(field)
        case DomainValidationError.InvalidNumber(field, _, _, _)   => Some(field)
        case DomainValidationError.TextTooLong(field, _, _)        => Some(field)
        case DomainValidationError.ByteLengthExceeded(field, _, _) => Some(field)
        case DomainValidationError.TooManyValues(field, _, _)      => Some(field)
        case DomainValidationError.PasswordTooShort(_, _)          => Some("password")
        case DomainValidationError.InvalidCoordinates              => Some("coordinates")
      })
  }

  private def validationMessage(error: DomainValidationError): String =
    error match {
      case DomainValidationError.BlankField(field)                         => s"$field is required"
      case DomainValidationError.EmptyCollection(field)                    => s"$field must not be empty"
      case DomainValidationError.InvalidNumber(field, minimum, maximum, _) =>
        s"$field must be between $minimum and $maximum"
      case DomainValidationError.TextTooLong(field, maximum, _)        => s"$field must be at most $maximum characters"
      case DomainValidationError.ByteLengthExceeded(field, maximum, _) => s"$field must be at most $maximum bytes"
      case DomainValidationError.TooManyValues(field, maximum, _)      => s"$field must contain at most $maximum values"
      case DomainValidationError.PasswordTooShort(minimum, _)          => s"password must be at least $minimum bytes"
      case DomainValidationError.InvalidCoordinates                    => "Coordinates are outside geographic bounds"
    }

  private def failure(metadata: FailureMetadata, message: String): GraphQLFailure =
    GraphQLFailure(metadata.code, message, metadata.exceptional, search = metadata.search)
}
