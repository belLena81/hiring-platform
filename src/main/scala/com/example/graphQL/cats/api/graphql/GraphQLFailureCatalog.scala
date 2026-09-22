package com.example.graphQL.cats.api.graphql

import cats.data.NonEmptyList
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.GraphQLFailure
import com.example.graphQL.cats.domain.error.{DomainError, DomainValidationError}
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.service.{AccountError, AnalyticsError, AuthenticationError, AvailabilityError, SearchError, UseCaseError}

private[graphql] object GraphQLFailureCatalog {
  private enum FailureMetadata(val code: String, val exceptional: Boolean) {
    case AuthenticationRequired extends FailureMetadata("UNAUTHORIZED", exceptional = true)
    case SingletonAdminForbidden extends FailureMetadata("FORBIDDEN", exceptional = true)
    case AdminBootstrapRequired extends FailureMetadata("ADMIN_BOOTSTRAP_REQUIRED", exceptional = false)
    case AdminAlreadyBootstrapped extends FailureMetadata("ADMIN_ALREADY_BOOTSTRAPPED", exceptional = false)
    case RegistrationFailed extends FailureMetadata("REGISTRATION_FAILED", exceptional = false)
    case InvalidCredentials extends FailureMetadata("INVALID_CREDENTIALS", exceptional = false)
    case DeletedAccountAuthenticationRequired extends FailureMetadata("UNAUTHORIZED", exceptional = false)
    case ProfileRoleMismatch extends FailureMetadata("PROFILE_ROLE_MISMATCH", exceptional = false)
    case ProfileUnsupportedForRole extends FailureMetadata("PROFILE_UNSUPPORTED_FOR_ROLE", exceptional = false)
    case InvalidPassword extends FailureMetadata("INVALID_PASSWORD", exceptional = false)
    case AccountAlreadyDeleted extends FailureMetadata("ACCOUNT_ALREADY_DELETED", exceptional = false)
    case AdminBootstrapOnly extends FailureMetadata("ADMIN_BOOTSTRAP_ONLY", exceptional = false)
    case ServiceNotReady extends FailureMetadata("SERVICE_NOT_READY", exceptional = true)
    case Forbidden extends FailureMetadata("FORBIDDEN", exceptional = true)
    case NotFound extends FailureMetadata("NOT_FOUND", exceptional = false)
    case DuplicateApplication extends FailureMetadata("DUPLICATE_APPLICATION", exceptional = false)
    case SearchSessionPending extends FailureMetadata("SEARCH_SESSION_PENDING", exceptional = false)
    case SearchSessionUnavailable extends FailureMetadata("SEARCH_SESSION_UNAVAILABLE", exceptional = false)
    case JobMustBeOpen extends FailureMetadata("JOB_MUST_BE_OPEN", exceptional = false)
    case CandidateRequired extends FailureMetadata("CANDIDATE_REQUIRED", exceptional = false)
    case RecruiterRequired extends FailureMetadata("RECRUITER_REQUIRED", exceptional = false)
    case InvalidJobTransition extends FailureMetadata("INVALID_JOB_TRANSITION", exceptional = false)
    case InvalidInitialJobStatus extends FailureMetadata("INVALID_INITIAL_JOB_STATUS", exceptional = false)
    case InvalidStatusTransition extends FailureMetadata("INVALID_STATUS_TRANSITION", exceptional = false)
    case RejectionFeedbackRequired extends FailureMetadata("REJECTION_FEEDBACK_REQUIRED", exceptional = false)
    case DeclineReasonRequired extends FailureMetadata("DECLINE_REASON_REQUIRED", exceptional = false)
    case Conflict extends FailureMetadata("CONFLICT", exceptional = false)
    case RepositoryUnavailable extends FailureMetadata("UNAVAILABLE", exceptional = true)
    case MissingEmbedding extends FailureMetadata("MISSING_EMBEDDING", exceptional = true)
    case StaleEmbedding extends FailureMetadata("STALE_EMBEDDING", exceptional = true)
    case InputTooLarge extends FailureMetadata("INPUT_TOO_LARGE", exceptional = true)
    case ProviderUnavailable extends FailureMetadata("PROVIDER_UNAVAILABLE", exceptional = true)
    case VectorSearchUnavailable extends FailureMetadata("VECTOR_SEARCH_UNAVAILABLE", exceptional = true)
    case ValidationFailed extends FailureMetadata("VALIDATION_FAILED", exceptional = false)
    case AnalyticsUnavailable extends FailureMetadata("ANALYTICS_UNAVAILABLE", exceptional = true)
    case AnalyticsInvalidPeriod extends FailureMetadata("INVALID_ANALYTICS_PERIOD", exceptional = false)
    case AnalyticsContextRequired extends FailureMetadata("ANALYTICS_CONTEXT_REQUIRED", exceptional = true)
  }

  def classify(error: UseCaseError): GraphQLFailure =
    error match {
      case UseCaseError.Authentication(value) => classifyAuthentication(value)
      case UseCaseError.Account(value) => classifyAccount(value)
      case UseCaseError.Availability(value) => classifyAvailability(value)
      case UseCaseError.Analytics(value) => classifyAnalytics(value)
      case UseCaseError.Domain(value) => classifyDomain(value)
      case UseCaseError.Repository(value) => classifyRepository(value)
      case UseCaseError.Search(value) => classifySearch(value)
      case UseCaseError.ValidationFailed(errors) => classifyValidation(errors)
    }

  private def classifyAuthentication(error: AuthenticationError): GraphQLFailure =
    error match {
      case AuthenticationError.Unauthorized => failure(FailureMetadata.AuthenticationRequired, "Authentication required")
      case AuthenticationError.SingletonAdminViolation => failure(FailureMetadata.SingletonAdminForbidden, "Forbidden")
    }

  private def classifyAccount(error: AccountError): GraphQLFailure =
    error match {
      case AccountError.BootstrapRequired => failure(FailureMetadata.AdminBootstrapRequired, "The first Admin must be bootstrapped")
      case AccountError.AlreadyBootstrapped => failure(FailureMetadata.AdminAlreadyBootstrapped, "Admin bootstrap is already complete")
      case AccountError.NameTaken => failure(FailureMetadata.RegistrationFailed, "Registration failed")
      case AccountError.InvalidCredentials => failure(FailureMetadata.InvalidCredentials, "Invalid credentials")
      case AccountError.DeletedAccount => failure(FailureMetadata.DeletedAccountAuthenticationRequired, "Authentication required")
      case AccountError.ProfileRoleMismatch => failure(FailureMetadata.ProfileRoleMismatch, "Profile does not match the selected role")
      case AccountError.ProfileUnsupportedForRole => failure(FailureMetadata.ProfileUnsupportedForRole, "This role does not support a profile")
      case AccountError.PasswordPolicyViolation => failure(FailureMetadata.InvalidPassword, "Password does not meet policy")
      case AccountError.AccountAlreadyDeleted => failure(FailureMetadata.AccountAlreadyDeleted, "Account is already deleted")
      case AccountError.AdminSignupForbidden => failure(FailureMetadata.AdminBootstrapOnly, "Admin accounts can only be created through bootstrap")
    }

  private def classifyAvailability(error: AvailabilityError): GraphQLFailure =
    error match {
      case AvailabilityError.ServiceNotReady => failure(FailureMetadata.ServiceNotReady, "Service not ready")
    }

  private def classifyAnalytics(error: AnalyticsError): GraphQLFailure =
    error match {
      case AnalyticsError.ReportsUnavailable => failure(FailureMetadata.AnalyticsUnavailable, "Analytics reports are unavailable")
      case AnalyticsError.InvalidPeriod => failure(FailureMetadata.AnalyticsInvalidPeriod, "Analytics period must be ordered and at most 30 days")
      case AnalyticsError.ErasureContextRequired => failure(FailureMetadata.AnalyticsContextRequired, "Account deletion is unavailable")
    }

  private def classifyDomain(error: DomainError): GraphQLFailure =
    error match {
      case DomainError.Forbidden => failure(FailureMetadata.Forbidden, "Forbidden")
      case DomainError.NotFound(entity) => failure(FailureMetadata.NotFound, s"$entity not found")
      case DomainError.DuplicateApplication => failure(FailureMetadata.DuplicateApplication, "Application already exists")
      case DomainError.SearchSessionPending => failure(FailureMetadata.SearchSessionPending, "Search session is being prepared; retry shortly")
      case DomainError.SearchSessionUnavailable => failure(FailureMetadata.SearchSessionUnavailable, "Search session is unavailable")
      case DomainError.JobMustBeOpen => failure(FailureMetadata.JobMustBeOpen, "Job must be open")
      case DomainError.CandidateRequired => failure(FailureMetadata.CandidateRequired, "Candidate role required")
      case DomainError.RecruiterRequired => failure(FailureMetadata.RecruiterRequired, "Recruiter role required")
      case DomainError.InvalidJobTransition(_, _) => failure(FailureMetadata.InvalidJobTransition, "Invalid job transition")
      case DomainError.InvalidInitialJobStatus(_) => failure(FailureMetadata.InvalidInitialJobStatus, "New jobs must be open")
      case DomainError.InvalidStatusTransition(_, _) => failure(FailureMetadata.InvalidStatusTransition, "Invalid application status transition")
      case DomainError.RejectionFeedbackRequired => failure(FailureMetadata.RejectionFeedbackRequired, "Rejection feedback is required")
      case DomainError.DeclineReasonRequired => failure(FailureMetadata.DeclineReasonRequired, "Decline reason is required")
    }

  private def classifyRepository(error: RepositoryError): GraphQLFailure =
    error match {
      case RepositoryError.DuplicateApplication => failure(FailureMetadata.DuplicateApplication, "Application already exists")
      case RepositoryError.Conflict => failure(FailureMetadata.Conflict, "Conflict")
      case RepositoryError.Unavailable => failure(FailureMetadata.RepositoryUnavailable, "Repository unavailable")
    }

  private def classifySearch(error: SearchError): GraphQLFailure =
    error match {
      case SearchError.MissingEmbedding(entity) => failure(FailureMetadata.MissingEmbedding, s"$entity embedding is missing")
      case SearchError.StaleEmbedding(entity) => failure(FailureMetadata.StaleEmbedding, s"$entity embedding is stale")
      case SearchError.InputTooLarge(field, maximum) => failure(FailureMetadata.InputTooLarge, s"$field must be at most $maximum characters")
      case SearchError.ProviderUnavailable => failure(FailureMetadata.ProviderUnavailable, "Embedding provider unavailable")
      case SearchError.VectorSearchUnavailable => failure(FailureMetadata.VectorSearchUnavailable, "Vector search unavailable")
    }

  private def classifyValidation(errors: NonEmptyList[DomainValidationError]): GraphQLFailure = {
    val fields = errors.toList.map(validationMessage).mkString(", ")
    failure(FailureMetadata.ValidationFailed, if (fields.isEmpty) "Validation failed" else fields)
  }

  private def validationMessage(error: DomainValidationError): String =
    error match {
      case DomainValidationError.BlankField(field) => s"$field is required"
      case DomainValidationError.EmptyCollection(field) => s"$field must not be empty"
      case DomainValidationError.InvalidNumber(field, minimum, maximum, _) => s"$field must be between $minimum and $maximum"
      case DomainValidationError.TextTooLong(field, maximum, _) => s"$field must be at most $maximum characters"
      case DomainValidationError.ByteLengthExceeded(field, maximum, _) => s"$field must be at most $maximum bytes"
      case DomainValidationError.TooManyValues(field, maximum, _) => s"$field must contain at most $maximum values"
    }

  private def failure(metadata: FailureMetadata, message: String): GraphQLFailure =
    GraphQLFailure(metadata.code, message, metadata.exceptional)
}
