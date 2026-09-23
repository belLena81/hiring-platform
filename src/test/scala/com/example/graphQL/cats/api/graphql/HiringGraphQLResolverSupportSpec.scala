package com.example.graphQL.cats.api.graphql

import cats.data.NonEmptyList
import cats.effect.IO
import com.example.graphQL.cats.api.graphql.HiringGraphQLModel.{DomainError, GraphQLFailure}
import com.example.graphQL.cats.domain.error.{DomainError as DomainFailure, DomainValidationError}
import com.example.graphQL.cats.domain.model.{ApplicationStatus, JobStatus}
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.service.{
  AccountError,
  AuthenticationError,
  AvailabilityError,
  SearchError,
  UseCaseError
}
import com.example.graphQL.cats.service.protocol.UseCaseIO
import munit.CatsEffectSuite

final class HiringGraphQLResolverSupportSpec extends CatsEffectSuite {
  test("maps every use-case error through the failure catalog") {
    final case class Scenario(name: String, error: UseCaseError, expected: GraphQLFailure)

    val scenarios = List(
      Scenario(
        "authentication required",
        UseCaseError.Authentication(AuthenticationError.Unauthorized),
        GraphQLFailure("UNAUTHORIZED", "Authentication required", exceptional = true)
      ),
      Scenario(
        "singleton Admin violation",
        UseCaseError.Authentication(AuthenticationError.SingletonAdminViolation),
        GraphQLFailure("FORBIDDEN", "Forbidden", exceptional = true)
      ),
      Scenario(
        "bootstrap required",
        UseCaseError.Account(AccountError.BootstrapRequired),
        GraphQLFailure("ADMIN_BOOTSTRAP_REQUIRED", "The first Admin must be bootstrapped", exceptional = false)
      ),
      Scenario(
        "already bootstrapped",
        UseCaseError.Account(AccountError.AlreadyBootstrapped),
        GraphQLFailure("ADMIN_ALREADY_BOOTSTRAPPED", "Admin bootstrap is already complete", exceptional = false)
      ),
      Scenario(
        "name taken",
        UseCaseError.Account(AccountError.NameTaken),
        GraphQLFailure("REGISTRATION_FAILED", "Registration failed", exceptional = false)
      ),
      Scenario(
        "invalid credentials",
        UseCaseError.Account(AccountError.InvalidCredentials),
        GraphQLFailure("INVALID_CREDENTIALS", "Invalid credentials", exceptional = false)
      ),
      Scenario(
        "deleted account",
        UseCaseError.Account(AccountError.DeletedAccount),
        GraphQLFailure("UNAUTHORIZED", "Authentication required", exceptional = false)
      ),
      Scenario(
        "profile role mismatch",
        UseCaseError.Account(AccountError.ProfileRoleMismatch),
        GraphQLFailure("PROFILE_ROLE_MISMATCH", "Profile does not match the selected role", exceptional = false)
      ),
      Scenario(
        "unsupported profile",
        UseCaseError.Account(AccountError.ProfileUnsupportedForRole),
        GraphQLFailure("PROFILE_UNSUPPORTED_FOR_ROLE", "This role does not support a profile", exceptional = false)
      ),
      Scenario(
        "password policy",
        UseCaseError.Account(AccountError.PasswordPolicyViolation),
        GraphQLFailure("INVALID_PASSWORD", "Password does not meet policy", exceptional = false)
      ),
      Scenario(
        "account already deleted",
        UseCaseError.Account(AccountError.AccountAlreadyDeleted),
        GraphQLFailure("ACCOUNT_ALREADY_DELETED", "Account is already deleted", exceptional = false)
      ),
      Scenario(
        "Admin signup",
        UseCaseError.Account(AccountError.AdminSignupForbidden),
        GraphQLFailure(
          "ADMIN_BOOTSTRAP_ONLY",
          "Admin accounts can only be created through bootstrap",
          exceptional = false
        )
      ),
      Scenario(
        "service unavailable",
        UseCaseError.Availability(AvailabilityError.ServiceNotReady),
        GraphQLFailure("SERVICE_NOT_READY", "Service not ready", exceptional = true)
      ),
      Scenario(
        "forbidden",
        UseCaseError.Domain(DomainFailure.Forbidden),
        GraphQLFailure("FORBIDDEN", "Forbidden", exceptional = true)
      ),
      Scenario(
        "dynamic not found",
        UseCaseError.Domain(DomainFailure.NotFound("candidate")),
        GraphQLFailure("NOT_FOUND", "candidate not found", exceptional = false)
      ),
      Scenario(
        "domain duplicate application",
        UseCaseError.Domain(DomainFailure.DuplicateApplication),
        GraphQLFailure("DUPLICATE_APPLICATION", "Application already exists", exceptional = false)
      ),
      Scenario(
        "search pending",
        UseCaseError.Domain(DomainFailure.SearchSessionPending),
        GraphQLFailure("SEARCH_SESSION_PENDING", "Search session is being prepared; retry shortly", exceptional = false)
      ),
      Scenario(
        "search unavailable",
        UseCaseError.Domain(DomainFailure.SearchSessionUnavailable),
        GraphQLFailure("SEARCH_SESSION_UNAVAILABLE", "Search session is unavailable", exceptional = false)
      ),
      Scenario(
        "job closed",
        UseCaseError.Domain(DomainFailure.JobMustBeOpen),
        GraphQLFailure("JOB_MUST_BE_OPEN", "Job must be open", exceptional = false)
      ),
      Scenario(
        "candidate required",
        UseCaseError.Domain(DomainFailure.CandidateRequired),
        GraphQLFailure("CANDIDATE_REQUIRED", "Candidate role required", exceptional = false)
      ),
      Scenario(
        "recruiter required",
        UseCaseError.Domain(DomainFailure.RecruiterRequired),
        GraphQLFailure("RECRUITER_REQUIRED", "Recruiter role required", exceptional = false)
      ),
      Scenario(
        "job transition",
        UseCaseError.Domain(DomainFailure.InvalidJobTransition(JobStatus.Open, JobStatus.Closed)),
        GraphQLFailure("INVALID_JOB_TRANSITION", "Invalid job transition", exceptional = false)
      ),
      Scenario(
        "initial job status",
        UseCaseError.Domain(DomainFailure.InvalidInitialJobStatus(JobStatus.Draft)),
        GraphQLFailure("INVALID_INITIAL_JOB_STATUS", "New jobs must be open", exceptional = false)
      ),
      Scenario(
        "application transition",
        UseCaseError.Domain(DomainFailure.InvalidStatusTransition(ApplicationStatus.Created, ApplicationStatus.Hired)),
        GraphQLFailure("INVALID_STATUS_TRANSITION", "Invalid application status transition", exceptional = false)
      ),
      Scenario(
        "rejection feedback",
        UseCaseError.Domain(DomainFailure.RejectionFeedbackRequired),
        GraphQLFailure("REJECTION_FEEDBACK_REQUIRED", "Rejection feedback is required", exceptional = false)
      ),
      Scenario(
        "decline reason",
        UseCaseError.Domain(DomainFailure.DeclineReasonRequired),
        GraphQLFailure("DECLINE_REASON_REQUIRED", "Decline reason is required", exceptional = false)
      ),
      Scenario(
        "repository duplicate application",
        UseCaseError.Repository(RepositoryError.DuplicateApplication),
        GraphQLFailure("DUPLICATE_APPLICATION", "Application already exists", exceptional = false)
      ),
      Scenario(
        "repository conflict",
        UseCaseError.Repository(RepositoryError.Conflict),
        GraphQLFailure("CONFLICT", "Conflict", exceptional = false)
      ),
      Scenario(
        "repository unavailable",
        UseCaseError.Repository(RepositoryError.Unavailable),
        GraphQLFailure("UNAVAILABLE", "Repository unavailable", exceptional = true)
      ),
      Scenario(
        "missing embedding",
        UseCaseError.Search(SearchError.MissingEmbedding("job")),
        GraphQLFailure("MISSING_EMBEDDING", "job embedding is missing", exceptional = true)
      ),
      Scenario(
        "stale embedding",
        UseCaseError.Search(SearchError.StaleEmbedding("candidate")),
        GraphQLFailure("STALE_EMBEDDING", "candidate embedding is stale", exceptional = true)
      ),
      Scenario(
        "large search input",
        UseCaseError.Search(SearchError.InputTooLarge("description", 2000)),
        GraphQLFailure("INPUT_TOO_LARGE", "description must be at most 2000 characters", exceptional = true)
      ),
      Scenario(
        "provider unavailable",
        UseCaseError.Search(SearchError.ProviderUnavailable),
        GraphQLFailure("PROVIDER_UNAVAILABLE", "Embedding provider unavailable", exceptional = true)
      ),
      Scenario(
        "vector search unavailable",
        UseCaseError.Search(SearchError.VectorSearchUnavailable),
        GraphQLFailure("VECTOR_SEARCH_UNAVAILABLE", "Vector search unavailable", exceptional = true)
      ),
      Scenario(
        "blank field",
        UseCaseError.ValidationFailed(NonEmptyList.one(DomainValidationError.BlankField("name"))),
        GraphQLFailure("VALIDATION_FAILED", "name is required", exceptional = false)
      ),
      Scenario(
        "empty collection",
        UseCaseError.ValidationFailed(NonEmptyList.one(DomainValidationError.EmptyCollection("skills"))),
        GraphQLFailure("VALIDATION_FAILED", "skills must not be empty", exceptional = false)
      ),
      Scenario(
        "invalid number",
        UseCaseError.ValidationFailed(NonEmptyList.one(DomainValidationError.InvalidNumber("first", 1, 100, 101))),
        GraphQLFailure("VALIDATION_FAILED", "first must be between 1 and 100", exceptional = false)
      ),
      Scenario(
        "text too long",
        UseCaseError.ValidationFailed(NonEmptyList.one(DomainValidationError.TextTooLong("summary", 500, 501))),
        GraphQLFailure("VALIDATION_FAILED", "summary must be at most 500 characters", exceptional = false)
      ),
      Scenario(
        "byte length",
        UseCaseError.ValidationFailed(
          NonEmptyList.one(DomainValidationError.ByteLengthExceeded("description", 1000, 1001))
        ),
        GraphQLFailure("VALIDATION_FAILED", "description must be at most 1000 bytes", exceptional = false)
      ),
      Scenario(
        "too many values",
        UseCaseError.ValidationFailed(NonEmptyList.one(DomainValidationError.TooManyValues("skills", 20, 21))),
        GraphQLFailure("VALIDATION_FAILED", "skills must contain at most 20 values", exceptional = false)
      ),
      Scenario(
        "multiple validation errors",
        UseCaseError.ValidationFailed(
          NonEmptyList.of(DomainValidationError.BlankField("name"), DomainValidationError.EmptyCollection("skills"))
        ),
        GraphQLFailure("VALIDATION_FAILED", "name is required, skills must not be empty", exceptional = false)
      )
    )

    IO {
      scenarios.foreach { scenario =>
        assertEquals(HiringGraphQLResolverSupport.toGraphQLFailure(scenario.error), scenario.expected, scenario.name)
      }
    }
  }

  test("keeps authentication and deleted-account UNAUTHORIZED classifications distinct") {
    val unauthenticated =
      HiringGraphQLResolverSupport.toGraphQLFailure(UseCaseError.Authentication(AuthenticationError.Unauthorized))
    val deletedAccount =
      HiringGraphQLResolverSupport.toGraphQLFailure(UseCaseError.Account(AccountError.DeletedAccount))

    IO {
      assertEquals(unauthenticated.code, deletedAccount.code)
      assert(unauthenticated.exceptional)
      assert(!deletedAccount.exceptional)
    }
  }

  test("turns expected mutation failures into typed domain errors") {
    for {
      account <- HiringGraphQLResolverSupport.mutationResult(
        UseCaseIO.left[String](UseCaseError.Account(AccountError.NameTaken))
      )
      repository <- HiringGraphQLResolverSupport.mutationResult(
        UseCaseIO.left[String](UseCaseError.Repository(RepositoryError.Conflict))
      )
    } yield {
      assertEquals(account, DomainError("REGISTRATION_FAILED", "Registration failed"))
      assertEquals(repository, DomainError("CONFLICT", "Conflict"))
    }
  }

  test("raises exceptional mutation failures as read failures") {
    val error = UseCaseError.Repository(RepositoryError.Unavailable)
    HiringGraphQLResolverSupport.mutationResult(UseCaseIO.left[String](error)).attempt.map {
      case Left(RequestContext.ReadFailure(actual)) => assertEquals(actual, error)
      case result                                   => fail(s"Expected a read failure, received $result")
    }
  }
}
