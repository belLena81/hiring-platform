package com.example.graphQL.cats.service

import cats.data.NonEmptyList
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.{DomainError, DomainValidationError}

enum AuthenticationError {
  case Unauthorized
  case SingletonAdminViolation
}

enum AccountError {
  case BootstrapRequired
  case AlreadyBootstrapped
  case NameTaken
  case InvalidCredentials
  case DeletedAccount
  case ProfileRoleMismatch
  case PasswordPolicyViolation
  case AccountAlreadyDeleted
  case AdminSignupForbidden
}

enum SearchError {
  case MissingEmbedding(entity: String)
  case StaleEmbedding(entity: String)
  case InputTooLarge(field: String, maximum: Int)
  case ProviderUnavailable
  case VectorSearchUnavailable
}

enum AvailabilityError {
  case ServiceNotReady
}

enum RepositoryError {
  case DuplicateApplication
  case Conflict
  case Unavailable
}

type UseCaseError = DomainError | RepositoryError | AuthenticationError | AccountError | SearchError | AvailabilityError | NonEmptyList[DomainValidationError]

object UseCaseError {
  extension [E <: UseCaseError, A](value: Either[E, A]) def widenUseCase: Either[UseCaseError, A] =
    value.leftMap(error => error: UseCaseError)
}
