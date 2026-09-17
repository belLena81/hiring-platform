package com.example.graphQL.cats.application

import cats.data.NonEmptyList
import cats.syntax.all.*
import com.example.graphQL.cats.application.port.RepositoryError
import com.example.graphQL.cats.domain.error.{DomainError, DomainValidationError}

enum AuthenticationError {
  case Unauthorized
  case SingletonAdminViolation
}

enum SearchError {
  case MissingEmbedding(entity: String)
  case StaleEmbedding(entity: String)
  case InputTooLarge(field: String, maximum: Int)
  case ProviderUnavailable
  case VectorSearchUnavailable
}

type UseCaseError = DomainError | RepositoryError | AuthenticationError | SearchError | NonEmptyList[DomainValidationError]

object UseCaseError {
  extension [E <: UseCaseError, A](value: Either[E, A]) def widenUseCase: Either[UseCaseError, A] =
    value.leftMap(error => error: UseCaseError)
}
