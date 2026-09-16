package com.example.graphQL.cats.application

import cats.data.NonEmptyList
import com.example.graphQL.cats.application.port.RepositoryError
import com.example.graphQL.cats.domain.error.{DomainError, DomainValidationError}

enum AuthenticationError {
  case Unauthorized
  case SingletonAdminViolation
}

type UseCaseError = DomainError | RepositoryError | AuthenticationError | NonEmptyList[DomainValidationError]
