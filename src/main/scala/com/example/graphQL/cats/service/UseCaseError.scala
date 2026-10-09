package com.example.graphQL.cats.service

import cats.data.{NonEmptyChain, NonEmptyList}
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
  case ProfileUnsupportedForRole
  case AccountAlreadyDeleted
  case AdminSignupForbidden
}

enum SearchError {
  case MissingEmbedding(entity: String)
  case StaleEmbedding(entity: String)
  case InputTooLarge(field: String, maximum: Int)
  case InvalidFilter(field: String)
  case InvalidFilters(fields: NonEmptyList[String])
  case ProviderUnavailable
  case VectorSearchUnavailable
}

object SearchError {

  /** Reports every violated filter field: one stays `InvalidFilter`, several become `InvalidFilters`. */
  def accumulated(errors: NonEmptyChain[SearchError]): SearchError = {
    val (others, fields) = errors.toNonEmptyList.toList.partitionMap {
      case InvalidFilter(field) => Right(field)
      case other                => Left(other)
    }
    (others, fields.distinct) match {
      case (Nil, List(field))  => InvalidFilter(field)
      case (Nil, head :: tail) => InvalidFilters(NonEmptyList(head, tail))
      case _                   => errors.head
    }
  }
}

enum AvailabilityError {
  case ServiceNotReady
}

enum AnalyticsError {
  case ErasureContextRequired
  case ErasureWorkerUnavailable
  case ErasureNotCompleted
  case ReportsUnavailable
  case InvalidPeriod
}

sealed trait UseCaseError

object UseCaseError {
  final case class Domain(error: DomainError) extends UseCaseError
  final case class Repository(error: RepositoryError) extends UseCaseError
  final case class Authentication(error: AuthenticationError) extends UseCaseError
  final case class Account(error: AccountError) extends UseCaseError
  final case class Search(error: SearchError) extends UseCaseError
  final case class Availability(error: AvailabilityError) extends UseCaseError
  final case class Analytics(error: AnalyticsError) extends UseCaseError
  final case class ValidationFailed(errors: NonEmptyList[DomainValidationError]) extends UseCaseError
}
