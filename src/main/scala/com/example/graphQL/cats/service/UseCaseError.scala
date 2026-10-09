package com.example.graphQL.cats.service

import cats.data.{NonEmptyChain, NonEmptyList}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.{DomainError, DomainValidationError}
import com.example.graphQL.cats.service.RepositoryError

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
  case PasswordPolicyViolation
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
    val fields = errors.toNonEmptyList.collect { case InvalidFilter(field) => field }.distinct
    val others = errors.toNonEmptyList.filterNot(_.isInstanceOf[InvalidFilter])
    (NonEmptyList.fromList(fields), others) match {
      case (Some(NonEmptyList(field, Nil)), Nil) => InvalidFilter(field)
      case (Some(many), Nil)                     => InvalidFilters(many)
      case _                                     => errors.head
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

  trait Widen[-E] {
    def apply(error: E): UseCaseError
  }

  given Widen[UseCaseError] with {
    def apply(error: UseCaseError): UseCaseError = error
  }

  given Widen[DomainError] with {
    def apply(error: DomainError): UseCaseError = Domain(error)
  }

  given Widen[RepositoryError] with {
    def apply(error: RepositoryError): UseCaseError = Repository(error)
  }

  given Widen[AuthenticationError] with {
    def apply(error: AuthenticationError): UseCaseError = Authentication(error)
  }

  given Widen[AccountError] with {
    def apply(error: AccountError): UseCaseError = Account(error)
  }

  given Widen[SearchError] with {
    def apply(error: SearchError): UseCaseError = Search(error)
  }

  given Widen[AvailabilityError] with {
    def apply(error: AvailabilityError): UseCaseError = Availability(error)
  }

  given Widen[AnalyticsError] with {
    def apply(error: AnalyticsError): UseCaseError = Analytics(error)
  }

  given Widen[NonEmptyList[DomainValidationError]] with {
    def apply(error: NonEmptyList[DomainValidationError]): UseCaseError = ValidationFailed(error)
  }

  extension [E, A](value: Either[E, A])(using widen: Widen[E])
    def widenUseCase: Either[UseCaseError, A] = value.leftMap(widen.apply)

}
