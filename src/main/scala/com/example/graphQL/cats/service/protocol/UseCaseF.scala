package com.example.graphQL.cats.service.protocol

import cats.{Applicative, Functor}
import cats.data.EitherT
import com.example.graphQL.cats.service.port.RepositoryF
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.service.{SearchError, UseCaseError}

/** A use-case result with its expected failure channel made explicit, over any effect `F`. */
type UseCaseF[F[_], A] = EitherT[F, UseCaseError, A]

object UseCaseF {

  /** Fails with `error` unless `condition` holds. */
  def ensure[F[_]: Applicative](condition: Boolean, error: => UseCaseError): UseCaseF[F, Unit] =
    EitherT.cond[F](condition, (), error)

  /** Fails with the `Forbidden` domain outcome unless `condition` holds. */
  def forbidUnless[F[_]: Applicative](condition: Boolean): UseCaseF[F, Unit] =
    ensure[F](condition, UseCaseError.Domain(DomainError.Forbidden))

  /** A repository lookup whose absence is the domain `NotFound(entity)` outcome. */
  def found[F[_]: Functor, A](value: RepositoryF[F, Option[A]], entity: String): UseCaseF[F, A] =
    repository(value).subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound(entity))))

  /** The one translation of a repository failure into a use-case failure; it is total and every layer above reads its
    * result instead of matching `RepositoryError` again. A revoked authority is the `Forbidden` outcome; a vector
    * branch that failed next to an answering lexical branch is the search-specific `VectorSearchUnavailable`; every
    * other repository failure is kept as `Repository(error)` and classified at the API boundary.
    */
  def translate(error: RepositoryError): UseCaseError =
    error match {
      case RepositoryError.AuthorityRevoked        => UseCaseError.Domain(DomainError.Forbidden)
      case RepositoryError.VectorSearchUnavailable => UseCaseError.Search(SearchError.VectorSearchUnavailable)
      case RepositoryError.DuplicateApplication | RepositoryError.Conflict | RepositoryError.InvalidEvent |
          RepositoryError.InvalidStoredData | RepositoryError.MissingStoredResult | RepositoryError.Unavailable |
          RepositoryError.Overloaded =>
        UseCaseError.Repository(error)
    }

  def repository[F[_]: Functor, A](value: RepositoryF[F, A]): UseCaseF[F, A] =
    value.leftMap(translate)

  def repositoryFromFEither[F[_]: Functor, A](value: F[Either[RepositoryError, A]]): UseCaseF[F, A] =
    repository(RepositoryF.fromFEither(value))
}
