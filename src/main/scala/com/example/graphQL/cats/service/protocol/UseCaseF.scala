package com.example.graphQL.cats.service.protocol

import cats.{Applicative, Functor}
import cats.data.EitherT
import com.example.graphQL.cats.service.port.RepositoryF
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.service.UseCaseError

/** A use-case result with its expected failure channel made explicit, over any effect `F`. */
type UseCaseF[F[_], A] = EitherT[F, UseCaseError, A]

object UseCaseF {

  /** Fails with `error` unless `condition` holds. */
  def ensure[F[_]: Applicative](condition: Boolean, error: => UseCaseError): UseCaseF[F, Unit] =
    EitherT.cond[F](condition, (), error)

  /** A repository lookup whose absence is the domain `NotFound(entity)` outcome. */
  def found[F[_]: Functor, A](value: RepositoryF[F, Option[A]], entity: String): UseCaseF[F, A] =
    repository(value).subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound(entity))))

  def repository[F[_]: Functor, A](value: RepositoryF[F, A]): UseCaseF[F, A] =
    value.leftMap {
      case RepositoryError.AuthorityRevoked =>
        UseCaseError.Domain(DomainError.Forbidden)
      case error => UseCaseError.Repository(error)
    }

  def repositoryFromFEither[F[_]: Functor, A](value: F[Either[RepositoryError, A]]): UseCaseF[F, A] =
    repository(RepositoryF.fromFEither(value))
}
