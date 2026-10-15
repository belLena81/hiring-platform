package com.example.graphQL.cats.service.port

import cats.{Applicative, Functor}
import cats.data.EitherT
import com.example.graphQL.cats.service.RepositoryError

/** An effectful port result with its expected failure channel made explicit, over any effect `F`. */
type RepositoryF[F[_], A] = EitherT[F, RepositoryError, A]

object RepositoryF {
  def fromFEither[F[_], A](value: F[Either[RepositoryError, A]]): RepositoryF[F, A] = EitherT(value)

  def fromEither[F[_]: Applicative, A](value: Either[RepositoryError, A]): RepositoryF[F, A] =
    EitherT.fromEither[F](value)

  def pure[F[_]: Applicative, A](value: A): RepositoryF[F, A] = EitherT.pure[F, RepositoryError](value)

  def raise[F[_]: Applicative, A](error: RepositoryError): RepositoryF[F, A] = EitherT.leftT[F, A](error)

  def lift[F[_]: Functor, A](value: F[A]): RepositoryF[F, A] = EitherT.liftF[F, RepositoryError, A](value)

  /** Helpers for call sites that name the effect and cannot infer it from an argument: `RepositoryF.at[F].pure(x)` with
    * a concrete `F`.
    */
  def at[F[_]]: At[F] = new At[F]

  final class At[F[_]] {
    def pure[A](value: A)(using Applicative[F]): RepositoryF[F, A] = RepositoryF.pure[F, A](value)

    def raise[A](error: RepositoryError)(using Applicative[F]): RepositoryF[F, A] = RepositoryF.raise[F, A](error)

    def fromEither[A](value: Either[RepositoryError, A])(using Applicative[F]): RepositoryF[F, A] =
      RepositoryF.fromEither[F, A](value)
  }
}
