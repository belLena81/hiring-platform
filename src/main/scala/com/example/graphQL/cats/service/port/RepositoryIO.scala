package com.example.graphQL.cats.service.port

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.service.RepositoryError

/** An effectful port result with its expected failure channel made explicit. */
type RepositoryIO[A] = EitherT[IO, RepositoryError, A]

object RepositoryIO {
  def fromIOEither[A](value: IO[Either[RepositoryError, A]]): RepositoryIO[A] = EitherT(value)

  def fromEither[A](value: Either[RepositoryError, A]): RepositoryIO[A] = EitherT.fromEither[IO](value)

  def lift[A](value: IO[A]): RepositoryIO[A] = EitherT.liftF[IO, RepositoryError, A](value)
}
