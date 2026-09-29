package com.example.graphQL.cats.repository.protocol

import cats.data.EitherT
import cats.effect.IO

/** Failures an operational repository can report without exposing driver details. */
enum RepositoryError {
  case DuplicateApplication
  case Conflict
  case InvalidStoredData
  case MissingWriteResult
  case Unavailable
}

/** An effectful repository result with its expected failure channel made explicit. */
type RepositoryIO[A] = EitherT[IO, RepositoryError, A]

object RepositoryIO {
  def fromIOEither[A](value: IO[Either[RepositoryError, A]]): RepositoryIO[A] = EitherT(value)

  def fromEither[A](value: Either[RepositoryError, A]): RepositoryIO[A] = EitherT.fromEither[IO](value)

  def lift[A](value: IO[A]): RepositoryIO[A] = EitherT.liftF[IO, RepositoryError, A](value)
}
