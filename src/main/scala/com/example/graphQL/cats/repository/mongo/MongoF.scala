package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.port.RepositoryF

/** The repository result at `IO`. Naming the effect once removes the `RepositoryF.at[IO]` and
  * `pure[[A] =>> RepositoryF[IO, A]]` type-inference workarounds from the Mongo adapters.
  */
type MongoF[A] = RepositoryF[IO, A]

object MongoF {
  def pure[A](value: A): MongoF[A] = RepositoryF.pure[IO, A](value)

  def raise[A](error: RepositoryError): MongoF[A] = RepositoryF.raise[IO, A](error)

  def fromEither[A](value: Either[RepositoryError, A]): MongoF[A] = RepositoryF.fromEither[IO, A](value)

  extension [A](value: A) {

    /** Lifts an already computed value into the repository result. */
    def asRepository: MongoF[A] = pure(value)
  }
}
