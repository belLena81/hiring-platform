package com.example.graphQL.cats.transport.http

import cats.effect.{IO, Ref, Resource}
import cats.effect.std.Semaphore

final class Admission private (semaphore: Semaphore[IO], open: Ref[IO, Boolean]) {
  val permit: Resource[IO, Boolean] = semaphore.tryPermit.evalMap { acquired =>
    if (acquired) open.get else IO.pure(false)
  }

  def close: IO[Unit] = open.set(false)
}

object Admission {
  def create(permits: Int): IO[Admission] =
    for {
      semaphore <- Semaphore[IO](permits)
      open <- Ref.of[IO, Boolean](true)
    } yield new Admission(semaphore, open)
}
