package com.example.graphQL.cats.api.http

import cats.effect.{Deferred, IO, Resource}
import cats.effect.std.Semaphore

final class Admission private (semaphore: Semaphore[IO], permits: Int, closed: Deferred[IO, Unit]) {
  val permit: Resource[IO, Boolean] = Resource.eval(closed.tryGet).flatMap {
    case Some(_) => Resource.pure(false)
    case None => semaphore.tryPermit.evalMap { acquired =>
      if (acquired) closed.tryGet.map(_.isEmpty) else IO.pure(false)
    }
  }

  def close: IO[Unit] = closed.complete(()).attempt.void *> semaphore.acquireN(permits.toLong)
}

object Admission {
  def create(permits: Int): IO[Admission] =
    for {
      semaphore <- Semaphore[IO](permits)
      closed <- Deferred[IO, Unit]
    } yield new Admission(semaphore, permits, closed)

  def resource(permits: Int): Resource[IO, Admission] =
    Resource.make(create(permits))(_.close)
}
