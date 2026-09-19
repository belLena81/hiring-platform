package com.example.graphQL.cats.api.http

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.effect.std.Semaphore
import scala.concurrent.duration.*

final class Admission private (
    semaphore: Semaphore[IO],
    permits: Int,
    closed: Deferred[IO, Unit],
    draining: Ref[IO, Boolean],
    drained: Deferred[IO, Unit]
) {
  val permit: Resource[IO, Boolean] = Resource.eval(closed.tryGet).flatMap {
    case Some(_) => Resource.pure(false)
    case None => semaphore.tryPermit.evalMap { acquired =>
      if (acquired) closed.tryGet.map(_.isEmpty) else IO.pure(false)
    }
  }

  def close(grace: FiniteDuration = 15.seconds): IO[Unit] =
    closed.complete(()).void *>
      draining.modify {
        case false =>
          val drain = semaphore.acquireN(permits.toLong)
            .timeoutTo(grace, IO.unit)
            .guarantee(drained.complete(()).void)
          true -> drain
        case true => true -> drained.get
      }.flatten

  def close: IO[Unit] = close(15.seconds)
}

object Admission {
  def create(permits: Int): IO[Admission] =
    for {
      semaphore <- Semaphore[IO](permits)
      closed <- Deferred[IO, Unit]
      draining <- Ref.of[IO, Boolean](false)
      drained <- Deferred[IO, Unit]
    } yield new Admission(semaphore, permits, closed, draining, drained)

  def resource(permits: Int): Resource[IO, Admission] =
    Resource.make(create(permits))(_.close())
}
