package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Resource}
import cats.effect.std.Dispatcher
import com.example.graphQL.cats.application.ProbeResult
import scala.concurrent.Future

final class RequestContext private (dispatcher: Dispatcher[IO], probe: IO[ProbeResult]) {
  private var open = true

  def readiness: Future[ProbeResult] = synchronized {
    if (!open) throw new IllegalStateException("Request context is closed")
    dispatcher.unsafeToFuture(probe)
  }

  private def close: IO[Unit] = IO.delay(synchronized { open = false })
}

object RequestContext {
  def resource(probe: IO[ProbeResult]): Resource[IO, RequestContext] =
    for {
      dispatcher <- Dispatcher.parallel[IO](await = false)
      memoized <- Resource.eval(probe.memoize)
      context <- Resource.make(IO(new RequestContext(dispatcher, memoized)))(_.close)
    } yield context
}
