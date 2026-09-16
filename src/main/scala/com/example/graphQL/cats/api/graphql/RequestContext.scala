package com.example.graphQL.cats.api.graphql

import cats.effect.{IO, Resource}
import cats.effect.std.Dispatcher
import com.example.graphQL.cats.application.ProbeResult
import scala.concurrent.Future

final class RequestContext private (dispatcher: Dispatcher[IO], probe: IO[ProbeResult]) {
  def readiness: Future[ProbeResult] = dispatcher.unsafeToFuture(probe)
}

object RequestContext {
  def resource(probe: IO[ProbeResult]): Resource[IO, RequestContext] =
    for {
      dispatcher <- Dispatcher.parallel[IO](await = false)
      memoized <- Resource.eval(probe.memoize)
    } yield new RequestContext(dispatcher, memoized)
}
