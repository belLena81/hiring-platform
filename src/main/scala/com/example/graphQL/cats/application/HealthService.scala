package com.example.graphQL.cats.application

import cats.effect.IO
import scala.concurrent.duration.*

final class HealthService(probe: DatabaseProbe, diagnostics: Diagnostics) {
  def readiness(requestId: Option[String]): IO[ProbeResult] =
    probe.check.timeoutTo(2.seconds, IO.pure(ProbeResult.Unavailable))
      .handleError(_ => ProbeResult.Unavailable)
      .flatTap {
        case ProbeResult.Ready                => IO.unit
        case ProbeResult.Unavailable          => diagnostics.event(LogEvent.MongoUnavailable, requestId)
        case ProbeResult.AuthenticationFailed => diagnostics.event(LogEvent.MongoAuthFailed, requestId)
      }
}
