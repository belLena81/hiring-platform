package com.example.graphQL.cats.application

import cats.effect.IO
import scala.concurrent.duration.*

final class HealthService(probe: DatabaseProbe, diagnostics: Diagnostics) {
  def readiness(requestId: Option[String]): IO[ProbeResult] =
    IO.defer(probe.check(requestId)).map(result => (result, Map.empty[LogField, String]))
      .timeoutTo(2.seconds, IO.delay((ProbeResult.Unavailable,
        LogFields.failure(new java.util.concurrent.TimeoutException()) + (LogField.Reason -> "PROBE_TIMEOUT"))))
      .handleError(error => (ProbeResult.Unavailable, LogFields.failure(error) + (LogField.Reason -> "DATABASE_ERROR")))
      .timed.flatMap { case (elapsed, (result, failure)) =>
        val fields = Map(LogField.DurationMs -> elapsed.toMillis.toString,
          LogField.Outcome -> "NOT_READY", LogField.Reason ->
            (if (result == ProbeResult.AuthenticationFailed) "AUTHENTICATION_FAILED" else "DATABASE_UNAVAILABLE")) ++ failure
        val diagnostic = result match {
          case ProbeResult.Ready => IO.unit
          case ProbeResult.Unavailable => Diagnostics.emit(diagnostics, LogEvent.MongoUnavailable, requestId, fields)
          case ProbeResult.AuthenticationFailed => Diagnostics.emit(diagnostics, LogEvent.MongoAuthFailed, requestId, fields)
        }
        diagnostic.as(result)
      }
}
