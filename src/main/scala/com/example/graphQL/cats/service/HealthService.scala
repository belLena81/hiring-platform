package com.example.graphQL.cats.service

import cats.effect.IO
import scala.concurrent.duration.*

private final case class ProbeOutcome(result: ProbeResult, failure: Map[LogField, String], elapsed: FiniteDuration)

final class HealthService(probe: DatabaseProbe, diagnostics: Diagnostics) {
  def readiness(requestId: Option[String]): IO[ProbeResult] =
    IO.defer(probe.check(requestId)).map(result => ProbeOutcome(result, Map.empty, Duration.Zero))
      .timeoutTo(2.seconds, IO.delay(ProbeOutcome(ProbeResult.Unavailable,
        LogFields.failure(new java.util.concurrent.TimeoutException()) + (LogField.Reason -> Rejection.ProbeTimeout.reason), 2.seconds)))
      .handleError(error => ProbeOutcome(ProbeResult.Unavailable,
        LogFields.failure(error) + (LogField.Reason -> Rejection.DatabaseError.reason), Duration.Zero))
      .timed.map { case (elapsed, outcome) => outcome.copy(elapsed = elapsed) }
      .flatMap { outcome =>
        val result = outcome.result
        val failure = outcome.failure
        val elapsed = outcome.elapsed
        val fields = Map(LogField.DurationMs -> elapsed.toMillis.toString,
          LogField.Outcome -> "NOT_READY", LogField.Reason ->
            (if (result == ProbeResult.AuthenticationFailed) Rejection.AuthenticationFailed.reason else Rejection.DatabaseUnavailable.reason)) ++ failure
        val diagnostic = result match {
          case ProbeResult.Ready => IO.unit
          case ProbeResult.Unavailable => diagnostics.emit(LogEvent.MongoUnavailable, requestId, fields = fields)
          case ProbeResult.AuthenticationFailed => diagnostics.emit(LogEvent.MongoAuthFailed, requestId, fields = fields)
        }
        diagnostic.as(result)
      }
}
