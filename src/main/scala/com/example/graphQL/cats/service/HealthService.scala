package com.example.graphQL.cats.service

import cats.Applicative
import cats.effect.Temporal
import cats.syntax.all.*
import cats.effect.syntax.all.*
import scala.concurrent.duration.*
import Diagnostics.*

private final case class ProbeOutcome(result: ProbeResult, failure: Map[LogField, String], elapsed: FiniteDuration)

final class HealthService[F[_]: Temporal](probe: DatabaseProbe[F], diagnostics: Diagnostics[F]) {
  def readiness(requestId: Option[String]): F[ProbeResult] =
    Applicative[F].unit
      .flatMap(_ => probe.check(requestId))
      .map(result => ProbeOutcome(result, Map.empty, Duration.Zero))
      .timeoutTo(
        2.seconds,
        Applicative[F].unit.map(_ =>
          ProbeOutcome(
            ProbeResult.Unavailable,
            LogFields.failure(
              new java.util.concurrent.TimeoutException()
            ) + (LogField.Reason -> FailureReason.ProbeTimeout.reason),
            2.seconds
          )
        )
      )
      .handleError(error =>
        ProbeOutcome(
          ProbeResult.Unavailable,
          LogFields.failure(error) + (LogField.Reason -> FailureReason.DatabaseError.reason),
          Duration.Zero
        )
      )
      .flatMap { outcome =>
        val result = outcome.result
        val failure = outcome.failure
        val elapsed = outcome.elapsed
        val fields = Map(
          LogField.DurationMs -> elapsed.toMillis.toString,
          LogField.Outcome -> LogOutcome.NotReady.wire,
          LogField.Reason ->
            (if (result == ProbeResult.AuthenticationFailed) FailureReason.AuthenticationFailed.reason
             else FailureReason.DatabaseUnavailable.reason)
        ) ++ failure
        val diagnostic = result match {
          case ProbeResult.Ready       => Applicative[F].unit
          case ProbeResult.Unavailable => diagnostics.emit(LogEvent.MongoUnavailable, requestId, fields = fields)
          case ProbeResult.AuthenticationFailed =>
            diagnostics.emit(LogEvent.MongoAuthFailed, requestId, fields = fields)
        }
        diagnostic.as(result)
      }
}
