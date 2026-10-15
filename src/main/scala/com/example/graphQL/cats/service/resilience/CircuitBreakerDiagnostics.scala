package com.example.graphQL.cats.service.resilience

import cats.MonadThrow
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy.Transition
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField}
import com.example.graphQL.cats.service.Diagnostics.*

/** Reports breaker events with the breaker name only; no request, dependency or configuration data. */
object CircuitBreakerDiagnostics {
  def listener[F[_]: MonadThrow](name: CircuitBreakerName, diagnostics: Diagnostics[F]): Transition => F[Unit] =
    transition =>
      diagnostics.emit(
        LogEvent.CircuitBreakerStateChanged,
        fields = Map(LogField.Breaker -> name.wire, LogField.BreakerTransition -> transition.wire)
      )

  def disabled[F[_]: MonadThrow](name: CircuitBreakerName, diagnostics: Diagnostics[F]): F[Unit] =
    diagnostics.emit(LogEvent.CircuitBreakerDisabled, fields = Map(LogField.Breaker -> name.wire))
}
