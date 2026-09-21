package com.example.graphQL.cats.infrastructure.telemetry

import cats.effect.{IO, Resource}
import org.typelevel.log4cats.StructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jFactory
import org.typelevel.otel4s.oteljava.OtelJava
import org.typelevel.otel4s.trace.Tracer

/** The single application boundary for logs and traces.
  *
  * Logs are always available through log4cats. Tracing is opt-in so Mongo/HTTP
  * startup does not depend on an OTLP collector; when enabled, otel4s uses the
  * OpenTelemetry SDK and its standard environment configuration.
  */
final case class TelemetryRuntime(logger: StructuredLogger[IO], tracer: Tracer[IO])

object TelemetryRuntime {
  private val LoggerName = "hiring.foundation"
  private val TracerName = "hiring-platform"

  def resource(tracingEnabled: Boolean): Resource[IO, TelemetryRuntime] =
    Resource.eval(Slf4jFactory.create[IO].fromName(LoggerName)).flatMap { logger =>
      if (!tracingEnabled) Resource.eval(IO.pure(TelemetryRuntime(logger, Tracer.noop[IO])))
      else OtelJava.autoConfigured[IO]().flatMap { otel =>
        Resource.eval(otel.tracerProvider.get(TracerName).map(TelemetryRuntime(logger, _)))
      }
    }
}
