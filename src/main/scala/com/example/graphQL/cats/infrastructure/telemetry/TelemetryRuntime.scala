package com.example.graphQL.cats.infrastructure.telemetry

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.shared.HiringHttpPaths
import org.http4s.{HttpApp, HttpRoutes, Request, Uri}
import org.http4s.otel4s.middleware.metrics.OtelMetrics
import org.http4s.otel4s.middleware.server.RouteClassifier
import org.http4s.otel4s.middleware.trace.PerRequestFilter
import org.http4s.otel4s.middleware.trace.redact.{PathRedactor, QueryRedactor}
import org.http4s.otel4s.middleware.trace.server.{ServerMiddleware, ServerSpanDataProvider}
import org.http4s.server.middleware.Metrics
import org.typelevel.otel4s.oteljava.OtelJava
import org.typelevel.otel4s.metrics.MeterProvider
import org.typelevel.otel4s.trace.Tracer
import org.typelevel.otel4s.trace.TracerProvider

/** The application boundary for HTTP traces and metrics.
  *
  * Tracing uses the OpenTelemetry SDK's standard environment configuration, so Mongo/HTTP startup does not depend on an
  * OTLP collector.
  */
final case class TelemetryRuntime(
    tracer: Tracer[IO],
    tracerProvider: TracerProvider[IO],
    meterProvider: MeterProvider[IO]
) {
  def instrument(routes: HttpRoutes[IO]): Resource[IO, HttpApp[IO]] = Resource.eval {
    given TracerProvider[IO] = tracerProvider
    given MeterProvider[IO] = meterProvider

    val pathRedactor = new PathRedactor with QueryRedactor {
      def redactPath(path: Uri.Path): Uri.Path =
        if (TelemetryRuntime.safePaths.contains(path.renderString)) path else Uri.Path.Root
      def redactQuery(query: org.http4s.Query): org.http4s.Query = org.http4s.Query.empty
    }
    val routeClassifier = RouteClassifier.of[IO] {
      case request if TelemetryRuntime.safePaths.contains(request.uri.path.renderString) =>
        request.uri.path.renderString
      case _ => "_unmatched"
    }
    val spanDataProvider = ServerSpanDataProvider
      .openTelemetry(pathRedactor)
      .withRouteClassifier(routeClassifier)

    for {
      metricsOps <- OtelMetrics.serverMetricsOps[IO]()
      serverMiddleware <- ServerMiddleware
        .builder[IO](spanDataProvider)
        .withPerRequestReversePropagationFilter(PerRequestFilter.alwaysEnabled)
        .build
    } yield {
      val metricsRoutes =
        Metrics(metricsOps, classifierF = request => Some(TelemetryRuntime.routeLabel(request)))(routes)
      serverMiddleware.wrapHttpApp(metricsRoutes.orNotFound)
    }
  }
}

object TelemetryRuntime {
  private val TracerName = "hiring-platform"
  private[telemetry] val safePaths = HiringHttpPaths.public

  private[telemetry] def routeLabel(request: Request[IO]): String =
    if (safePaths.contains(request.uri.path.renderString)) request.uri.path.renderString else "_unmatched"

  def resource: Resource[IO, TelemetryRuntime] =
    OtelJava.autoConfigured[IO]().flatMap { otel =>
      Resource.eval(
        otel.tracerProvider
          .get(TracerName)
          .map(tracer => TelemetryRuntime(tracer, otel.tracerProvider, otel.meterProvider))
      )
    }
}
