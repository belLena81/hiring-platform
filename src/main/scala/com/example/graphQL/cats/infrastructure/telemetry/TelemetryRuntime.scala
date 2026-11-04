package com.example.graphQL.cats.infrastructure.telemetry

import cats.effect.unsafe.metrics.IORuntimeMetrics
import cats.effect.{IO, IOLocal, Resource}
import com.example.graphQL.cats.shared.HiringHttpPaths
import org.http4s.{HttpApp, HttpRoutes, Query, Request, Uri}
import org.http4s.otel4s.middleware.metrics.OtelMetrics
import org.http4s.otel4s.middleware.server.RouteClassifier
import org.http4s.otel4s.middleware.trace.PerRequestFilter
import org.http4s.otel4s.middleware.trace.redact.{PathRedactor, QueryRedactor}
import org.http4s.otel4s.middleware.trace.server.{ServerMiddleware, ServerSpanDataProvider}
import org.http4s.server.middleware.Metrics
import org.typelevel.otel4s.context.LocalProvider
import org.typelevel.otel4s.oteljava.OtelJava
import org.typelevel.otel4s.oteljava.context.{Context, IOLocalContextStorage, LocalContextProvider}
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
    meterProvider: MeterProvider[IO],
    openTelemetry: io.opentelemetry.api.OpenTelemetry
) {
  def instrument(routes: HttpRoutes[IO]): Resource[IO, HttpApp[IO]] = Resource.eval {
    given TracerProvider[IO] = tracerProvider
    given MeterProvider[IO] = meterProvider

    val pathRedactor = new PathRedactor with QueryRedactor {
      def redactPath(path: Uri.Path): Uri.Path =
        if (TelemetryRuntime.isSafe(path)) path else Uri.Path.Root
      def redactQuery(query: Query): Query = Query.empty
    }
    val routeClassifier = RouteClassifier.of[IO] { case request => TelemetryRuntime.routeLabel(request) }
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

  private[telemetry] def isSafe(path: Uri.Path): Boolean = HiringHttpPaths.public.contains(path.renderString)

  private[telemetry] def routeLabel(request: Request[IO]): String =
    if (isSafe(request.uri.path)) request.uri.path.renderString else "_unmatched"

  private val ContextStorageProperty = "io.opentelemetry.context.contextStorageProvider"

  /** Must run before the first OpenTelemetry use in the process. `otel4s-oteljava-context-storage` registers a
    * fiber-backed Java context storage by service loader; without `-Dcats.effect.trackFiberContext=true` that storage
    * throws on every `Context.current()` and `makeCurrent()`, which breaks otel4s instruments and the SDK. So when
    * fiber tracking is off the standard thread-local storage is selected explicitly (an operator's own setting is
    * kept).
    */
  def selectContextStorage: IO[Unit] =
    IO.delay {
      if (!IOLocal.isPropagating && Option(System.getProperty(ContextStorageProperty)).isEmpty)
        System.setProperty(ContextStorageProperty, "default"): Unit
    }

  /** The fiber-following context provider when `-Dcats.effect.trackFiberContext=true` is set; otherwise otel4s's own
    * `IOLocal` provider. With the first, `Context.current()` on a thread running a fiber is that fiber's context, which
    * is how the MongoDB driver adapter finds the request span for its client spans. Without the flag everything else
    * works as before and Mongo commands are recorded as metrics only (no spans).
    */
  private[telemetry] def contextProvider: IO[LocalContextProvider[IO]] = {
    val following: LocalContextProvider[IO] = IOLocalContextStorage.localProvider[IO]
    following.local.as(following).handleError(_ => LocalProvider[IO, Context])
  }

  def resource(runtimeMetrics: IORuntimeMetrics): Resource[IO, TelemetryRuntime] =
    Resource.eval(contextProvider).flatMap { provider =>
      OtelJava.autoConfigured[IO]()(using summon, provider).flatMap { otel =>
        for {
          tracer <- Resource.eval(otel.tracerProvider.get(TracerName))
          _ <- RuntimeMetrics.register(otel.underlying, otel.meterProvider, runtimeMetrics)
        } yield TelemetryRuntime(tracer, otel.tracerProvider, otel.meterProvider, otel.underlying)
      }
    }
}
