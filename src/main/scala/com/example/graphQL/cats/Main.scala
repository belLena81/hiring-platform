package com.example.graphQL.cats

import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.example.graphQL.cats.api.auth.JwtActorAuthenticator
import com.example.graphQL.cats.api.graphql.RequestContextFactory
import com.example.graphQL.cats.api.http.{ClientAddressResolver, FixedWindowRateLimiter, HiringApiRoutes}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.config.AppConfig
import com.example.graphQL.cats.infrastructure.logging.SafeDiagnostics
import com.example.graphQL.cats.infrastructure.telemetry.TelemetryRuntime
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.example.graphQL.cats.runtime.{HiringPlatformServer, MongoHiringRuntime}

object Main extends IOApp {
  System.setProperty("cats.effect.trackFiberContext", "true")

  override protected def reportFailure(error: Throwable): IO[Unit] =
    val logger = Slf4jLogger.getLoggerFromName[IO]("hiring.foundation")
    logger.error(Map("errorType" -> error.getClass.getName))("Unhandled runtime failure")

  def run(args: List[String]): IO[ExitCode] =
    AppConfig.loadMaskSensitive.flatMap { maskSensitive =>
      val tracingEnabled = sys.env.get("OTEL_TRACES_EXPORTER").exists(value => value.nonEmpty && value != "none")
      TelemetryRuntime.resource(tracingEnabled).use { telemetry =>
      SafeDiagnostics.configure(maskSensitive).flatMap { fallback =>
        AppConfig.load.flatMap {
          case Left(errors) => Diagnostics.emit(fallback, LogEvent.ConfigInvalid,
            fields = Map(LogField.ConfigKey -> errors.head.key)).as(ExitCode.Error)
          case Right(config) => SafeDiagnostics.configure(config.maskSensitive).flatMap { diagnostics =>
            MongoHiringRuntime.resource(config.mongoUri, config.mongoDatabase, diagnostics, config.vectorSearch,
              config.jwtAuth,
              config.resolverTimeout, config.passwordHash, config.kafka, telemetry.tracer)
              .flatMap { runtime =>
                for {
                  contextFactory <- RequestContextFactory.resource
                  rateLimiter <- FixedWindowRateLimiter.resource(config.authRateLimit)
                  authenticate = new JwtActorAuthenticator(
                    config.jwtAuth,
                    runtime.userAuthenticator,
                    cats.effect.Clock[IO]
                  ).authenticateDetailed
                  routeBuilder = new HiringApiRoutes(
                    new com.example.graphQL.cats.service.HealthService(runtime.probe, diagnostics),
                    diagnostics,
                    HiringApiRoutes.Dependencies(
                      runtime.services,
                      authenticate,
                      runtime.hiringReadiness,
                      contextFactory,
                      rateLimiter,
                      ClientAddressResolver(config.trustedProxy)
                    ),
                    telemetry.tracer
                  )
                  routes <- Resource.eval(routeBuilder.httpApp(HiringApiRoutes.HttpConfig(config.admissionPermits, config.requestTimeout)))
                  server <- HiringPlatformServer.resource(config.host, config.port, routes, telemetry.logger)
                } yield server
              }
              .use(_ => Diagnostics.emit(diagnostics, LogEvent.Started, fields = Map(
                LogField.HttpHost -> config.host,
                LogField.HttpPort -> config.port.toString
              )) *> IO.never[ExitCode])
              .guarantee(Diagnostics.emit(diagnostics, LogEvent.Shutdown))
              .handleErrorWith(error => Diagnostics.emit(diagnostics, LogEvent.StartupFailed,
                fields = LogFields.failure(error)).as(ExitCode.Error))
          }
        }.handleErrorWith(error => Diagnostics.emit(fallback, LogEvent.StartupFailed,
          fields = LogFields.failure(error)).as(ExitCode.Error))
      }
      }
    }
}
