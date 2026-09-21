package com.example.graphQL.cats

import cats.data.NonEmptyList
import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.example.graphQL.cats.api.auth.JwtActorAuthenticator
import com.example.graphQL.cats.api.graphql.RequestContextFactory
import com.example.graphQL.cats.api.http.{AuthRateLimiter, ClientAddressResolver, HiringApiRoutes}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.config.{AppConfig, ConfigError}
import com.example.graphQL.cats.infrastructure.logging.SafeDiagnostics
import com.example.graphQL.cats.infrastructure.telemetry.TelemetryRuntime
import org.typelevel.log4cats.slf4j.Slf4jLogger
import com.example.graphQL.cats.runtime.{HiringPlatformServer, MongoHiringRuntime}
import scala.util.control.NoStackTrace

object Main extends IOApp {
  System.setProperty("cats.effect.trackFiberContext", "true")

  private final case class ConfigInvalid(errors: NonEmptyList[ConfigError])
      extends RuntimeException with NoStackTrace

  override protected def reportFailure(error: Throwable): IO[Unit] =
    val logger = Slf4jLogger.getLoggerFromName[IO]("hiring.foundation")
    logger.error(Map("errorType" -> error.getClass.getName))("Unhandled runtime failure")

  private def program: Resource[IO, Unit] = for {
    telemetry <- TelemetryRuntime.resource
    config <- Resource.eval(AppConfig.load.flatMap(_.fold(
      errors => IO.raiseError[AppConfig](ConfigInvalid(errors)),
      IO.pure
    )))
    diagnostics <- Resource.eval(SafeDiagnostics.configure(config.maskSensitive))
    runtime <- MongoHiringRuntime.resource(config.mongoUri, config.mongoDatabase, diagnostics, config.vectorSearch,
      config.jwtAuth, config.resolverTimeout, config.passwordHash, config.kafka, telemetry.tracer)
    contextFactory <- RequestContextFactory.resource
    rateLimiter <- Resource.eval(AuthRateLimiter.create(config.authRateLimit))
    authenticate = new JwtActorAuthenticator(config.jwtAuth, runtime.userAuthenticator, cats.effect.Clock[IO])
      .authenticateDetailed
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
    _ <- HiringPlatformServer.resource(config.host, config.port, routes, telemetry.logger)
    _ <- Resource.make(Diagnostics.emit(diagnostics, LogEvent.Started, fields = Map(
      LogField.HttpHost -> config.host.toString,
      LogField.HttpPort -> config.port.toString
    )))(_ => Diagnostics.emit(diagnostics, LogEvent.Shutdown))
  } yield ()

  private def handleStartupFailure(maskSensitive: Boolean)(error: Throwable): IO[ExitCode] =
    SafeDiagnostics.configure(maskSensitive).flatMap { diagnostics =>
      val event = error match {
        case ConfigInvalid(errors) => Diagnostics.emit(diagnostics, LogEvent.ConfigInvalid,
          fields = Map(LogField.ConfigKey -> errors.head.key))
        case _ => Diagnostics.emit(diagnostics, LogEvent.StartupFailed, fields = LogFields.failure(error))
      }
      event.as(ExitCode.Error)
    }

  def run(args: List[String]): IO[ExitCode] =
    AppConfig.loadMaskSensitive.flatMap { maskSensitive =>
      program.useForever.as(ExitCode.Success).handleErrorWith(handleStartupFailure(maskSensitive))
    }
}
