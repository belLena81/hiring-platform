package com.example.graphQL.cats

import cats.data.NonEmptyList
import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.data.Kleisli
import com.example.graphQL.cats.api.admission.AuthRateLimiter
import com.example.graphQL.cats.api.auth.JwtActorAuthenticator
import com.example.graphQL.cats.api.graphql.{GraphQLDocumentCache, RequestContextFactory}
import com.example.graphQL.cats.api.http.{ClientAddressResolver, HiringApiRoutes}
import com.example.graphQL.cats.service.{Diagnostics, HealthService, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.config.{AppConfig, ConfigError}
import com.example.graphQL.cats.infrastructure.logging.SafeDiagnostics
import com.example.graphQL.cats.infrastructure.telemetry.TelemetryRuntime
import com.example.graphQL.cats.runtime.{HiringPlatformServer, MongoHiringRuntime}

import scala.util.control.NoStackTrace

object Main extends IOApp {
  private final case class ConfigInvalid(errors: NonEmptyList[ConfigError])
      extends RuntimeException with NoStackTrace

  override protected def reportFailure(error: Throwable): IO[Unit] =
    SafeDiagnostics.configure().flatMap { diagnostics =>
      diagnostics.emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error))
    }

  private def program: Resource[IO, Unit] = for {
    mask <- Resource.eval(AppConfig.loadMaskSensitive)
    telemetry <- TelemetryRuntime.resource
    config <- Resource.eval(AppConfig.load.flatMap(_.fold(
      errors => IO.raiseError[AppConfig](ConfigInvalid(errors)),
      IO.pure
    )))
    diagnostics <- Resource.eval(SafeDiagnostics.configure(mask && config.maskSensitive))
    runtime <- MongoHiringRuntime.resource(MongoHiringRuntime.RuntimeConfig(
      config.mongoUri,
      config.mongoDatabase,
      diagnostics,
      config.vectorSearch,
      config.jwtAuth,
      config.passwordHash,
      config.kafka,
      config.resetOnStart
    ))
    contextFactory <- RequestContextFactory.resource
    documentCache <- GraphQLDocumentCache.resource
    rateLimiter <- Resource.eval(AuthRateLimiter.create(config.authRateLimit))
    authenticator = new JwtActorAuthenticator(config.jwtAuth, runtime.userAuthenticator, cats.effect.Clock[IO])
    authenticate = Kleisli(authenticator.authenticate)
    routeBuilder = new HiringApiRoutes(
      new HealthService(runtime.probe, diagnostics),
      diagnostics,
      HiringApiRoutes.Dependencies(
        runtime.services,
        authenticate,
        runtime.hiringReadiness,
        contextFactory,
        documentCache,
        rateLimiter,
        ClientAddressResolver(config.trustedProxy)
      ),
      telemetry.tracer
    )
    routeConfig = HiringApiRoutes.HttpConfig(config.admissionPermits, config.requestTimeout)
    routeSet <- Resource.eval(routeBuilder.httpRoutes(routeConfig))
    routes <- telemetry.instrument(routeSet)
    _ <- HiringPlatformServer.resource(config.host, config.port, routes, diagnostics)
    _ <- Resource.make(diagnostics.emit(LogEvent.Started, fields = Map(
      LogField.HttpHost -> config.host.toString,
      LogField.HttpPort -> config.port.toString
    )))(_ => diagnostics.emit(LogEvent.Shutdown))
  } yield ()

  private def handleStartupFailure(error: Throwable): IO[ExitCode] =
    AppConfig.loadMaskSensitive.flatMap { maskSensitive =>
      SafeDiagnostics.configure(maskSensitive).flatMap { diagnostics =>
        val event = error match {
          case ConfigInvalid(errors) => diagnostics.emit(LogEvent.ConfigInvalid,
            fields = Map(LogField.ConfigKey -> errors.head.key))
          case _ => diagnostics.emit(LogEvent.StartupFailed, fields = LogFields.failure(error))
        }
        event.as(ExitCode.Error)
      }
    }

  def run(args: List[String]): IO[ExitCode] =
    program.useForever.as(ExitCode.Success).handleErrorWith(handleStartupFailure)
}
