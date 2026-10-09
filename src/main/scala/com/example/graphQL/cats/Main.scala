package com.example.graphQL.cats

import cats.data.NonEmptyList
import cats.effect.{ExitCode, IO, IOApp, Resource}
import cats.data.Kleisli
import cats.syntax.all.*
import com.example.graphQL.cats.api.admission.{AuthRateLimiter, InterviewActionRateLimiter}
import com.example.graphQL.cats.api.auth.JwtActorAuthenticator
import com.example.graphQL.cats.api.graphql.{GraphQLDocumentCache, RequestContextFactory}
import com.example.graphQL.cats.api.http.{ClientAddressResolver, HiringApiRoutes}
import com.example.graphQL.cats.service.{Diagnostics, HealthService, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.config.{AppConfig, ConfigError}
import com.example.graphQL.cats.infrastructure.logging.SafeDiagnostics
import com.example.graphQL.cats.infrastructure.telemetry.TelemetryRuntime
import com.example.graphQL.cats.runtime.{HiringPlatformServer, MongoHiringRuntime}

object Main extends IOApp {
  override protected def reportFailure(error: Throwable): IO[Unit] =
    SafeDiagnostics.configure().flatMap { diagnostics =>
      diagnostics.emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error))
    }

  private def program(config: AppConfig, diagnostics: Diagnostics): Resource[IO, Unit] = for {
    telemetry <- TelemetryRuntime.resource
    runtime <- MongoHiringRuntime.resource(config, diagnostics)
    contextFactory <- RequestContextFactory.resource
    documentCache <- GraphQLDocumentCache.resource
    rateLimiter <- Resource.eval(AuthRateLimiter.create(config.authRateLimit))
    interviewActionLimiter <- Resource.eval(InterviewActionRateLimiter.create(config.interviewActionRateLimit))
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
        interviewActionLimiter,
        ClientAddressResolver(config.trustedProxy),
        config.discovery.maxRoots
      ),
      telemetry.tracer
    )
    routeConfig = HiringApiRoutes.HttpConfig(config.admissionPermits, config.requestTimeout)
    routeSet <- Resource.eval(routeBuilder.httpRoutes(routeConfig))
    routes <- telemetry.instrument(routeSet)
    _ <- HiringPlatformServer.resource(config.host, config.port, routes, diagnostics)
    _ <- Resource.make(
      diagnostics.emit(
        LogEvent.Started,
        fields = Map(
          LogField.HttpHost -> config.host.toString,
          LogField.HttpPort -> config.port.toString
        )
      )
    )(_ => diagnostics.emit(LogEvent.Shutdown))
  } yield ()

  /** One `CONFIG_INVALID` event per distinct public key; values never leave the validation boundary. */
  private[cats] def configInvalidEvents(diagnostics: Diagnostics, errors: NonEmptyList[ConfigError]): IO[Unit] =
    errors.map(_.key).distinct.traverse_ { key =>
      diagnostics.emit(LogEvent.ConfigInvalid, fields = Map(LogField.ConfigKey -> key))
    }

  /** The configuration is read once; the masking flag is read leniently first so invalid files still log safely. */
  def run(args: List[String]): IO[ExitCode] =
    for {
      mask <- AppConfig.loadMaskSensitive
      diagnostics <- SafeDiagnostics.configure(mask)
      loaded <- AppConfig.load
      exit <- loaded.fold(
        errors => configInvalidEvents(diagnostics, errors).as(ExitCode.Error),
        config =>
          program(config, diagnostics).useForever
            .as(ExitCode.Success)
            .handleErrorWith(error =>
              diagnostics.emit(LogEvent.StartupFailed, fields = LogFields.failure(error)).as(ExitCode.Error)
            )
      )
    } yield exit
}
