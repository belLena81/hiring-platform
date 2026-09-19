package com.example.graphQL.cats

import cats.effect.{ExitCode, IO, IOApp}
import com.example.graphQL.cats.api.auth.JwtActorAuthenticator
import com.example.graphQL.cats.api.graphql.RequestContextFactory
import com.example.graphQL.cats.api.http.{Admission, ClientAddressResolver, FixedWindowRateLimiter, HiringApiRoutes}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields, ProbeResult}
import com.example.graphQL.cats.config.AppConfig
import com.example.graphQL.cats.infrastructure.logging.SafeDiagnostics
import com.example.graphQL.cats.runtime.{HiringPlatformServer, MongoHiringRuntime}

object Main extends IOApp {
  override protected def reportFailure(error: Throwable): IO[Unit] =
    SafeDiagnostics.configure().flatMap { diagnostics =>
      Diagnostics.emit(diagnostics, LogEvent.RuntimeFailed, fields = LogFields.failure(error))
    }

  def run(args: List[String]): IO[ExitCode] =
    AppConfig.loadMaskSensitive.flatMap { maskSensitive =>
      SafeDiagnostics.configure(maskSensitive).flatMap { fallback =>
        AppConfig.load.flatMap {
          case Left(errors) => Diagnostics.emit(fallback, LogEvent.ConfigInvalid,
            fields = Map(LogField.ConfigKey -> errors.head.key)).as(ExitCode.Error)
          case Right(config) => SafeDiagnostics.configure(config.maskSensitive).flatMap { diagnostics =>
            MongoHiringRuntime.resource(config.mongoUri, config.mongoDatabase, diagnostics, config.vectorSearch,
              (vector, apiKey) => new com.example.graphQL.cats.infrastructure.embedding.VoyageEmbeddingService(
                apiKey, vector.voyageEndpoint, vector.voyageModel, vector.voyageDimension, vector.timeoutMillis), config.jwtAuth,
              config.resolverTimeout, config.passwordHash)
              .flatMap { runtime =>
                for {
                  admission <- Admission.resource(config.admissionPermits)
                  contextFactory <- RequestContextFactory.resource
                  rateLimiter <- FixedWindowRateLimiter.resource(config.authRateLimit)
                  authenticate = new JwtActorAuthenticator(
                    config.jwtAuth,
                    runtime.userAuthenticator,
                    cats.effect.Clock[IO]
                  ).authenticateDetailed
                  routes = new HiringApiRoutes(
                    new com.example.graphQL.cats.service.HealthService(runtime.probe, diagnostics),
                    diagnostics,
                    admission,
                    HiringApiRoutes.Dependencies(
                      runtime.services,
                      authenticate,
                      runtime.ensureSetup.map(if (_) ProbeResult.Ready else ProbeResult.Unavailable),
                      contextFactory,
                      rateLimiter,
                      ClientAddressResolver(config.trustedProxy),
                      config.requestTimeout
                    )
                  ).app
                  server <- HiringPlatformServer.resource(config.host, config.port, routes)
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
