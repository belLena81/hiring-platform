package com.example.graphQL.cats

import cats.effect.{ExitCode, IO, IOApp}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields, ProbeResult}
import com.example.graphQL.cats.config.AppConfig
import com.example.graphQL.cats.infrastructure.logging.SafeDiagnostics
import com.example.graphQL.cats.runtime.{HiringPlatformServer, MongoHiringRuntime}
import scala.concurrent.duration.*

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
                apiKey, vector.voyageEndpoint, vector.voyageModel, vector.voyageDimension, vector.timeoutMillis), config.jwtAuth)
              .flatMap(runtime => HiringPlatformServer.resource(
                config.host,
                config.port,
                runtime.probe,
                diagnostics,
                config.admissionPermits,
                runtime.services,
                config.jwtAuth,
                config.authRateLimit,
                runtime.userAuthenticator,
                runtime.ensureSetup.map(if (_) ProbeResult.Ready else ProbeResult.Unavailable),
                5.seconds
              ))
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
