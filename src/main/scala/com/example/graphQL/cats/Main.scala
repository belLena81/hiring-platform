package com.example.graphQL.cats

import cats.effect.{ExitCode, IO, IOApp}
import com.example.graphQL.cats.application.{Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.config.AppConfig
import com.example.graphQL.cats.infrastructure.logging.SafeDiagnostics
import com.example.graphQL.cats.runtime.{HiringPlatformServer, MongoHiringRuntime}

object Main extends IOApp {
  override protected def reportFailure(error: Throwable): IO[Unit] =
    SafeDiagnostics.configure("ERROR").flatMap { diagnostics =>
      Diagnostics.emit(diagnostics, LogEvent.RuntimeFailed, fields = LogFields.failure(error))
    }

  def run(args: List[String]): IO[ExitCode] =
    SafeDiagnostics.configure("INFO").flatMap { fallback =>
      AppConfig.load.flatMap {
        case Left(error) => Diagnostics.emit(fallback, LogEvent.ConfigInvalid,
          fields = Map(LogField.ConfigKey -> error.key)).as(ExitCode.Error)
        case Right(config) => SafeDiagnostics.configure(config.logLevel, config.maskSensitive).flatMap { diagnostics =>
          MongoHiringRuntime.resource(config.mongoUri, config.mongoDatabase, diagnostics, config.vectorSearch)
            .flatMap(runtime => HiringPlatformServer.resource(
              config.host,
              config.port,
              runtime.probe,
              diagnostics,
              config.admissionPermits,
              Some(runtime.services),
              Some(config.jwtAuth),
              runtime.ensureSetup
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
