package com.example.graphQL.cats

import cats.effect.{ExitCode, IO, IOApp}
import com.example.graphQL.cats.application.LogEvent
import com.example.graphQL.cats.config.AppConfig
import com.example.graphQL.cats.infrastructure.logging.SafeDiagnostics
import com.example.graphQL.cats.infrastructure.mongo.MongoDatabaseProbe
import com.example.graphQL.cats.runtime.FoundationServer

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] =
    SafeDiagnostics.configure("INFO").flatMap { fallback =>
      AppConfig.load.flatMap {
        case Left(_) => fallback.event(LogEvent.ConfigInvalid).as(ExitCode.Error)
        case Right(config) => SafeDiagnostics.configure(config.logLevel).flatMap { diagnostics =>
          MongoDatabaseProbe.resource(config.mongoUri, config.mongoDatabase)
            .flatMap(probe => FoundationServer.resource(config.host, config.port, probe, diagnostics))
            .use(_ => diagnostics.event(LogEvent.Started) *> IO.never[ExitCode])
            .guarantee(diagnostics.event(LogEvent.Shutdown))
            .handleErrorWith(_ => diagnostics.event(LogEvent.StartupFailed).as(ExitCode.Error))
        }
      }.handleErrorWith(_ => fallback.event(LogEvent.StartupFailed).as(ExitCode.Error))
    }
}
