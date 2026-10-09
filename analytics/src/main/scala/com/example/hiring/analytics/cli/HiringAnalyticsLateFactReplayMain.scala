package com.example.hiring.analytics.cli

import cats.effect.{ExitCode, IO}
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Explicit operator replay configured through HOCON, with no command-line credential or payload arguments. */
object HiringAnalyticsLateFactReplayMain
    extends HoconConfiguredCommand("hiring-analytics-late-fact-replay", "Replay selected late hiring facts") {
  private val logger = Slf4jLogger.getLogger[IO]

  override protected def program: IO[ExitCode] =
    AnalyticsRuntimeConfig
      .loadLateFactReplay[IO]
      .flatMap(settings =>
        AppModule
          .lateFactReplay[IO](settings)
          .use(identity)
          .flatMap(outcome => logger.info(s"late replay outcome: $outcome"))
      )
      .as(ExitCode.Success)
}
