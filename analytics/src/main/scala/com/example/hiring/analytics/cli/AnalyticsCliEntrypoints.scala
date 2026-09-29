package com.example.hiring.analytics.cli

import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig
import com.example.hiring.analytics.service.batch.AnalyticsPublication
import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import org.typelevel.log4cats.slf4j.Slf4jLogger

private[cli] object AnalyticsCliProgram {
  private val logger = Slf4jLogger.getLogger[IO]

  def runProgram[A](program: IO[A]): IO[ExitCode] =
    program.attempt.flatMap {
      case Right(_)                    => IO.pure(ExitCode.Success)
      case Left(error: AnalyticsError) => logger.error(error.getMessage).as(ExitCode.Error)
      case Left(error)                 =>
        logger.error(s"analytics program failed unexpectedly: ${error.getClass.getSimpleName}").as(ExitCode.Error)
    }
}

object HiringAnalyticsBatchMain extends IOApp {
  private val logger = Slf4jLogger.getLogger[IO]

  private def program: IO[AnalyticsPublication] =
    AnalyticsRuntimeConfig.loadBatch[IO].flatMap(settings => AppModule.batch[IO](settings).use(_.run))

  override def run(args: List[String]): IO[ExitCode] =
    AnalyticsCliProgram.runProgram(
      (if (args.nonEmpty)
         IO.raiseError[AnalyticsPublication](
           AnalyticsError.InvalidConfiguration(
             "batch inputs are loaded from HOCON; command-line arguments are not accepted"
           )
         )
       else program).flatTap(publication => logger.info(publication.toString))
    )
}

object AnalyticsErasureWorkerMain extends IOApp {
  private val logger = Slf4jLogger.getLogger[IO]

  private def program: IO[Unit] =
    AnalyticsRuntimeConfig.loadWorker[IO].flatMap(settings => AppModule.worker[IO](settings).use(_.run))

  override def run(args: List[String]): IO[ExitCode] =
    AnalyticsCliProgram.runProgram(
      if (args.nonEmpty)
        IO.raiseError[Unit](
          AnalyticsError.InvalidConfiguration(
            "worker settings are loaded from HOCON; command-line arguments are not accepted"
          )
        )
      else program
    )
}
