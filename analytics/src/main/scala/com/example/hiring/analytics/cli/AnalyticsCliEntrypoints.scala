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

  // Throwable messages and stack traces can contain credentials or source payloads.
  private[analytics] def safeFailureSummary(error: Throwable): String = {
    val causes = Iterator
      .iterate(Option(error.getCause))(_.flatMap(cause => Option(cause.getCause)))
      .take(3)
      .takeWhile(_.nonEmpty)
      .flatten
      .map(_.getClass.getSimpleName)
      .toList
    val label = error match {
      case _: AnalyticsError.InvalidInput              => "analytics input is invalid"
      case _: AnalyticsError.InvalidConfiguration      => "analytics configuration is invalid"
      case _: AnalyticsError.InvalidSourceSchema       => "analytics source schema is invalid"
      case _: AnalyticsError.EmptyRequestedRange       => "requested analytics range is empty"
      case _: AnalyticsError.ExpiredOffsetRange        => "requested analytics range expired"
      case _: AnalyticsError.MissingOffsetRange        => "requested analytics range is incomplete"
      case _: AnalyticsError.UnexpectedOffsetPartition => "analytics source returned an unrequested partition"
      case _: AnalyticsError.RunIdRangeConflict        => "analytics run ID conflicts with existing ranges"
      case _: AnalyticsError.MarkerLimitExceeded       => "analytics erasure marker limit exceeded"
      case analyticsError: AnalyticsError              => analyticsError.getMessage
      case _ => s"analytics program failed unexpectedly: ${error.getClass.getSimpleName}"
    }
    if (causes.isEmpty) label else s"$label; cause types: ${causes.mkString(" -> ")}"
  }

  def runProgram[A](program: IO[A]): IO[ExitCode] =
    program.attempt.flatMap {
      case Right(_)    => IO.pure(ExitCode.Success)
      case Left(error) => logger.error(safeFailureSummary(error)).as(ExitCode.Error)
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
