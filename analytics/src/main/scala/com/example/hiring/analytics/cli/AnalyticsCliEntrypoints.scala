package com.example.hiring.analytics.cli

import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig
import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.{ExitCode, IO}
import com.monovore.decline.Opts
import com.monovore.decline.effect.CommandIOApp
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

  /** Maps an expected process outcome to its exit code; failures are logged as a sanitized summary and exit with error.
    */
  def runCommand(program: IO[ExitCode]): IO[ExitCode] =
    program.attempt.flatMap {
      case Right(code) => IO.pure(code)
      case Left(error) => logger.error(safeFailureSummary(error)).as(ExitCode.Error)
    }

  def runProgram[A](program: IO[A]): IO[ExitCode] = runCommand(program.as(ExitCode.Success))
}

/** Commands configured only through HOCON. Decline rejects any command-line argument, option or help flag, so no
  * credential or payload can arrive through the process arguments.
  */
private[cli] abstract class HoconConfiguredCommand(name: String, header: String)
    extends CommandIOApp(name = name, header = header, helpFlag = false) {
  protected def program: IO[ExitCode]

  final override def main: Opts[IO[ExitCode]] = Opts.unit.map(_ => AnalyticsCliProgram.runCommand(program))
}

object HiringAnalyticsBatchMain
    extends HoconConfiguredCommand("hiring-analytics-batch", "Run one bounded hiring analytics batch") {
  private val logger = Slf4jLogger.getLogger[IO]

  override protected def program: IO[ExitCode] =
    AnalyticsRuntimeConfig
      .loadBatch[IO]
      .flatMap(settings => AppModule.batch[IO](settings).use(identity))
      .flatTap(publication => logger.info(publication.toString))
      .as(ExitCode.Success)
}

object HiringAnalyticsStreamingMain
    extends HoconConfiguredCommand("hiring-analytics-streaming", "Run the hiring analytics streaming job") {
  override protected def program: IO[ExitCode] =
    StreamingProcessTermination
      .run(
        AnalyticsRuntimeConfig.loadStreaming[IO].flatMap(settings => AppModule.streaming[IO](settings).use(identity))
      )
      .as(ExitCode.Success)
}

object AnalyticsErasureWorkerMain
    extends HoconConfiguredCommand("analytics-erasure-worker", "Run the analytics erasure worker") {
  override protected def program: IO[ExitCode] =
    StreamingProcessTermination
      .run(AnalyticsRuntimeConfig.loadWorker[IO].flatMap(settings => AppModule.worker[IO](settings).use(identity)))
      .as(ExitCode.Success)
}
