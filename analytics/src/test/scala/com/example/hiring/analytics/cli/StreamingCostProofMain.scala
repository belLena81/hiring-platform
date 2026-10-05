package com.example.hiring.analytics.cli

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.spark.StreamingBatchCost
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.config.AnalyticsRuntimeConfig
import com.example.hiring.analytics.domain.AnalyticsTopic

/** Explicit isolated profiling entrypoint; uses the unchanged fail-closed production activation gate. */
object StreamingCostProofMain extends IOApp {
  private def emit(value: StreamingBatchCost.Summary): IO[Unit] = {
    val work = value.work
    IO.println(
      s"STREAMING_COST stage=${value.stage} result=${value.result} elapsedMillis=${value.elapsed.toMillis} " +
        s"jobs=${work.jobs} stages=${work.stages} tasks=${work.tasks} jobMillis=${work.jobMillis} " +
        s"stageMillis=${work.stageMillis} executorMillis=${work.executorMillis} " +
        s"inputBytes=${work.inputBytes} inputRows=${work.inputRows} " +
        s"shuffleReadBytes=${work.shuffleReadBytes} shuffleWriteBytes=${work.shuffleWriteBytes} " +
        s"memorySpillBytes=${work.memorySpillBytes} diskSpillBytes=${work.diskSpillBytes} untrackedJobs=${work.untrackedJobs}"
    )
  }

  override def run(args: List[String]): IO[ExitCode] =
    (for {
      _ <- IO.raiseUnless(args.isEmpty)(new IllegalArgumentException("cost diagnostics accepts no arguments"))
      settings <- AnalyticsRuntimeConfig.loadStreaming[IO]
      _ <- IO.fromEither(
        StreamingProofIsolation
          .validate(
            settings.common.mongoDatabase,
            AnalyticsTopic.unwrap(settings.topic),
            settings.common.lakehouseRoot,
            settings.streaming.checkpointLocation,
            settings.common.sparkLocalDirectory
          )
          .leftMap(new IllegalArgumentException(_))
      )
      _ <- IO.println("STREAMING_COST_DIAGNOSTICS contextWideAsyncCounters=true exactStageAttribution=false")
      _ <- StreamingProcessTermination.run(
        AppModule.streamingObserved[IO](settings, None, Some(emit)).use(_.run)
      )
    } yield ExitCode.Success).handleErrorWith(_ => IO.println("STREAMING_COST_DIAGNOSTICS_FAILED").as(ExitCode.Error))
}
