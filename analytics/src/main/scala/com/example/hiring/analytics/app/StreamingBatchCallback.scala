package com.example.hiring.analytics.app

import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.config.AnalyticsCommonSettings
import com.example.hiring.analytics.domain.{StreamingBatchId, StreamingLineage}
import com.example.hiring.analytics.service.streaming.*

import cats.effect.Async
import cats.syntax.all.*
import org.apache.spark.sql.DataFrame
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** The per-micro-batch streaming callback: builds the batch stages and stacks the cost and durable-boundary decorators
  * around the journal, writer and stages. Every dependency is explicit; composition only constructs it.
  */
private[app] final class StreamingBatchCallback[F[_]: Async](
    shared: AppModule.Shared[F],
    common: AnalyticsCommonSettings,
    journal: StreamingBatchJournal[F],
    checkpoint: StreamingCheckpointAcknowledgement[F],
    cost: Option[StreamingBatchCost[F]],
    observer: Option[StreamingDurableBoundaryObserver[F]]
) {
  import StreamingBatchCallback.timed

  def apply(
      rawFrame: DataFrame,
      batchId: StreamingBatchId,
      lineage: StreamingLineage,
      sourceEndOffsets: Map[(String, Int), Long],
      authorize: F[Unit]
  ): F[Unit] = {
    val callback = for {
      observedAt <- Async[F].realTimeInstant
      _ <- timed(cost, StreamingBatchCost.Stage.HmacValidation)(shared.maintenance.validateHmacConfigurationLocked)
      parsed <- shared.lakehouseExecution(OperationalEventTransforms.parseKafkaRecords(rawFrame))
      preparation <- timed(cost, StreamingBatchCost.Stage.Prepare)(
        StreamingBatchPreparation(
          shared.lakehouseExecution,
          parsed,
          lineage,
          batchId,
          observedAt,
          sourceEndOffsets,
          journal
        )
      )
      observedWriter = observer.fold[DeltaWriter[F]](shared.writer)(value =>
        StreamingDurableBoundaryObserver.writer(shared.writer, shared.paths, preparation.identity, value)
      )
      (ingestion, silver) = AppModule.batchStages(
        shared,
        common,
        new DeltaManifestStore[F](shared.spark, shared.paths, shared.sparkExecution),
        observedWriter
      )
      _ <- SparkStreamingBatchStages
        .resource(
          shared.spark,
          parsed,
          shared.paths,
          common.pseudonymizer,
          shared.lakehouseExecution,
          ingestion,
          silver,
          new AnalyticsLateFactStage[F](shared.paths, shared.lakehouseExecution, observedWriter),
          observedWriter,
          shared.reader,
          shared.publisher,
          common.operational.retention,
          shared.maintenance.configureRawTables,
          shared.maintenance.purgeMarkedSubjects,
          authorize
        )
        .use { stages =>
          val costed = cost.fold(stages)(_.wrap(stages))
          new StreamingBatchCoordinator[F](
            observer.fold(journal)(StreamingDurableBoundaryObserver.journal(journal, _)),
            shared.markers,
            observer.fold(costed)(StreamingDurableBoundaryObserver.stages(costed, _)),
            checkpoint,
            authorize
          ).process(preparation).void
        }
    } yield ()
    timed(cost, StreamingBatchCost.Stage.Callback)(callback)
  }
}

private[app] object StreamingBatchCallback {
  def timed[F[_], A](cost: Option[StreamingBatchCost[F]], stage: StreamingBatchCost.Stage)(work: F[A]): F[A] =
    cost.fold(work)(_.timed(stage)(work))

  /** The sanitized maintenance observation as one structured log line. */
  def maintenanceLogLine(observation: AnalyticsStreamingMaintenance.Observation): String =
    s"STREAMING_MAINTENANCE outcome=${observation.outcome.fold("none")(_.toString)} " +
      s"elapsedSinceSuccessMillis=${observation.elapsedSinceSuccess.toMillis} " +
      s"maximumElapsedSinceSuccessMillis=${observation.maximumElapsedSinceSuccess.toMillis} " +
      s"consecutiveDeferrals=${observation.consecutiveDeferrals} " +
      s"lastSuccess=${observation.lastSuccess.fold("none")(_.toString)}"

  def logMaintenance[F[_]: Async](observation: AnalyticsStreamingMaintenance.Observation): F[Unit] =
    Slf4jLogger
      .getLoggerFromName[F]("com.example.hiring.analytics.streaming.maintenance")
      .info(maintenanceLogLine(observation))
}
