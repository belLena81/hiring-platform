package com.example.hiring.analytics.app

import com.example.hiring.analytics.adapter.kafka.{KafkaProducerFencer, KafkaRetentionAdapter}
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.service.batch.HiringAnalyticsBatch
import com.example.hiring.analytics.service.erasure.AnalyticsErasureWorker
import com.example.hiring.analytics.config.{
  KafkaConnection,
  AnalyticsBatchSettings,
  AnalyticsCommonSettings,
  AnalyticsErasureWorkerPolicy,
  AnalyticsWorkerSettings,
  AnalyticsStreamingRuntimeSettings,
  AnalyticsNonBlank,
  AnalyticsKeyRetirementAuditSettings,
  AnalyticsLateFactReplaySettings
}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.service.erasure.*
import com.example.hiring.analytics.service.streaming.*

import cats.data.NonEmptyChain
import cats.effect.{Async, Clock, Resource}
import cats.effect.std.{SecureRandom, UUIDGen}
import fs2.io.file.{Files, Path}
import cats.syntax.all.*
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import org.apache.spark.sql.SparkSession
import org.typelevel.log4cats.slf4j.Slf4jLogger
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.Not
import io.github.iltotore.iron.constraint.string.Blank

/** Resource-managed composition for analytics command-line applications. */
object AppModule {
  private[app] final case class Shared[F[_]](
      paths: AnalyticsLakehousePaths,
      spark: SparkSession,
      client: MongoClient[F],
      database: MongoDatabase[F],
      sparkExecution: SparkBlockingExecution[F],
      lakehouseExecution: LakehouseOperation[F],
      streams: MongoPublisherStream,
      markers: MongoActiveDeletionMarkerSource[F],
      lock: MongoAnalyticsLakehouseLock[F],
      maintenance: DeltaAnalyticsErasureLakehouse[F],
      writer: DeltaBatchWriter[F],
      reader: DeltaBatchReader[F],
      publisher: MongoAnalyticsReportPublisher[F]
  )

  /** One OS-backed generator per program replaces direct `UUID.randomUUID` calls in composed adapters. */
  private def withSecureUuids[F[_]: Async, A](use: UUIDGen[F] ?=> Resource[F, A]): Resource[F, A] =
    Resource.eval(SecureRandom.javaSecuritySecureRandom[F]).flatMap { random =>
      use(using UUIDGen.fromSecureRandom[F](using Async[F], random))
    }

  private[analytics] def resolveLakehousePaths(root: String): Either[AnalyticsError, AnalyticsLakehousePaths] =
    AnalyticsLakehousePaths
      .from(root)
      .toEither
      .leftMap(AnalyticsError.fromProblems)

  def batch[F[_]: Async](settings: AnalyticsBatchSettings): Resource[F, F[AnalyticsPublication]] =
    withSecureUuids[F, F[AnalyticsPublication]] {
      for {
        shared <- shared[F](settings.common, appName = "hiring-analytics-batch")
      } yield {
        val common = settings.common
        val manifestStore = new DeltaManifestStore[F](shared.spark, shared.paths, shared.sparkExecution)
        val (ingestionStage, silverStage) = batchStages(shared, common, manifestStore, shared.writer)
        val job = new HiringAnalyticsBatch[F](
          shared.paths,
          shared.markers,
          shared.publisher,
          manifestStore,
          new SparkAnalyticsBatchLakehouse[F](
            shared.spark,
            shared.paths,
            new KafkaOffsetRangeSource[F](common.kafka, shared.sparkExecution, shared.sparkExecution),
            shared.lakehouseExecution,
            shared.maintenance,
            ingestionStage,
            silverStage
          ),
          shared.lock,
          new MongoAnalyticsStreamingRegistry[F](shared.database, shared.streams),
          common.operational,
          Clock[F]
        )

        job.run(settings.manifest)

      }
    }

  /** The ingestion and Silver stages share one wiring; only the writer and manifest store differ per program. */
  private[app] def batchStages[F[_]: Async](
      shared: Shared[F],
      common: AnalyticsCommonSettings,
      manifestStore: AnalyticsRunManifestStore[F],
      writer: DeltaWriter[F]
  ): (AnalyticsBatchIngestionStage[F], AnalyticsBatchSilverStage[F]) =
    (
      new AnalyticsBatchIngestionStage[F](
        shared.paths,
        common.pseudonymizer,
        shared.lakehouseExecution,
        manifestStore,
        writer,
        common.operational.retention,
        Clock[F]
      ),
      new AnalyticsBatchSilverStage[F](
        shared.paths,
        common.pseudonymizer,
        shared.lakehouseExecution,
        writer,
        shared.reader,
        QuarantineIdentifier,
        common.operational.retention
      )
    )

  def worker[F[_]: Async](settings: AnalyticsWorkerSettings): Resource[F, F[Unit]] =
    withSecureUuids[F, F[Unit]] {
      for {
        shared <- shared[F](settings.common, appName = "hiring-analytics-erasure-worker")
        program <- MongoAnalyticsErasureStores.resource(shared.client, shared.database, shared.streams).map { stores =>
          val common = settings.common
          val job = new AnalyticsErasureWorker[F](
            stores.queue,
            stores.progress,
            stores.barrier,
            AnalyticsErasureKafkaRuntime(
              settings.fencerKafka,
              KafkaProducerFencer[F](shared.sparkExecution),
              KafkaRetentionAdapter.liveRetention[F](common.kafka, settings.topic, shared.sparkExecution)
            ),
            shared.paths,
            shared.publisher,
            shared.markers,
            shared.maintenance,
            shared.lock,
            Slf4jLogger.getLogger[F],
            AnalyticsErasureWorkerPolicy(
              common.operational.retention,
              common.operational.erasureWorkerTimings
            ),
            Clock[F]
          )
          job.run
        }
      } yield program
    }

  /** Long-lived, opt-in Structured Streaming runtime. The Mongo activation gate is read-only and fail-closed. */
  def streaming[F[_]: Async](settings: AnalyticsStreamingRuntimeSettings): Resource[F, F[Unit]] =
    streamingObserved(settings, None)

  /** Adapter-private observation seam: callbacks execute only after durable operations return. */
  private[analytics] def streamingObserved[F[_]: Async](
      settings: AnalyticsStreamingRuntimeSettings,
      observer: Option[StreamingDurableBoundaryObserver[F]],
      costObserver: Option[StreamingBatchCost.Summary => F[Unit]] = None
  ): Resource[F, F[Unit]] =
    withSecureUuids[F, F[Unit]] {
      for {
        localDirectory <- ownedStreamingDirectory[F](settings.common.sparkLocalDirectory)
        common = settings.common.copy(sparkLocalDirectory = localDirectory)
        shared <- shared[F](common, appName = "hiring-analytics-streaming")
        streamingLock <- AnalyticsLakehouseLock.serialized(shared.lock)
        cost <- costObserver.traverse(emit => StreamingBatchCost.resource(shared.spark, emit))
        program <- Resource.eval {
          val journal = new DeltaStreamingBatchJournal[F](shared.spark, shared.paths, shared.sparkExecution)
          val observedJournal = cost.fold[StreamingBatchJournal[F]](journal)(_.wrapJournal(journal))
          val checkpoint = new DeltaStreamingCheckpointAcknowledgement[F](observedJournal)
          val observedLock = cost.fold[AnalyticsLakehouseLock[F]](streamingLock)(_.wrapLock(streamingLock))
          val replayJournal =
            new MongoAnalyticsLateFactReplayJournal[F](shared.database, shared.streams, common.lakehouseRoot)
          val maintenanceTick = new StreamingMaintenanceTick[F](
            shared.maintenance,
            shared.markers,
            shared.publisher,
            journal,
            replayJournal,
            common.operational.retention,
            settings.streaming.progressRetention
          )
          val stream = new SparkHiringAnalyticsStream[F](
            shared.spark,
            shared.lakehouseExecution,
            shared.sparkExecution,
            common.kafka,
            settings.topic,
            settings.streaming,
            new MongoStreamingActivationGate[F](shared.database, shared.streams),
            new MongoAnalyticsStreamingRegistry[F](shared.database, shared.streams),
            observedLock,
            common.lakehouseRoot,
            checkpoint,
            new StreamingBatchCallback[F](shared, common, observedJournal, checkpoint, cost, observer).apply,
            () => KafkaOffsetRangeSource.sourceIdentity(common.kafka, settings.topic, shared.sparkExecution),
            maintenance = Some((identity, lineage, retainedCheckpoint, authorize) =>
              new AnalyticsStreamingMaintenance[F](
                common.lakehouseRoot,
                observedLock,
                settings.streaming.maintenanceInterval,
                at => {
                  val maintaining = maintenanceTick.run(identity, lineage, retainedCheckpoint, authorize, at)
                  StreamingBatchCallback.timed(cost, StreamingBatchCost.Stage.Maintenance)(maintaining)
                },
                Some(StreamingBatchCallback.logMaintenance[F])
              ).resource
            )
          )
          shared.publisher.ensurePinnedRetentionIndex.as(stream.resource.use(_ => Async[F].unit))
        }
      } yield program
    }

  def lateFactReplay[F[_]: Async](
      settings: AnalyticsLateFactReplaySettings
  ): Resource[F, F[AnalyticsLateFactReplayOutcome]] =
    withSecureUuids[F, F[AnalyticsLateFactReplayOutcome]] {
      for {
        shared <- shared[F](settings.common, appName = "hiring-analytics-late-replay")
      } yield {
        val common = settings.common
        val journal = new MongoAnalyticsLateFactReplayJournal[F](shared.database, shared.streams, common.lakehouseRoot)
        val stages = new SparkAnalyticsLateFactReplayStages[F](
          shared.spark,
          shared.paths,
          shared.lakehouseExecution,
          shared.reader,
          shared.writer,
          shared.maintenance,
          Clock[F]
        )
        val service = new AnalyticsLateFactReplayService[F](
          common.lakehouseRoot,
          journal,
          shared.markers,
          stages,
          shared.publisher,
          shared.lock,
          common.operational.retention,
          Clock[F]
        )
        journal.ensureIndexes *> service.run(settings.request)
      }
    }

  def keyRetirementAudit[F[_]: Async](
      settings: AnalyticsKeyRetirementAuditSettings
  ): Resource[F, F[Either[NonEmptyChain[String], AnalyticsKeyRetirement.AuditSummary]]] =
    withSecureUuids[F, F[Either[NonEmptyChain[String], AnalyticsKeyRetirement.AuditSummary]]] {
      for {
        paths <- Resource.eval(Async[F].fromEither(resolveLakehousePaths(settings.lakehouseRoot)))
        (spark, client, sparkExecution) <- SparkRuntime.sparkMongo[F](
          settings.mongoUri,
          settings.sparkMaster,
          appName = "hiring-analytics-key-retirement-audit",
          sparkUiEnabled = Some(false)
        )
        database <- Resource.eval(SparkRuntime.mongoDatabase[F](client, settings.mongoDatabase))
      } yield {
        val streams = new MongoPublisherStream(settings.operational)

        Clock[F].realTimeInstant.flatMap(now =>
          AnalyticsKeyRetirement.audit(
            spark,
            paths,
            database.underlying,
            settings.retiringKeyId,
            settings.retention,
            settings.writers,
            now,
            new MongoAnalyticsLakehouseLock[F](database, streams, Clock[F]),
            streams,
            sparkExecution
          )
        )

      }
    }

  def repair[F[_]: Async](settings: AnalyticsWorkerSettings): Resource[F, MongoAnalyticsErasureQueue[F]] =
    withSecureUuids[F, MongoAnalyticsErasureQueue[F]] {
      for {
        client <- SparkRuntime.mongoClient[F](settings.common.mongoUri)
        database <- Resource.eval(SparkRuntime.mongoDatabase[F](client, settings.common.mongoDatabase))
        queue <- MongoAnalyticsErasureQueue.resource(database, new MongoPublisherStream(settings.common.operational))
      } yield queue
    }

  private def shared[F[_]: Async: UUIDGen](
      common: AnalyticsCommonSettings,
      appName: String
  ): Resource[F, Shared[F]] =
    for {
      _ <- Resource.eval(KafkaConnection.preflight[F](common.kafka))
      paths <- Resource.eval(Async[F].fromEither(resolveLakehousePaths(common.lakehouseRoot)))
      (spark, client, sparkExecution) <- SparkRuntime.sparkMongo[F](
        common.mongoUri,
        common.sparkMaster,
        appName,
        sparkLocalDirectory = common.sparkLocalDirectory
      )
      database <- Resource.eval(SparkRuntime.mongoDatabase[F](client, common.mongoDatabase))
    } yield {
      val streams = new MongoPublisherStream(common.operational)
      val lock = new MongoAnalyticsLakehouseLock[F](database, streams, Clock[F])
      val lakehouseExecution = new LakehouseOperation[F](sparkExecution)
      Shared(
        paths,
        spark,
        client,
        database,
        sparkExecution,
        lakehouseExecution,
        streams,
        new MongoActiveDeletionMarkerSource[F](database, common.pseudonymizer, streams),
        lock,
        new DeltaAnalyticsErasureLakehouse[F](
          spark,
          paths,
          common.pseudonymizer,
          lock,
          new MongoHmacKeyRetirementAuthorizationStore[F](database, streams),
          common.operational,
          lakehouseExecution,
          Slf4jLogger.getLogger[F],
          Clock[F]
        ),
        new DeltaBatchWriter[F](paths, lakehouseExecution),
        new DeltaBatchReader[F](lakehouseExecution),
        new MongoAnalyticsReportPublisher[F](client, database, common.operational)
      )
    }

  private def ownedStreamingDirectory[F[_]: Async](baseDirectory: String): Resource[F, AnalyticsNonBlank] = {
    val files = Files.forAsync[F]
    val base = Path(baseDirectory).absolute.normalize
    Resource
      .eval(files.createDirectories(base))
      .flatMap(_ => files.tempDirectory(Some(base), "streaming-", None))
      .evalMap(directory =>
        Async[F].fromEither(
          directory.toString
            .refineEither[Not[Blank]]
            .leftMap(_ => AnalyticsError.InvalidConfiguration("owned Spark directory is invalid"))
        )
      )
  }
}
