package com.example.hiring.analytics.app

import com.example.hiring.analytics.adapter.kafka.{KafkaProducerFencer, KafkaRetentionAdapter}
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.service.batch.HiringAnalyticsBatch
import com.example.hiring.analytics.service.erasure.AnalyticsErasureWorker
import com.example.hiring.analytics.config.{
  AnalyticsBatchSettings,
  AnalyticsCommonSettings,
  AnalyticsErasureWorkerPolicy,
  AnalyticsWorkerSettings,
  AnalyticsStreamingRuntimeSettings,
  AnalyticsNonBlank,
  AnalyticsLateFactReplaySettings
}
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.service.erasure.*
import com.example.hiring.analytics.service.streaming.*

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import org.apache.spark.sql.SparkSession
import org.typelevel.log4cats.slf4j.Slf4jLogger
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.Not
import io.github.iltotore.iron.constraint.string.Blank

import scala.util.control.NonFatal
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.Comparator

/** Resource-managed composition for analytics command-line applications. */
object AppModule {
  final case class BatchProgram[F[_]] private[app] (run: F[AnalyticsPublication])
  final case class WorkerProgram[F[_]] private[app] (run: F[Unit])
  final case class StreamingProgram[F[_]] private[app] (run: F[Unit])
  final case class ReplayProgram[F[_]] private[app] (run: F[AnalyticsLateFactReplayOutcome])

  private final case class Shared[F[_]](
      paths: AnalyticsLakehousePaths,
      spark: SparkSession,
      client: MongoClient[F],
      database: MongoDatabase[F],
      sparkExecution: SparkBlockingExecution[F],
      lakehouseExecution: LakehouseOperation[F],
      streams: MongoPublisherStream,
      markers: MongoActiveDeletionMarkerSource[F],
      lock: MongoAnalyticsLakehouseLock[F],
      maintenance: DeltaAnalyticsErasureLakehouse[F]
  )

  private[analytics] def resolveLakehousePaths(root: String): Either[AnalyticsError, AnalyticsLakehousePaths] =
    AnalyticsLakehousePaths
      .from(root)
      .toEither
      .leftMap(errors => AnalyticsError.InvalidConfiguration(errors.toNonEmptyList.toList.mkString("; ")))

  def batch[F[_]: Async](settings: AnalyticsBatchSettings): Resource[F, BatchProgram[F]] =
    for {
      pseudonymizer <- Resource.eval(buildPseudonymizer[F](settings.common))
      shared <- shared[F](settings.common, pseudonymizer, appName = "hiring-analytics-batch")
    } yield {
      val common = settings.common
      val manifestStore = new DeltaManifestStore[F](shared.spark, shared.paths, shared.sparkExecution)
      val deltaWriter = new DeltaBatchWriter[F](shared.paths, shared.lakehouseExecution)
      val deltaReader = new DeltaBatchReader[F](shared.lakehouseExecution)
      val ingestionStage = new AnalyticsBatchIngestionStage[F](
        shared.paths,
        pseudonymizer,
        shared.lakehouseExecution,
        manifestStore,
        deltaWriter,
        common.operational.retention
      )
      val silverStage = new AnalyticsBatchSilverStage[F](
        shared.paths,
        pseudonymizer,
        shared.lakehouseExecution,
        deltaWriter,
        deltaReader,
        QuarantineIdentifier,
        common.operational.retention
      )
      val job = new HiringAnalyticsBatch[F](
        shared.paths,
        shared.markers,
        new MongoAnalyticsReportPublisher[F](shared.client, shared.database, common.operational),
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
        common.operational
      )
      BatchProgram(
        job.run(settings.manifest)
      )
    }

  def worker[F[_]: Async](settings: AnalyticsWorkerSettings): Resource[F, WorkerProgram[F]] =
    for {
      pseudonymizer <- Resource.eval(buildPseudonymizer[F](settings.common))
      shared <- shared[F](settings.common, pseudonymizer, appName = "hiring-analytics-erasure-worker")
      program <- MongoAnalyticsErasureStores.resource(shared.client, shared.database, shared.streams).map { stores =>
        val common = settings.common
        val publisher = new MongoAnalyticsReportPublisher[F](shared.client, shared.database, common.operational)
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
          publisher,
          shared.markers,
          shared.maintenance,
          shared.lock,
          Slf4jLogger.getLogger[F],
          AnalyticsErasureWorkerPolicy(
            common.operational.retention,
            common.operational.erasureWorkerTimings
          )
        )
        WorkerProgram(job.run)
      }
    } yield program

  /** Long-lived, opt-in Structured Streaming runtime. The Mongo activation gate is read-only and fail-closed. */
  def streaming[F[_]: Async](settings: AnalyticsStreamingRuntimeSettings): Resource[F, StreamingProgram[F]] =
    streamingObserved(settings, None)

  /** Adapter-private observation seam: callbacks execute only after durable operations return. */
  private[analytics] def streamingObserved[F[_]: Async](
      settings: AnalyticsStreamingRuntimeSettings,
      observer: Option[StreamingDurableBoundaryObserver[F]],
      costObserver: Option[StreamingBatchCost.Summary => F[Unit]] = None
  ): Resource[F, StreamingProgram[F]] =
    for {
      pseudonymizer <- Resource.eval(buildPseudonymizer[F](settings.common))
      localDirectory <- ownedStreamingDirectory[F](settings.common.sparkLocalDirectory)
      common = settings.common.copy(sparkLocalDirectory = localDirectory)
      shared <- shared[F](common, pseudonymizer, appName = "hiring-analytics-streaming")
      streamingLock <- AnalyticsLakehouseLock.serialized(shared.lock)
      cost <- costObserver.traverse(emit => StreamingBatchCost.resource(shared.spark, emit))
      program <- Resource.eval {
        val journal = new DeltaStreamingBatchJournal[F](shared.spark, shared.paths, shared.sparkExecution)
        val observedJournal = cost.fold[StreamingBatchJournal[F]](journal)(_.wrapJournal(journal))
        val checkpoint = new DeltaStreamingCheckpointAcknowledgement[F](observedJournal)
        val observedLock = cost.fold[AnalyticsLakehouseLock[F]](streamingLock)(_.wrapLock(streamingLock))
        val writer = new DeltaBatchWriter[F](shared.paths, shared.lakehouseExecution)
        val reader = new DeltaBatchReader[F](shared.lakehouseExecution)
        val publisher = new MongoAnalyticsReportPublisher[F](shared.client, shared.database, common.operational)
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
          (rawFrame, batchId, lineage, sourceEndOffsets, authorize) => {
            val callback = for {
              observedAt <- Async[F].realTimeInstant
              _ <- cost.fold(shared.maintenance.validateHmacConfigurationLocked)(
                _.timed(StreamingBatchCost.Stage.HmacValidation)(shared.maintenance.validateHmacConfigurationLocked)
              )
              parsed <- shared.lakehouseExecution(OperationalEventTransforms.parseKafkaRecords(rawFrame))
              preparing = prepareStreamingBatch(
                shared,
                parsed,
                lineage,
                batchId,
                observedAt,
                sourceEndOffsets,
                observedJournal
              )
              preparation <- cost.fold(preparing)(_.timed(StreamingBatchCost.Stage.Prepare)(preparing))
              observedWriter = observer.fold[DeltaWriter[F]](writer)(value =>
                StreamingDurableBoundaryObserver.writer(writer, shared.paths, preparation.identity, value)
              )
              ingestion = new AnalyticsBatchIngestionStage[F](
                shared.paths,
                pseudonymizer,
                shared.lakehouseExecution,
                new DeltaManifestStore[F](shared.spark, shared.paths, shared.sparkExecution),
                observedWriter,
                common.operational.retention
              )
              silver = new AnalyticsBatchSilverStage[F](
                shared.paths,
                pseudonymizer,
                shared.lakehouseExecution,
                observedWriter,
                reader,
                QuarantineIdentifier,
                common.operational.retention
              )
              _ <- SparkStreamingBatchStages
                .resource(
                  shared.spark,
                  parsed,
                  shared.paths,
                  pseudonymizer,
                  shared.lakehouseExecution,
                  ingestion,
                  silver,
                  new AnalyticsLateFactStage[F](shared.paths, shared.lakehouseExecution, observedWriter),
                  observedWriter,
                  reader,
                  publisher,
                  common.operational.retention,
                  shared.maintenance.configureRawTables,
                  shared.maintenance.purgeMarkedSubjects,
                  authorize
                )
                .use(stages =>
                  new StreamingBatchCoordinator[F](
                    observer.fold[StreamingBatchJournal[F]](observedJournal)(value =>
                      StreamingDurableBoundaryObserver.journal(observedJournal, value)
                    ),
                    shared.markers,
                    observer.fold[StreamingBatchStages[F]](cost.fold(stages)(_.wrap(stages)))(value =>
                      StreamingDurableBoundaryObserver.stages(cost.fold(stages)(_.wrap(stages)), value)
                    ),
                    checkpoint
                  )
                    .process(preparation, authorize)
                    .void
                )
            } yield ()
            cost.fold(callback)(_.timed(StreamingBatchCost.Stage.Callback)(callback))
          },
          () => KafkaOffsetRangeSource.sourceIdentity(common.kafka, settings.topic, shared.sparkExecution),
          maintenance = Some((identity, lineage, retainedCheckpoint, authorize) =>
            new AnalyticsStreamingMaintenance[F](
              common.lakehouseRoot,
              observedLock,
              settings.streaming.maintenanceInterval,
              at => {
                val maintaining = maintainStreaming(
                  shared,
                  common,
                  settings,
                  publisher,
                  journal,
                  identity,
                  lineage,
                  retainedCheckpoint,
                  authorize,
                  at
                )
                cost.fold(maintaining)(_.timed(StreamingBatchCost.Stage.Maintenance)(maintaining))
              },
              Some(observation =>
                Slf4jLogger
                  .getLoggerFromName[F]("com.example.hiring.analytics.streaming.maintenance")
                  .info(
                    s"STREAMING_MAINTENANCE outcome=${observation.outcome.fold("none")(_.toString)} " +
                      s"elapsedSinceSuccessMillis=${observation.elapsedSinceSuccess.toMillis} " +
                      s"maximumElapsedSinceSuccessMillis=${observation.maximumElapsedSinceSuccess.toMillis} " +
                      s"consecutiveDeferrals=${observation.consecutiveDeferrals} " +
                      s"lastSuccess=${observation.lastSuccess.fold("none")(_.toString)}"
                  )
              )
            ).resource
          )
        )
        publisher.ensurePinnedRetentionIndex.as(StreamingProgram(stream.resource.use(_ => Async[F].unit)))
      }
    } yield program

  def lateFactReplay[F[_]: Async](settings: AnalyticsLateFactReplaySettings): Resource[F, ReplayProgram[F]] =
    for {
      pseudonymizer <- Resource.eval(buildPseudonymizer[F](settings.common))
      shared <- shared[F](settings.common, pseudonymizer, appName = "hiring-analytics-late-replay")
    } yield {
      val common = settings.common
      val journal = new MongoAnalyticsLateFactReplayJournal[F](shared.database, shared.streams, common.lakehouseRoot)
      val stages = new SparkAnalyticsLateFactReplayStages[F](
        shared.spark,
        shared.paths,
        shared.lakehouseExecution,
        new DeltaBatchReader[F](shared.lakehouseExecution),
        new DeltaBatchWriter[F](shared.paths, shared.lakehouseExecution),
        shared.maintenance
      )
      val publisher = new MongoAnalyticsReportPublisher[F](shared.client, shared.database, common.operational)
      val service = new AnalyticsLateFactReplayService[F](
        common.lakehouseRoot,
        journal,
        shared.markers,
        stages,
        publisher,
        shared.lock,
        common.operational.retention.publishedSnapshotDays
      )
      ReplayProgram(journal.ensureIndexes *> service.run(settings.request))
    }

  def repair[F[_]: Async](settings: AnalyticsWorkerSettings): Resource[F, MongoAnalyticsErasureQueue[F]] =
    for {
      client <- mongoClient[F](settings.common.mongoUri)
      database <- Resource.eval(mongoDatabase[F](client, settings.common.mongoDatabase))
      queue <- MongoAnalyticsErasureQueue.resource(database, new MongoPublisherStream(settings.common.operational))
    } yield queue

  private def shared[F[_]: Async](
      common: AnalyticsCommonSettings,
      pseudonymizer: SubjectPseudonymizer,
      appName: String
  ): Resource[F, Shared[F]] =
    for {
      paths <- Resource.eval(Async[F].fromEither(resolveLakehousePaths(common.lakehouseRoot)))
      (spark, client, sparkExecution) <- sparkMongo[F](
        common.mongoUri,
        common.sparkMaster,
        appName,
        sparkLocalDirectory = common.sparkLocalDirectory
      )
      database <- Resource.eval(mongoDatabase[F](client, common.mongoDatabase))
    } yield {
      val streams = new MongoPublisherStream(common.operational)
      val lock = new MongoAnalyticsLakehouseLock[F](database, streams)
      val lakehouseExecution = new LakehouseOperation[F](sparkExecution)
      Shared(
        paths,
        spark,
        client,
        database,
        sparkExecution,
        lakehouseExecution,
        streams,
        new MongoActiveDeletionMarkerSource[F](database, pseudonymizer, streams),
        lock,
        new DeltaAnalyticsErasureLakehouse[F](
          spark,
          paths,
          pseudonymizer,
          lock,
          new MongoHmacKeyRetirementAuthorizationStore[F](database, streams),
          common.operational,
          lakehouseExecution,
          Slf4jLogger.getLogger[F]
        )
      )
    }

  private def buildPseudonymizer[F[_]: Async](common: AnalyticsCommonSettings): F[SubjectPseudonymizer] =
    Async[F].fromEither(
      SubjectPseudonymizer
        .validateFromBase64(
          Some(common.hmac.secretBase64),
          common.hmac.keyId,
          common.hmac.previousKeyId,
          common.hmac.previousSecretBase64
        )
        .toEither
        .leftMap(errors => AnalyticsError.InvalidConfiguration(errors.toChain.toList.mkString("; ")))
    )

  private def maintainStreaming[F[_]: Async](
      shared: Shared[F],
      common: AnalyticsCommonSettings,
      settings: AnalyticsStreamingRuntimeSettings,
      publisher: MongoAnalyticsReportPublisher[F],
      journal: DeltaStreamingBatchJournal[F],
      identity: StreamingActivationIdentity,
      lineage: StreamingLineage,
      retainedCheckpoint: () => F[Vector[StreamingCheckpointBatch]],
      authorize: F[Unit],
      at: java.time.Instant
  ): F[Unit] =
    for {
      _ <- authorize
      _ <- shared.maintenance.validateHmacConfigurationLocked
      _ <- shared.maintenance.configureRawTables
      runId <- Async[F]
        .delay(java.util.UUID.randomUUID().toString)
        .flatMap(value =>
          Async[F]
            .fromEither(RunId.from(s"stream-maintenance-$value").leftMap(AnalyticsError.InvalidConfiguration.apply))
        )
      fingerprint <- Async[F].fromEither(
        RangeFingerprint
          .from(AnalyticsDigest.sha256Hex(s"${identity.canonical}\n$at".getBytes(StandardCharsets.UTF_8)))
          .leftMap(AnalyticsError.InvalidConfiguration.apply)
      )
      existingMarkers <- shared.markers.activeSubjectTokens
      reservation <-
        if (existingMarkers.nonEmpty) Async[F].pure(Option.empty[AnalyticsReportReservation])
        else publisher.reservePinned(runId, fingerprint, at).map(Some(_))
      markers <- shared.markers.activeSubjectTokens
      _ <- shared.maintenance.expireStored(at)
      _ <- if (markers.nonEmpty) shared.maintenance.purgeMarkedSubjects(markers) else Async[F].unit
      _ <-
        if (markers.nonEmpty || reservation.isEmpty) Async[F].unit
        else
          for {
            reserved <- Async[F].fromOption(
              reservation,
              AnalyticsError.InvalidConfiguration("maintenance reservation is unavailable")
            )
            report <- shared.maintenance.rebuildGoldAndExtractReport(at)
            currentMarkers <- shared.markers.activeSubjectTokens
            _ <- authorize
            _ <-
              if (currentMarkers.nonEmpty) Async[F].unit
              else
                publisher
                  .publish(
                    reserved,
                    report,
                    at.plusSeconds(common.operational.retention.publishedSnapshotDays.asInstanceOf[Int].toLong * 86400L)
                  )
                  .handleErrorWith {
                    case AnalyticsError.GuardedErasurePublicationRejected => Async[F].unit
                    case error                                            => Async[F].raiseError[Unit](error)
                  }
          } yield ()
      retained <- retainedCheckpoint()
      _ <- journal.prune(lineage, retained.map(_.batchId).toSet, at, settings.streaming.progressRetention)
      replayJournal = new MongoAnalyticsLateFactReplayJournal[F](shared.database, shared.streams, common.lakehouseRoot)
      _ <- replayJournal.ensureIndexes
      _ <- replayJournal.compactCompleted(at)
      streamReceiptIds <- journal.allRetainedPublicationRunIds
      replayReceiptIds <- replayJournal.activePublicationRunIds
      _ <- publisher.compactPublished(streamReceiptIds ++ replayReceiptIds, at)
      _ <- shared.maintenance.reclaimExpiredFiles
      _ <- authorize
    } yield ()

  private def prepareStreamingBatch[F[_]: Async](
      shared: Shared[F],
      parsed: org.apache.spark.sql.DataFrame,
      lineage: StreamingLineage,
      batchId: StreamingBatchId,
      observedAt: java.time.Instant,
      sourceEndOffsets: Map[(String, Int), Long],
      journal: StreamingBatchJournal[F]
  ): F[StreamingInputPreparation] = {
    import org.apache.spark.sql.functions.{col, sha2}
    val fingerprintAndOffsets = shared.lakehouseExecution.either {
      val rows = parsed
        .withColumn("_fingerprint", sha2(col(Columns.RawValue), 256))
        .select(col(Columns.Topic), col(Columns.Partition), col(Columns.Offset), col("_fingerprint"))
        .collect()
        .toVector
      val canonical = rows
        .map(row => s"${row.getString(0)}:${row.getInt(1)}:${row.getLong(2)}:${row.getString(3)}")
        .sorted
        .mkString("\n")
      val offsets: Either[AnalyticsError, Vector[StreamingPartitionSummary]] = rows
        .groupBy(row => (row.getString(0), row.getInt(1)))
        .toVector
        .sortBy { case ((topic, partition), _) => (topic, partition) }
        .traverse { case ((topic, partition), partitionRows) =>
          StreamingPartitionSummary
            .from(
              topic,
              partition,
              partitionRows.map(_.getLong(2)).min,
              partitionRows.map(_.getLong(2)).max,
              partitionRows.size.toLong
            )
            .toEither
            .left
            .map(errors => AnalyticsError.InvalidInput(errors))
        }
      for {
        safeOffsets <- offsets
        fingerprint <- RangeFingerprint
          .from(AnalyticsDigest.sha256Hex(canonical.getBytes(StandardCharsets.UTF_8)))
          .leftMap(problem => AnalyticsError.InvalidInput(cats.data.NonEmptyChain.one(problem)))
        safeEndOffsets <- sourceEndOffsets.toVector
          .sortBy(_._1)
          .traverse { case ((topic, partition), offset) =>
            StreamingPartitionEndOffset
              .from(topic, partition, offset)
              .toEither
              .left
              .map(errors => AnalyticsError.InvalidInput(errors))
          }
      } yield (fingerprint, safeOffsets, safeEndOffsets)
    }
    fingerprintAndOffsets.flatMap { case (fingerprint, offsets, safeEndOffsets) =>
      journal.latestWatermark(lineage).map { watermark =>
        StreamingInputPreparation(
          StreamingBatchIdentity(lineage, batchId),
          observedAt,
          watermark,
          fingerprint,
          safeEndOffsets,
          offsets
        )
      }
    }
  }

  private def ownedStreamingDirectory[F[_]: Async](baseDirectory: String): Resource[F, AnalyticsNonBlank] =
    Resource
      .make(
        Async[F].blocking {
          val base = Paths.get(baseDirectory).toAbsolutePath.normalize()
          val owned = base.resolve(s"streaming-${java.util.UUID.randomUUID()}")
          Files.createDirectories(owned)
          owned.toString
        }
      )(directory =>
        Async[F].blocking {
          val root = Paths.get(directory)
          if (Files.exists(root)) {
            val paths = Files.walk(root)
            try paths.sorted(Comparator.reverseOrder()).forEach(path => Files.deleteIfExists(path))
            finally paths.close()
          }
        }.void
      )
      .evalMap(directory =>
        Async[F].fromEither(
          directory
            .refineEither[Not[Blank]]
            .leftMap(_ => AnalyticsError.InvalidConfiguration("owned Spark directory is invalid"))
        )
      )

  /** Shared resource boundary for operator diagnostics that need Spark and Mongo without batch services. */
  def sparkMongo[F[_]: Async](
      mongoUri: String,
      sparkMaster: String,
      appName: String,
      sparkUiEnabled: Option[Boolean] = None,
      sparkLocalDirectory: String = "/var/lib/hiring-analytics/spark-temp"
  ): Resource[F, (SparkSession, MongoClient[F], SparkBlockingExecution[F])] =
    managedSparkMongo(
      execution => sparkSession(sparkMaster, appName, sparkUiEnabled, sparkLocalDirectory, execution),
      mongoClient[F](mongoUri)
    )

  private[analytics] def managedSparkMongo[F[_]: Async](
      acquireSpark: SparkBlockingExecution[F] => F[SparkSession],
      acquireMongo: Resource[F, MongoClient[F]]
  ): Resource[F, (SparkSession, MongoClient[F], SparkBlockingExecution[F])] =
    for {
      sparkExecution <- SparkBlockingExecution.resource[F]
      shutdown <- Resource.eval(Async[F].ref(Option.empty[SparkShutdownDrain]))
      spark <- Resource.make(
        acquireSpark(sparkExecution)
          .adaptError { case NonFatal(cause) =>
            AnalyticsError.SparkStartupFailure(cause)
          }
      )(session =>
        shutdown.get.flatMap { owned =>
          val stop = sparkExecution(session.stop()).adaptError {
            case error: AnalyticsError => error
            case NonFatal(cause)       => AnalyticsError.LakehouseFailure(cause)
          }
          owned.fold(stop)(drain =>
            SparkShutdownDrain.close(
              drain.await(sparkExecution),
              stop,
              sparkExecution.blocking(drain.remove())
            )
          )
        }
      )
      _ <- Resource.eval(Async[F].uncancelable { _ =>
        sparkExecution.attachSparkContext(spark.sparkContext) *>
          sparkExecution
            .blocking(SparkShutdownDrain.install(spark.sparkContext))
            .flatMap(drain => shutdown.set(Some(drain)))
      })
      mongo <- acquireMongo
    } yield (spark, mongo, sparkExecution)

  private[analytics] def mongoClient[F[_]: Async](uri: String): Resource[F, MongoClient[F]] =
    MongoClient.fromConnectionString[F](uri).handleErrorWith {
      case _: IllegalArgumentException =>
        Resource.eval(
          Async[F].raiseError[MongoClient[F]](AnalyticsError.InvalidConfiguration("MONGODB_URI is invalid"))
        )
      case NonFatal(cause) =>
        Resource.eval(Async[F].raiseError[MongoClient[F]](AnalyticsError.MongoConnectionFailure(cause)))
    }

  private def mongoDatabase[F[_]: Async](client: MongoClient[F], name: String): F[MongoDatabase[F]] =
    client.getDatabase(name).adaptError {
      case _: IllegalArgumentException => AnalyticsError.InvalidConfiguration("analytics.mongo.database is invalid")
      case NonFatal(cause)             => AnalyticsError.MongoConnectionFailure(cause)
    }

  private def sparkSession[F[_]: Async](
      master: String,
      appName: String,
      sparkUiEnabled: Option[Boolean],
      sparkLocalDirectory: String,
      sparkExecution: SparkExecution[F]
  ): F[SparkSession] =
    sparkExecution {
      val builder = SparkSession
        .builder()
        .appName(appName)
        .master(master)
        .config("spark.local.dir", sparkLocalDirectory)
        .config("spark.sql.session.timeZone", "UTC")
      sparkUiEnabled.foreach(enabled => builder.config("spark.ui.enabled", enabled.toString))
      builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    }
}
