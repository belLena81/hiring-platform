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
  AnalyticsLateFactReplaySettings,
  AnalyticsRuntimeDirectory
}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.errors.AnalyticsErrorTranslation.{translating, translatingWith}
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

import scala.util.control.NonFatal

/** Resource-managed composition for analytics command-line applications. */
object AppModule {
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
        val ingestionStage = new AnalyticsBatchIngestionStage[F](
          shared.paths,
          common.pseudonymizer,
          shared.lakehouseExecution,
          manifestStore,
          shared.writer,
          common.operational.retention,
          Clock[F]
        )
        val silverStage = new AnalyticsBatchSilverStage[F](
          shared.paths,
          common.pseudonymizer,
          shared.lakehouseExecution,
          shared.writer,
          shared.reader,
          QuarantineIdentifier,
          common.operational.retention
        )
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
            (rawFrame, batchId, lineage, sourceEndOffsets, authorize) => {
              val callback = for {
                observedAt <- Async[F].realTimeInstant
                _ <- cost.fold(shared.maintenance.validateHmacConfigurationLocked)(
                  _.timed(StreamingBatchCost.Stage.HmacValidation)(shared.maintenance.validateHmacConfigurationLocked)
                )
                parsed <- shared.lakehouseExecution(OperationalEventTransforms.parseKafkaRecords(rawFrame))
                preparing = StreamingBatchPreparation(
                  shared.lakehouseExecution,
                  parsed,
                  lineage,
                  batchId,
                  observedAt,
                  sourceEndOffsets,
                  observedJournal
                )
                preparation <- cost.fold(preparing)(_.timed(StreamingBatchCost.Stage.Prepare)(preparing))
                observedWriter = observer.fold[DeltaWriter[F]](shared.writer)(value =>
                  StreamingDurableBoundaryObserver.writer(shared.writer, shared.paths, preparation.identity, value)
                )
                ingestion = new AnalyticsBatchIngestionStage[F](
                  shared.paths,
                  common.pseudonymizer,
                  shared.lakehouseExecution,
                  new DeltaManifestStore[F](shared.spark, shared.paths, shared.sparkExecution),
                  observedWriter,
                  common.operational.retention,
                  Clock[F]
                )
                silver = new AnalyticsBatchSilverStage[F](
                  shared.paths,
                  common.pseudonymizer,
                  shared.lakehouseExecution,
                  observedWriter,
                  shared.reader,
                  QuarantineIdentifier,
                  common.operational.retention
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
                  .use(stages =>
                    new StreamingBatchCoordinator[F](
                      observer.fold[StreamingBatchJournal[F]](observedJournal)(value =>
                        StreamingDurableBoundaryObserver.journal(observedJournal, value)
                      ),
                      shared.markers,
                      observer.fold[StreamingBatchStages[F]](cost.fold(stages)(_.wrap(stages)))(value =>
                        StreamingDurableBoundaryObserver.stages(cost.fold(stages)(_.wrap(stages)), value)
                      ),
                      checkpoint,
                      authorize
                    )
                      .process(preparation)
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
                  val maintaining = maintenanceTick.run(identity, lineage, retainedCheckpoint, authorize, at)
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
        (spark, client, sparkExecution) <- sparkMongo[F](
          settings.mongoUri,
          settings.sparkMaster,
          appName = "hiring-analytics-key-retirement-audit",
          sparkUiEnabled = Some(false)
        )
        database <- Resource.eval(mongoDatabase[F](client, settings.mongoDatabase))
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
        client <- mongoClient[F](settings.common.mongoUri)
        database <- Resource.eval(mongoDatabase[F](client, settings.common.mongoDatabase))
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
      (spark, client, sparkExecution) <- sparkMongo[F](
        common.mongoUri,
        common.sparkMaster,
        appName,
        sparkLocalDirectory = common.sparkLocalDirectory
      )
      database <- Resource.eval(mongoDatabase[F](client, common.mongoDatabase))
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

  /** Shared resource boundary for operator diagnostics that need Spark and Mongo without batch services. */
  def sparkMongo[F[_]: Async](
      mongoUri: String,
      sparkMaster: String,
      appName: String,
      sparkUiEnabled: Option[Boolean] = None,
      sparkLocalDirectory: String = AnalyticsRuntimeDirectory.container(AnalyticsRuntimeDirectory.SparkTemp).toString
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
          val stop = sparkExecution(session.stop()).translating(AnalyticsError.LakehouseFailure(_))
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
    MongoClient
      .fromConnectionString[F](uri)
      .translatingWith { case _: IllegalArgumentException =>
        AnalyticsError.InvalidConfiguration("MONGODB_URI is invalid")
      }(AnalyticsError.MongoConnectionFailure(_))

  private def mongoDatabase[F[_]: Async](client: MongoClient[F], name: String): F[MongoDatabase[F]] =
    client
      .getDatabase(name)
      .translatingWith { case _: IllegalArgumentException =>
        AnalyticsError.InvalidConfiguration("analytics.mongo.database is invalid")
      }(AnalyticsError.MongoConnectionFailure(_))

  private def sparkSession[F[_]](
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
