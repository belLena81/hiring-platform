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
  AnalyticsWorkerSettings
}
import com.example.hiring.analytics.domain.SubjectPseudonymizer
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.service.erasure.*

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import org.apache.spark.sql.SparkSession
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.util.control.NonFatal

/** Resource-managed composition for analytics command-line applications. */
object AppModule {
  final case class BatchProgram[F[_]] private[app] (run: F[AnalyticsPublication])
  final case class WorkerProgram[F[_]] private[app] (run: F[Unit])

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
      (spark, client, sparkExecution) <- sparkMongo[F](common.mongoUri, common.sparkMaster, appName)
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

  /** Shared resource boundary for operator diagnostics that need Spark and Mongo without batch services. */
  def sparkMongo[F[_]: Async](
      mongoUri: String,
      sparkMaster: String,
      appName: String,
      sparkUiEnabled: Option[Boolean] = None
  ): Resource[F, (SparkSession, MongoClient[F], SparkBlockingExecution[F])] =
    managedSparkMongo(
      execution => sparkSession(sparkMaster, appName, sparkUiEnabled, execution),
      mongoClient[F](mongoUri)
    )

  private[analytics] def managedSparkMongo[F[_]: Async](
      acquireSpark: SparkBlockingExecution[F] => F[SparkSession],
      acquireMongo: Resource[F, MongoClient[F]]
  ): Resource[F, (SparkSession, MongoClient[F], SparkBlockingExecution[F])] =
    for {
      sparkExecution <- SparkBlockingExecution.resource[F]
      spark <- Resource.make(
        acquireSpark(sparkExecution)
          .adaptError { case NonFatal(cause) =>
            AnalyticsError.SparkStartupFailure(cause)
          }
          .flatTap(session => sparkExecution.attachSparkContext(session.sparkContext))
      )(session =>
        sparkExecution(session.stop()).adaptError { case NonFatal(cause) =>
          AnalyticsError.LakehouseFailure(cause)
        }
      )
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
      sparkExecution: SparkExecution[F]
  ): F[SparkSession] =
    sparkExecution {
      val builder = SparkSession
        .builder()
        .appName(appName)
        .master(master)
      sparkUiEnabled.foreach(enabled => builder.config("spark.ui.enabled", enabled.toString))
      builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    }
}
