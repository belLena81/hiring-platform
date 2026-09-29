package com.example.hiring.analytics.app

import com.example.hiring.analytics.adapter.kafka.{KafkaProducerFencer, KafkaRetentionAdapter}
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.config.{AnalyticsBatchSettings, AnalyticsCommonSettings, AnalyticsWorkerSettings}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.adapter.spark.AnalyticsErasureWorker
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.service.erasure.*

import cats.effect.{Async, Clock, Resource}
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
    shared[F](settings.common, appName = "hiring-analytics-batch").map { shared =>
      val common = settings.common
      val job = new HiringAnalyticsBatch[F](
        shared.paths,
        common.pseudonymizer,
        shared.markers,
        Clock[F],
        new MongoAnalyticsReportPublisher[F](shared.client, shared.database, common.operational),
        new DeltaManifestStore[F](shared.paths),
        shared.lock,
        common.operational,
        shared.sparkExecution,
        shared.maintenance
      )
      BatchProgram(
        job.run(
          shared.spark,
          new KafkaOffsetRangeSource[F](common.kafka, shared.sparkExecution),
          settings.manifest
        )
      )
    }

  def worker[F[_]: Async](settings: AnalyticsWorkerSettings): Resource[F, WorkerProgram[F]] =
    shared[F](settings.common, appName = "hiring-analytics-erasure-worker").map { shared =>
      val common = settings.common
      val store = new MongoAnalyticsErasureWorkerStore[F](shared.client, shared.database, shared.streams)
      val publisher = new MongoAnalyticsReportPublisher[F](shared.client, shared.database, common.operational)
      val job = new AnalyticsErasureWorker[F](
        shared.spark,
        store,
        store,
        store,
        common.kafka,
        settings.fencerKafka,
        settings.topic,
        shared.paths,
        publisher,
        shared.markers,
        shared.maintenance,
        shared.lock,
        Clock[F],
        Slf4jLogger.getLogger[F],
        KafkaProducerFencer[F],
        KafkaRetentionAdapter.liveRetention[F],
        common.operational.retention
      )
      WorkerProgram(job.run)
    }

  def repair[F[_]: Async](settings: AnalyticsWorkerSettings): Resource[F, MongoAnalyticsErasureWorkerStore[F]] =
    for {
      client <- mongoClient[F](settings.common.mongoUri)
      database <- Resource.eval(mongoDatabase[F](client, settings.common.mongoDatabase))
    } yield new MongoAnalyticsErasureWorkerStore[F](
      client,
      database,
      new MongoPublisherStream(settings.common.operational)
    )

  private def shared[F[_]: Async](
      common: AnalyticsCommonSettings,
      appName: String
  ): Resource[F, Shared[F]] =
    for {
      paths <- Resource.eval(Async[F].fromEither(resolveLakehousePaths(common.lakehouseRoot)))
      (spark, client, sparkExecution) <- sparkMongo[F](common.mongoUri, common.sparkMaster, appName)
      database <- Resource.eval(mongoDatabase[F](client, common.mongoDatabase))
    } yield {
      val streams = new MongoPublisherStream(common.operational)
      val lock = new MongoAnalyticsLakehouseLock[F](database, Clock[F], streams)
      Shared(
        paths,
        spark,
        client,
        database,
        sparkExecution,
        streams,
        new MongoActiveDeletionMarkerSource[F](database, common.pseudonymizer, streams, sparkExecution),
        lock,
        new DeltaAnalyticsErasureLakehouse[F](
          paths,
          common.pseudonymizer,
          Clock[F],
          lock,
          new MongoHmacKeyRetirementAuthorizationStore[F](database, streams),
          common.operational,
          sparkExecution,
          Slf4jLogger.getLogger[F]
        )
      )
    }

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
      spark <- Resource.make(acquireSpark(sparkExecution).adaptError { case NonFatal(cause) =>
        AnalyticsError.SparkStartupFailure(cause)
      })(session =>
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
      val builder = org.apache.spark.sql.classic.SparkSession
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
