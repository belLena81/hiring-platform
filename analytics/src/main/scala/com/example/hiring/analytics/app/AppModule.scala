package com.example.hiring.analytics.app

import com.example.hiring.analytics.adapter.kafka.{KafkaProducerFencer, KafkaRetentionAdapter}
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.config.{AnalyticsBatchSettings, AnalyticsWorkerSettings}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.adapter.spark.AnalyticsErasureWorker
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.service.erasure.*

import cats.effect.{Async, Clock, Resource, Temporal}
import cats.syntax.all.*
import com.mongodb.reactivestreams.client.{MongoClient, MongoClients, MongoDatabase}
import org.apache.spark.sql.SparkSession
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.util.control.NonFatal

/** Resource-managed composition for analytics command-line applications. */
object AppModule {
  final case class BatchProgram[F[_]] private[app] (run: F[AnalyticsPublication])
  final case class WorkerProgram[F[_]] private[app] (run: F[Unit])

  def batch[F[_]: Async: Clock](settings: AnalyticsBatchSettings): Resource[F, BatchProgram[F]] = {
    val common = settings.common
    for {
      (spark, client) <- sparkMongo[F](
        common.mongoUri,
        common.sparkMaster,
        appName = "hiring-analytics-batch"
      )
      database <- Resource.eval(mongoDatabase[F](client, common.mongoDatabase))
    } yield {
      val markers = new MongoActiveDeletionMarkerSource[F](database, common.pseudonymizer)
      val job = new HiringAnalyticsBatch[F](
        common.lakehousePaths,
        common.pseudonymizer,
        markers,
        Clock[F],
        new MongoAnalyticsReportPublisher[F](client, database),
        new DeltaManifestStore[F](common.lakehousePaths),
        new MongoAnalyticsLakehouseLock[F](database, Clock[F]),
        new MongoHmacKeyRetirementAuthorizationStore[F](database),
        Slf4jLogger.getLogger[F]
      )
      BatchProgram(job.run(spark, new KafkaOffsetRangeSource[F](common.kafka), settings.manifest))
    }
  }

  def worker[F[_]: Async: Clock: Temporal](settings: AnalyticsWorkerSettings): Resource[F, WorkerProgram[F]] = {
    val common = settings.common
    for {
      (spark, client) <- sparkMongo[F](
        common.mongoUri,
        common.sparkMaster,
        appName = "hiring-analytics-erasure-worker"
      )
      database <- Resource.eval(mongoDatabase[F](client, common.mongoDatabase))
    } yield {
      val paths = common.lakehousePaths
      val store = new MongoAnalyticsErasureWorkerStore[F](client, database)
      val publisher = new MongoAnalyticsReportPublisher[F](client, database)
      val lock = new MongoAnalyticsLakehouseLock[F](database, Clock[F])
      val markers = new MongoActiveDeletionMarkerSource[F](database, common.pseudonymizer)
      val batch = new HiringAnalyticsBatch[F](
        paths,
        common.pseudonymizer,
        markers,
        Clock[F],
        publisher,
        new DeltaManifestStore[F](paths),
        lock,
        new MongoHmacKeyRetirementAuthorizationStore[F](database),
        Slf4jLogger.getLogger[F]
      )
      val job = new AnalyticsErasureWorker[F](
        spark,
        store,
        store,
        store,
        common.kafka,
        settings.fencerKafka,
        settings.topic,
        paths,
        publisher,
        markers,
        batch,
        lock,
        Clock[F],
        Slf4jLogger.getLogger[F],
        KafkaProducerFencer[F],
        KafkaRetentionAdapter.liveRetention[F]
      )
      WorkerProgram(job.run)
    }
  }

  def repair[F[_]: Async](settings: AnalyticsWorkerSettings): Resource[F, MongoAnalyticsErasureWorkerStore[F]] =
    for {
      client <- mongoClient[F](settings.common.mongoUri)
      database <- Resource.eval(mongoDatabase[F](client, settings.common.mongoDatabase))
    } yield new MongoAnalyticsErasureWorkerStore[F](client, database)

  /** Shared resource boundary for operator diagnostics that need Spark and Mongo without batch services. */
  def sparkMongo[F[_]: Async](
      mongoUri: String,
      sparkMaster: String,
      appName: String,
      sparkUiEnabled: Option[Boolean] = None
  ): Resource[F, (SparkSession, MongoClient)] =
    managedSparkMongo(
      sparkSession[F](sparkMaster, appName, sparkUiEnabled),
      Async[F].delay(MongoClients.create(mongoUri))
    )

  private[analytics] def managedSparkMongo[F[_]: Async](
      acquireSpark: F[SparkSession],
      acquireMongo: F[MongoClient]
  ): Resource[F, (SparkSession, MongoClient)] =
    for {
      spark <- Resource.make(acquireSpark.adaptError { case NonFatal(cause) =>
        AnalyticsError.SparkStartupFailure(cause)
      })(session =>
        Async[F].blocking(session.stop()).adaptError { case NonFatal(cause) =>
          AnalyticsError.LakehouseFailure(cause)
        }
      )
      mongo <- mongoClientFrom[F](acquireMongo)
    } yield (spark, mongo)

  private[analytics] def mongoClient[F[_]: Async](uri: String): Resource[F, MongoClient] =
    mongoClientFrom(Async[F].delay(MongoClients.create(uri)))

  private def mongoClientFrom[F[_]: Async](acquire: F[MongoClient]): Resource[F, MongoClient] =
    Resource.make(
      acquire.adaptError {
        case _: IllegalArgumentException => AnalyticsError.InvalidConfiguration("MONGODB_URI is invalid")
        case NonFatal(cause)             => AnalyticsError.MongoConnectionFailure(cause)
      }
    )(client =>
      Async[F].delay(client.close()).adaptError { case NonFatal(cause) =>
        AnalyticsError.MongoConnectionFailure(cause)
      }
    )

  private def mongoDatabase[F[_]: Async](client: MongoClient, name: String): F[MongoDatabase] =
    Async[F].delay(client.getDatabase(name)).adaptError {
      case _: IllegalArgumentException => AnalyticsError.InvalidConfiguration("analytics.mongo.database is invalid")
      case NonFatal(cause)             => AnalyticsError.MongoConnectionFailure(cause)
    }

  private def sparkSession[F[_]: Async](
      master: String,
      appName: String,
      sparkUiEnabled: Option[Boolean]
  ): F[SparkSession] =
    Async[F].blocking {
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
