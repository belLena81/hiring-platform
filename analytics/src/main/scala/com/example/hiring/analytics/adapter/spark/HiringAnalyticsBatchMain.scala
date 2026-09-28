package com.example.hiring.analytics.adapter.spark
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import com.example.hiring.analytics.*
import com.example.hiring.analytics.adapter.mongo.{MongoAnalyticsLakehouseLock, MongoAnalyticsReportPublisher}

import cats.effect.{ExitCode, IO, Resource}
import cats.syntax.all.*
import com.mongodb.reactivestreams.client.{MongoClient, MongoClients}
import org.apache.spark.sql.SparkSession
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.util.control.NonFatal

/** Bounded batch composition root. Runtime settings and the explicit offset range load from HOCON. */
object HiringAnalyticsBatchMain {
  private val logger = Slf4jLogger.getLogger[IO]

  private[analytics] def managedResources(
      acquireSpark: IO[SparkSession],
      acquireMongo: IO[MongoClient]
  ): Resource[IO, (SparkSession, MongoClient)] =
    for {
      spark <- Resource.make(acquireSpark.adaptError { case NonFatal(cause) =>
        AnalyticsError.SparkStartupFailure(cause)
      })(session =>
        IO.blocking(session.stop()).adaptError { case NonFatal(cause) =>
          AnalyticsError.LakehouseFailure(cause)
        }
      )
      mongo <- Resource.make(acquireMongo.adaptError {
        case _: IllegalArgumentException => AnalyticsError.InvalidConfiguration("MONGODB_URI is invalid")
        case NonFatal(cause)             => AnalyticsError.MongoConnectionFailure(cause)
      })(client =>
        IO.delay(client.close()).adaptError { case NonFatal(cause) =>
          AnalyticsError.MongoConnectionFailure(cause)
        }
      )
    } yield (spark, mongo)

  private[analytics] def resources(mongoUri: String, sparkMaster: String): Resource[IO, (SparkSession, MongoClient)] =
    managedResources(
      IO.blocking(
        org.apache.spark.sql.classic.SparkSession
          .builder()
          .appName("hiring-analytics-batch")
          .master(sparkMaster)
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .getOrCreate()
      ),
      IO.delay(MongoClients.create(mongoUri))
    )

  private def program: IO[AnalyticsPublication] =
    AnalyticsRuntimeConfig.loadBatch.flatMap { configured =>
      val common = configured.common
      resources(common.mongoUri, common.sparkMaster).use { case (spark, mongo) =>
        IO.delay(mongo.getDatabase(common.mongoDatabase))
          .adaptError {
            case _: IllegalArgumentException =>
              AnalyticsError.InvalidConfiguration("analytics.mongo.database is invalid")
            case NonFatal(cause) => AnalyticsError.MongoConnectionFailure(cause)
          }
          .flatMap { database =>
            val markers = new MongoActiveDeletionMarkerSource(database, common.pseudonymizer)
            new HiringAnalyticsBatch(
              common.lakehousePaths,
              common.pseudonymizer,
              markers,
              reportPublisher = Some(new MongoAnalyticsReportPublisher(mongo, database)),
              lakehouseLock = new MongoAnalyticsLakehouseLock(database),
              retirementStore = new MongoHmacKeyRetirementAuthorizationStore(database)
            ).run(spark, new KafkaOffsetRangeSource(common.kafka), configured.manifest)
          }
      }
    }

  def run(args: List[String]): IO[ExitCode] =
    (if (args.nonEmpty)
       IO.raiseError[AnalyticsPublication](
         AnalyticsError.InvalidConfiguration(
           "batch inputs are loaded from HOCON; command-line arguments are not accepted"
         )
       )
     else program).attempt.flatMap {
      case Right(publication)          => logger.info(publication.toString).as(ExitCode.Success)
      case Left(error: AnalyticsError) => logger.error(error.getMessage).as(ExitCode.Error)
      case Left(error)                 =>
        logger.error(s"analytics batch failed unexpectedly: ${error.getClass.getSimpleName}").as(ExitCode.Error)
    }
}
