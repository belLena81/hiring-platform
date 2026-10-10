package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.config.AnalyticsRuntimeDirectory
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.errors.AnalyticsErrorTranslation.{translating, translatingWith}

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import org.apache.spark.sql.SparkSession

import scala.util.control.NonFatal

/** Resource-managed Spark session and Mongo client shared by every analytics program. */
private[analytics] object SparkRuntime {

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

  private[analytics] def mongoDatabase[F[_]: Async](client: MongoClient[F], name: String): F[MongoDatabase[F]] =
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
