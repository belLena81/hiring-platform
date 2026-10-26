package com.example.hiring.analytics

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.spark.{
  AnalyticsDeltaRetention,
  SparkBlockingExecution,
  SparkExecution,
  SparkPhysicalLocation
}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import munit.CatsEffectSuite
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.file.Files
import java.util.Comparator
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class AnalyticsDeltaPrivacyPropertiesSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes

  private def withLakehouse(
      body: (SparkSession, SparkExecution[IO], AnalyticsLakehousePaths, AnalyticsDeltaRetention[IO]) => IO[Unit]
  ): IO[Unit] = {
    val temporary = Resource.make(IO.blocking(Files.createTempDirectory("hiring-delta-privacy-properties-")))(root =>
      IO.blocking {
        val entries = Files.walk(root)
        try
          entries.sorted(Comparator.reverseOrder()).forEach { path =>
            val _ = Files.deleteIfExists(path)
          }
        finally entries.close()
      }
    )
    temporary.use { root =>
      SparkBlockingExecution.resource[IO].use { execution =>
        Resource
          .make(execution {
            SparkSession
              .builder()
              .master("local[2]")
              .appName("AnalyticsDeltaPrivacyPropertiesSpec")
              .config("spark.ui.enabled", "false")
              .config("spark.sql.shuffle.partitions", "2")
              .config("spark.databricks.delta.snapshotPartitions", "2")
              .config("spark.databricks.delta.vacuum.retentionWindowIgnore.enabled", "false")
              .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
              .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
              .getOrCreate()
          })(spark => execution.blocking(spark.stop()))
          .use { spark =>
            for {
              _ <- execution.attachSparkContext(spark.sparkContext)
              paths <- IO.fromEither(
                AnalyticsLakehousePaths
                  .from(root.toUri.toString)
                  .toEither
                  .leftMap(errors => new IllegalArgumentException(errors.toString))
              )
              retention = new AnalyticsDeltaRetention[IO](
                paths,
                AnalyticsTestOperationalConfig.operational,
                execution,
                Slf4jLogger.getLogger[IO]
              )
              _ <- body(spark, execution, paths, retention)
            } yield ()
          }
      }
    }
  }

  test("fresh privacy metadata covers the inventory, preserves unrelated properties and repairs later changes") {
    withLakehouse { (spark, execution, paths, retention) =>
      val tables = paths.inventory.delta.map(_.location)
      val subjectTables = paths.inventory.subjectDelta.map(_.location).toSet
      for {
        _ <- execution {
          val shape = StructType(Vector(StructField("controlValue", StringType, nullable = false)))
          tables.foreach { path =>
            spark
              .createDataFrame(Vector(Row("synthetic retained control")).asJava, shape)
              .write
              .format("delta")
              .option("delta.dataSkippingNumIndexedCols", "7")
              .option("delta.checkpointInterval", "100")
              .save(SparkPhysicalLocation.resolve(path))
          }
        }
        _ <- retention.configureRawTablePrivacy(spark)
        versions <- execution {
          tables.map { path =>
            val snapshot = DeltaLog.forTable(spark, SparkPhysicalLocation.resolve(path)).update()
            val properties = snapshot.metadata.configuration
            assertEquals(properties("delta.deletedFileRetentionDuration"), "interval 7 days")
            assertEquals(properties("delta.logRetentionDuration"), "interval 14 days")
            assertEquals(properties("delta.enableDeletionVectors"), "false")
            assertEquals(properties("delta.dataSkippingNumIndexedCols"), if (subjectTables(path)) "0" else "7")
            assertEquals(properties("delta.checkpointInterval"), "100")
            path -> snapshot.version
          }.toMap
        }
        _ <- retention.configureRawTablePrivacy(spark)
        _ <- execution {
          versions.foreach { case (path, version) =>
            assertEquals(DeltaLog.forTable(spark, SparkPhysicalLocation.resolve(path)).update().version, version)
          }
          assertEquals(spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled"), "true")
          val escaped = SparkPhysicalLocation.resolve(paths.bronze).replace("`", "``")
          spark.sql(
            s"ALTER TABLE delta.`$escaped` SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 9 days', 'delta.dataSkippingNumIndexedCols' = '9')"
          )
        }
        _ <- retention.configureRawTablePrivacy(spark)
        _ <- execution {
          val repaired = DeltaLog.forTable(spark, SparkPhysicalLocation.resolve(paths.bronze)).update()
          assertEquals(repaired.metadata.configuration("delta.deletedFileRetentionDuration"), "interval 7 days")
          assertEquals(repaired.metadata.configuration("delta.dataSkippingNumIndexedCols"), "0")
          assertEquals(repaired.metadata.configuration("delta.checkpointInterval"), "100")
          assertEquals(repaired.version, versions(paths.bronze) + 2L)
          tables.filterNot(_ == paths.bronze).foreach { path =>
            assertEquals(
              DeltaLog.forTable(spark, SparkPhysicalLocation.resolve(path)).update().version,
              versions(path)
            )
          }
        }
      } yield ()
    }
  }

  test("AEC-03.1 a table created with a 30 day log retention is reconciled to the configured 14 days") {
    withLakehouse { (spark, execution, paths, retention) =>
      for {
        _ <- execution {
          val shape = StructType(Vector(StructField("controlValue", StringType, nullable = false)))
          spark
            .createDataFrame(Vector(Row("synthetic retained control")).asJava, shape)
            .write
            .format("delta")
            .option("delta.logRetentionDuration", "interval 30 days")
            .save(SparkPhysicalLocation.resolve(paths.bronze))
          val before = DeltaLog.forTable(spark, SparkPhysicalLocation.resolve(paths.bronze)).update()
          assertEquals(before.metadata.configuration("delta.logRetentionDuration"), "interval 30 days")
        }
        _ <- retention.configureRawTablePrivacy(spark)
        _ <- execution {
          val after = DeltaLog.forTable(spark, SparkPhysicalLocation.resolve(paths.bronze)).update()
          assertEquals(after.metadata.configuration("delta.logRetentionDuration"), "interval 14 days")
        }
      } yield ()
    }
  }

  test("AEC-04.1 deletion vectors are pinned off and an enabled table fails closed") {
    withLakehouse { (spark, execution, paths, retention) =>
      for {
        _ <- execution {
          val shape = StructType(Vector(StructField("controlValue", StringType, nullable = false)))
          spark
            .createDataFrame(Vector(Row("synthetic retained control")).asJava, shape)
            .write
            .format("delta")
            .save(SparkPhysicalLocation.resolve(paths.bronze))
        }
        _ <- retention.configureRawTablePrivacy(spark)
        _ <- execution {
          val pinned = DeltaLog.forTable(spark, SparkPhysicalLocation.resolve(paths.bronze)).update()
          assertEquals(pinned.metadata.configuration("delta.enableDeletionVectors"), "false")
          val escaped = SparkPhysicalLocation.resolve(paths.bronze).replace("`", "``")
          spark.sql(s"ALTER TABLE delta.`$escaped` SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
        }
        failure <- retention.configureRawTablePrivacy(spark).attempt
        _ = assertEquals(failure, Left(AnalyticsError.DeltaSchemaMismatch(paths.bronze)))
      } yield ()
    }
  }
}
