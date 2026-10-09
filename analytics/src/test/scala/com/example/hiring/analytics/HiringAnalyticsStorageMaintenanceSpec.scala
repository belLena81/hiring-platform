package com.example.hiring.analytics

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.spark.{AnalyticsDeltaRetention, SparkBlockingExecution}
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import munit.CatsEffectSuite
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.file.{Files, Path}
import java.util.Comparator
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class HiringAnalyticsStorageMaintenanceSpec extends CatsEffectSuite {
  // This checks retained files and versions, not a latency SLO; full-suite Spark jobs contend for local CPU.
  override val munitIOTimeout: FiniteDuration = 15.minutes

  private def dataFiles(root: Path): IO[Set[String]] = IO.blocking {
    val files = Files.walk(root)
    try
      files
        .iterator()
        .asScala
        .filter(path => path.toString.endsWith(".parquet") && !path.toString.contains("_delta_log"))
        .map(_.toString)
        .toSet
    finally files.close()
  }

  test("repeated idle reclamation creates no replacement data files or Delta versions") {
    val temporaryRoot = Resource.make(IO.blocking(Files.createTempDirectory("hiring-storage-maintenance-")))(root =>
      IO.blocking {
        val files = Files.walk(root)
        try
          files.sorted(Comparator.reverseOrder()).forEach { path =>
            val _ = Files.deleteIfExists(path)
          }
        finally files.close()
      }
    )
    temporaryRoot.use { root =>
      SparkBlockingExecution.resource[IO].use { execution =>
        Resource
          .make(execution {
            SparkSession
              .builder()
              .master("local[2]")
              .appName("HiringAnalyticsStorageMaintenanceSpec")
              .config("spark.ui.enabled", "false")
              .config("spark.sql.shuffle.partitions", "2")
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
              tables = Vector(paths.bronze, paths.streamingProgress, paths.hmacKeyRegistry)
              shape = StructType(Vector(StructField("controlValue", StringType, nullable = false)))
              _ <- execution {
                tables.foreach(path =>
                  spark
                    .createDataFrame(Vector(Row("retained business value")).asJava, shape)
                    .write
                    .format("delta")
                    .save(path)
                )
              }
              _ <- retention.configureRawTablePrivacy(spark)
              versions <- execution(tables.map(path => path -> DeltaLog.forTable(spark, path).update().version).toMap)
              before <- dataFiles(root)
              first <- retention.vacuumUnreferencedFiles(spark)
              second <- retention.vacuumUnreferencedFiles(spark)
              after <- dataFiles(root)
              _ <- IO {
                assertEquals(first, 0L)
                assertEquals(second, 0L)
                assertEquals(after, before)
              }
              _ <- execution {
                versions.foreach { case (path, version) =>
                  assertEquals(DeltaLog.forTable(spark, path).update().version, version)
                  assertEquals(spark.read.format("delta").load(path).count(), 1L)
                }
              }
            } yield ()
          }
      }
    }
  }
}
