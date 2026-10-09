package com.example.hiring.analytics

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.adapter.spark.{
  AnalyticsDeltaRetention,
  DeltaVacuumEligibility,
  SparkBlockingExecution,
  SparkPhysicalLocation
}
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import io.delta.tables.DeltaTable
import munit.CatsEffectSuite
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations}
import org.apache.spark.sql.delta.actions.RemoveFile
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path, StandardOpenOption}
import java.nio.file.attribute.FileTime
import java.util.Comparator
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class DeltaVacuumEligibilitySpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes
  private val safetyHours = 168d
  private val maximumEntries = 10000

  private def withTable(
      check: (SparkSession, SparkBlockingExecution[IO], AnalyticsLakehousePaths, Path) => IO[Unit]
  ): IO[Unit] = {
    val temporary = Resource.make(IO.blocking(Files.createTempDirectory("hiring-vacuum-eligibility-")))(root =>
      IO.blocking {
        val files = Files.walk(root)
        try
          files.sorted(Comparator.reverseOrder()).forEach { path =>
            val _ = Files.deleteIfExists(path)
          }
        finally files.close()
      }
    )
    temporary.use { root =>
      SparkBlockingExecution.resource[IO].use { execution =>
        Resource
          .make(execution {
            SparkSession
              .builder()
              .master("local[2]")
              .appName("DeltaVacuumEligibilitySpec")
              .config("spark.ui.enabled", "false")
              .config("spark.sql.shuffle.partitions", "2")
              .config("spark.databricks.delta.snapshotPartitions", "2")
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
              _ <- execution {
                spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
                val shape = StructType(Vector(StructField("value", StringType, nullable = false)))
                spark
                  .createDataFrame(Vector(Row("synthetic retained control")).asJava, shape)
                  .write
                  .format("delta")
                  .save(SparkPhysicalLocation.resolve(paths.bronze))
              }
              _ <- check(spark, execution, paths, Path.of(java.net.URI.create(paths.bronze)))
            } yield ()
          }
      }
    }
  }

  test("retention uses native whole-hour rounding and unsupported numbers stay on the native path") {
    assertEquals(DeltaVacuumEligibility.roundedRetentionMillis(0.49d), Some(0L))
    assertEquals(DeltaVacuumEligibility.roundedRetentionMillis(0.5d), Some(TimeUnit.HOURS.toMillis(1L)))
    assertEquals(DeltaVacuumEligibility.roundedRetentionMillis(167.5d), Some(TimeUnit.HOURS.toMillis(168L)))
    Vector(-1d, -0.1d, Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).foreach { hours =>
      assertEquals(DeltaVacuumEligibility.roundedRetentionMillis(hours), None)
    }
  }

  test("native cutoff equality is retained and a file one millisecond older requires native vacuum") {
    assert(DeltaVacuumEligibility.fileIsFresh(1000L, 1000L))
    assert(DeltaVacuumEligibility.fileIsFresh(1001L, 1000L))
    assert(!DeltaVacuumEligibility.fileIsFresh(999L, 1000L))
  }

  test("fresh flat files and young orphans avoid native work without changing Delta versions") {
    withTable { (spark, execution, paths, table) =>
      val retention = new AnalyticsDeltaRetention[IO](
        paths,
        AnalyticsTestOperationalConfig.operational,
        execution,
        Slf4jLogger.getLogger[IO]
      )
      for {
        version <- execution(DeltaLog.forTable(spark, paths.bronze).update().version)
        _ <- execution {
          Files.writeString(table.resolve("young-orphan.parquet"), "synthetic orphan", UTF_8)
          assert(DeltaVacuumEligibility.canSkip(spark, paths.bronze, safetyHours, maximumEntries))
          assert(!DeltaVacuumEligibility.canSkip(spark, paths.bronze, safetyHours, 1))
        }
        _ <- Resource
          .make(execution {
            val jobs = new AtomicInteger(0)
            val listener = new SparkListener {
              override def onJobStart(event: SparkListenerJobStart): Unit = { jobs.incrementAndGet(); () }
            }
            spark.sparkContext.addSparkListener(listener)
            (listener, jobs)
          }) { case (listener, _) => execution(spark.sparkContext.removeSparkListener(listener)) }
          .use { case (_, jobs) =>
            for {
              _ <- IO.sleep(200.millis)
              _ <- IO.delay(jobs.set(0))
              first <- retention.vacuumUnreferencedFiles(spark)
              second <- retention.vacuumUnreferencedFiles(spark)
              _ <- IO.sleep(200.millis)
              _ <- IO {
                assertEquals(first, 0L)
                assertEquals(second, 0L)
                assertEquals(jobs.get(), 0, "fresh flat no-op reclamation must submit no Spark jobs")
              }
            } yield ()
          }
        _ <- execution {
          assertEquals(DeltaLog.forTable(spark, paths.bronze).update().version, version)
          assert(Files.exists(table.resolve("young-orphan.parquet")))
        }
      } yield ()
    }
  }

  test("old tracked files and freshly discovered old orphans fall back and native vacuum preserves live data") {
    withTable { (spark, execution, paths, table) =>
      val retention = new AnalyticsDeltaRetention[IO](
        paths,
        AnalyticsTestOperationalConfig.operational,
        execution,
        Slf4jLogger.getLogger[IO]
      )
      for {
        tracked <- execution {
          val entries = Files.list(table)
          try
            entries
              .iterator()
              .asScala
              .find(_.getFileName.toString.endsWith(".parquet"))
              .getOrElse(throw new AssertionError("tracked data file absent"))
          finally entries.close()
        }
        _ <- execution {
          val old = FileTime.fromMillis(System.currentTimeMillis() - 8.days.toMillis)
          Files.setLastModifiedTime(tracked, old)
          assert(!DeltaVacuumEligibility.canSkip(spark, paths.bronze, safetyHours, maximumEntries))
          Files.writeString(table.resolve("old-orphan.parquet"), "synthetic orphan", UTF_8)
          Files.setLastModifiedTime(table.resolve("old-orphan.parquet"), old)
          assert(!DeltaVacuumEligibility.canSkip(spark, paths.bronze, safetyHours, maximumEntries))
        }
        _ <- retention.vacuumUnreferencedFiles(spark)
        _ <- execution {
          assert(!Files.exists(table.resolve("old-orphan.parquet")))
          assert(Files.exists(tracked))
          assertEquals(spark.read.format("delta").load(paths.bronze).count(), 1L)
        }
      } yield ()
    }
  }

  test("a directory larger than the probe bound falls back without changing its files") {
    withTable { (spark, execution, paths, table) =>
      execution {
        (1 to 128).foreach(index => Files.writeString(table.resolve(s"young-control-$index"), "synthetic", UTF_8))
        (1 to 16).foreach(_ => assert(!DeltaVacuumEligibility.canSkip(spark, paths.bronze, safetyHours, 4)))
        assert(DeltaVacuumEligibility.canSkip(spark, paths.bronze, safetyHours, maximumEntries))
        assert((1 to 128).forall(index => Files.exists(table.resolve(s"young-control-$index"))))
      }
    }
  }

  test("directories, links and a linked table root never use the no-delete shortcut") {
    withTable { (spark, execution, paths, table) =>
      execution {
        val directory = Files.createDirectory(table.resolve("fresh-empty-directory"))
        assert(!DeltaVacuumEligibility.canSkip(spark, paths.bronze, safetyHours, maximumEntries))
        Files.delete(directory)
        val target = Files.writeString(table.resolve("young-target"), "synthetic", UTF_8)
        val link = Files.createSymbolicLink(table.resolve("young-link"), target)
        assert(!DeltaVacuumEligibility.canSkip(spark, paths.bronze, safetyHours, maximumEntries))
        Files.delete(link)
        val alias = Files.createSymbolicLink(table.getParent.resolve("linked-table"), table)
        assert(!DeltaVacuumEligibility.canSkip(spark, alias.toUri.toString, safetyHours, maximumEntries))
        val log = table.resolve("_delta_log")
        val savedLog = table.getParent.resolve("saved-log-control")
        Files.move(log, savedLog)
        Files.createSymbolicLink(log, savedLog)
        try assert(!DeltaVacuumEligibility.canSkip(spark, paths.bronze, safetyHours, maximumEntries))
        finally {
          Files.delete(log)
          val _ = Files.move(savedLog, log)
        }
      }
    }
  }

  test("native fallback protects young tombstones and reclaims eligible tombstones with the unchanged horizon") {
    withTable { (spark, execution, paths, table) =>
      val retention = new AnalyticsDeltaRetention[IO](
        paths,
        AnalyticsTestOperationalConfig.operational,
        execution,
        Slf4jLogger.getLogger[IO]
      )
      def currentFiles: Set[Path] = {
        val entries = Files.list(table)
        try entries.iterator().asScala.filter(_.getFileName.toString.endsWith(".parquet")).toSet
        finally entries.close()
      }
      def replace(value: String): Unit = {
        val shape = StructType(Vector(StructField("value", StringType, nullable = false)))
        spark
          .createDataFrame(Vector(Row(value)).asJava, shape)
          .write
          .format("delta")
          .mode("overwrite")
          .save(paths.bronze)
      }
      for {
        first <- execution(currentFiles)
        _ <- execution {
          first.foreach(path =>
            Files.setLastModifiedTime(path, FileTime.fromMillis(System.currentTimeMillis() - 8.days.toMillis))
          )
          replace("young tombstone control")
          assert(!DeltaVacuumEligibility.canSkip(spark, paths.bronze, safetyHours, maximumEntries))
        }
        _ <- retention.vacuumUnreferencedFiles(spark)
        _ <- execution(assert(first.forall(path => Files.exists(path))))
        second <- execution(currentFiles -- first)
        _ <- execution {
          second.foreach(path =>
            Files.setLastModifiedTime(path, FileTime.fromMillis(System.currentTimeMillis() - 8.days.toMillis))
          )
          // Delete/overwrite uses the system clock independently of DeltaLog's cleanup clock.
          // Backdate only this test-owned native remove action; keep the earlier young tombstone unchanged.
          val log = DeltaLog.forTable(spark, paths.bronze)
          val expiredAt = System.currentTimeMillis() - 8.days.toMillis
          val expiredRemovals = log
            .update()
            .allFiles
            .collect()
            .toVector
            .map(_.removeWithTimestamp(expiredAt))
          assertEquals(expiredRemovals.map(_.path).toSet, second.map(_.getFileName.toString))
          assert(expiredRemovals.forall(_.deletionTimestamp.contains(expiredAt)))
          val version =
            log.startTransaction(catalogTableOpt = None).commit(expiredRemovals, DeltaOperations.Truncate())
          val committedRemovals =
            log.getChanges(version).take(1).flatMap(_._2).collect { case removed: RemoveFile => removed }.toVector
          assertEquals(
            committedRemovals.map(removed => removed.path -> removed.deletionTimestamp).toSet,
            expiredRemovals.map(removed => removed.path -> removed.deletionTimestamp).toSet
          )
          replace("eligible tombstone control")
          assert(!DeltaVacuumEligibility.canSkip(spark, paths.bronze, safetyHours, maximumEntries))
        }
        _ <- retention.vacuumUnreferencedFiles(spark)
        _ <- execution {
          assert(second.forall(path => !Files.exists(path)))
          assert(first.forall(path => Files.exists(path)))
          assertEquals(spark.read.format("delta").load(paths.bronze).count(), 1L)
        }
      } yield ()
    }
  }

  test("unsafe and negative retention retain native validation errors") {
    withTable { (spark, execution, paths, _) =>
      for {
        _ <- execution {
          assert(!DeltaVacuumEligibility.canSkip(spark, paths.bronze, 167.49d, maximumEntries))
          assert(DeltaVacuumEligibility.canSkip(spark, paths.bronze, 167.5d, maximumEntries))
          assert(!DeltaVacuumEligibility.canSkip(spark, paths.bronze, -1d, maximumEntries))
          spark.conf.set("spark.databricks.delta.vacuum.lite.enabled", "true")
          try assert(!DeltaVacuumEligibility.canSkip(spark, paths.bronze, safetyHours, maximumEntries))
          finally spark.conf.set("spark.databricks.delta.vacuum.lite.enabled", "false")
        }
        unsafe <- execution(DeltaTable.forPath(spark, paths.bronze).vacuum(167d)).attempt
        negative <- execution(DeltaTable.forPath(spark, paths.bronze).vacuum(-1d)).attempt
        _ <- IO { assert(unsafe.isLeft); assert(negative.isLeft) }
      } yield ()
    }
  }

  test("unsupported protocol cannot be hidden by a fresh filesystem listing") {
    withTable { (spark, execution, paths, table) =>
      for {
        _ <- execution {
          val update = table.resolve("_delta_log").resolve("00000000000000000001.json")
          Files.writeString(
            update,
            "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":99}}\n",
            UTF_8,
            StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE
          )
          DeltaLog.invalidateCache(spark, new org.apache.hadoop.fs.Path(paths.bronze))
          assert(!DeltaVacuumEligibility.canSkip(spark, paths.bronze, safetyHours, maximumEntries))
        }
        native <- execution(DeltaTable.forPath(spark, paths.bronze).vacuum(safetyHours)).attempt
        _ <- IO(assert(native.isLeft))
      } yield ()
    }
  }
}
