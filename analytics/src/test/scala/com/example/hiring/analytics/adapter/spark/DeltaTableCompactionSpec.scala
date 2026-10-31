package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.AnalyticsTestSpark
import com.example.hiring.analytics.TestAnalyticsLakehousePaths
import com.example.hiring.analytics.domain.{DeltaErasureHorizons, ErasureCompletionRule}

import cats.effect.{IO, Ref, Resource}
import munit.CatsEffectSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}
import org.typelevel.log4cats.Logger

import java.nio.file.{Files, Path}
import java.time.Instant
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** AC-04.3 and AC-04.4: compaction of the subject-attributed tables, and its effect on the erasure horizon. */
final class DeltaTableCompactionSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes

  private def withLakehouse(
      check: (
          SparkSession,
          SparkExecution[IO],
          com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
      ) => IO[Unit]
  ): IO[Unit] =
    SparkBlockingExecution.resource[IO].use { execution =>
      val resource = for {
        root <- Resource.make(IO.blocking(Files.createTempDirectory("hiring-compaction-")))(path =>
          IO.blocking {
            val entries = Files.walk(path)
            try entries.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
            finally entries.close()
          }
        )
        spark <- Resource.make(execution(AnalyticsTestSpark.builder("DeltaTableCompactionSpec").getOrCreate()))(spark =>
          execution.blocking(spark.stop())
        )
        _ <- Resource.eval(execution.attachSparkContext(spark.sparkContext))
      } yield (spark, root)
      resource.use { case (spark, root) =>
        check(spark, execution, TestAnalyticsLakehousePaths.unsafe(root.toUri.toString.stripSuffix("/")))
      }
    }

  private def append(spark: SparkSession, path: String, spec: DeltaTableSpec, first: Long, count: Int): Unit =
    DeltaSubjectTableFixtures
      .frame(spark, spec, first, count)
      .coalesce(1)
      .write
      .format("delta")
      .mode("append")
      .save(SparkPhysicalLocation.resolve(path))

  private def capturingLogger(lines: Ref[IO, Vector[String]]): Logger[IO] = new Logger[IO] {
    private def add(message: => String): IO[Unit] = lines.update(_ :+ message)
    override def error(t: Throwable)(message: => String): IO[Unit] = add(message)
    override def warn(t: Throwable)(message: => String): IO[Unit] = add(message)
    override def info(t: Throwable)(message: => String): IO[Unit] = add(message)
    override def debug(t: Throwable)(message: => String): IO[Unit] = add(message)
    override def trace(t: Throwable)(message: => String): IO[Unit] = add(message)
    override def error(message: => String): IO[Unit] = add(message)
    override def warn(message: => String): IO[Unit] = add(message)
    override def info(message: => String): IO[Unit] = add(message)
    override def debug(message: => String): IO[Unit] = add(message)
    override def trace(message: => String): IO[Unit] = add(message)
  }

  private val start = Instant.parse("2026-10-30T00:00:00Z")

  test("AC-04.3 compaction rewrites many small files into fewer, records the counts and waits for the interval") {
    withLakehouse { (spark, execution, paths) =>
      val spec = AnalyticsDeltaCatalog.Bronze
      for {
        lines <- Ref.of[IO, Vector[String]](Vector.empty)
        compaction = new DeltaTableCompaction[IO](paths, execution, capturingLogger(lines))
        _ <- execution {
          AnalyticsTableSchemas.createOrValidate(spark, paths.bronze, spec)
          AnalyticsTableSchemas.createOrValidate(spark, paths.silver, AnalyticsDeltaCatalog.Silver)
          (0 until 6).foreach(batch => append(spark, paths.bronze, spec, batch * 10L, 10))
          (0 until 4).foreach(batch => append(spark, paths.silver, AnalyticsDeltaCatalog.Silver, batch * 10L, 10))
        }
        beforeRows <- execution(DeltaTables.read(spark, paths.bronze).orderBy(col(Columns.Offset)).collect().toVector)
        disabled <- compaction.compactDue(spark, start, Duration.Zero)
        first <- compaction.compactDue(spark, start, 1.hour)
        tooSoon <- compaction.compactDue(spark, start.plusSeconds(30.minutes.toSeconds), 1.hour)
        elapsed <- compaction.compactDue(spark, start.plusSeconds(1.hour.toSeconds), 1.hour)
        afterRows <- execution(DeltaTables.read(spark, paths.bronze).orderBy(col(Columns.Offset)).collect().toVector)
        recorded <- execution(
          DeltaLogFactory
            .system(spark, paths.bronze)
            .update()
            .metadata
            .configuration(DeltaTableCompaction.LastOptimizedProperty)
        )
        logged <- lines.get
        _ = assertEquals(disabled, Vector.empty)
        _ = assertEquals(
          first.toSet,
          Set(
            DeltaCompactionResult("bronze_operational_events", 6L, 1L),
            DeltaCompactionResult("silver_operational_events", 4L, 1L)
          )
        )
        _ = assertEquals(tooSoon, Vector.empty)
        _ = assertEquals(
          elapsed.toSet,
          Set(
            DeltaCompactionResult("bronze_operational_events", 1L, 1L),
            DeltaCompactionResult("silver_operational_events", 1L, 1L)
          )
        )
        _ = assertEquals(afterRows, beforeRows, "OPTIMIZE does not change table contents")
        _ = assertEquals(recorded, start.plusSeconds(1.hour.toSeconds).toString)
        _ = assert(
          logged.contains("DELTA_COMPACTION table=bronze_operational_events filesBefore=6 filesAfter=1"),
          logged.toString
        )
        _ = assert(logged.forall(!_.contains(DeltaSubjectTableFixtures.TokenPrefix)), "the log line carries no token")
      } yield ()
    }
  }

  test("AC-04.1 AC-04.3 compacted files keep the statistics allow-list and a token-free log") {
    withLakehouse { (spark, execution, paths) =>
      val spec = AnalyticsDeltaCatalog.LateFacts
      val compaction = new DeltaTableCompaction[IO](paths, execution, org.typelevel.log4cats.noop.NoOpLogger[IO])
      for {
        _ <- execution {
          AnalyticsTableSchemas.createOrValidate(spark, paths.lateFacts, spec)
          (0 until 3).foreach(batch => append(spark, paths.lateFacts, spec, batch * 10L, 10))
        }
        _ <- compaction.compactDue(spark, start, 1.hour)
        _ <- execution {
          val log = Path.of(SparkPhysicalLocation.resolve(paths.lateFacts)).resolve("_delta_log")
          val entries = Files.list(log)
          val text =
            try entries.iterator().asScala.toVector.filter(_.toString.endsWith(".json")).map(Files.readString).mkString
            finally entries.close()
          assert(text.contains("OPTIMIZE"), "the compaction committed")
          assert(text.contains("minValues"), "the compacted file has statistics")
          assert(!text.contains(DeltaSubjectTableFixtures.TokenPrefix), "no token value is in the log")
        }
      } yield ()
    }
  }

  test("AC-04.4 an OPTIMIZE after a purge moves the data-file erasure horizon to the OPTIMIZE") {
    withLakehouse { (spark, execution, paths) =>
      val spec = AnalyticsDeltaCatalog.Bronze
      val compaction = new DeltaTableCompaction[IO](paths, execution, org.typelevel.log4cats.noop.NoOpLogger[IO])
      val erasure = new AnalyticsBatchErasureStage[IO](paths, execution, _ => IO.unit, 100)
      val horizons = DeltaErasureHorizons(vacuumSafety = 7.days, logRetention = 1.minute)
      val deleted = DeltaSubjectTableFixtures.marker(3L)
      for {
        _ <- execution {
          AnalyticsTableSchemas.createOrValidate(spark, paths.bronze, spec)
          (0 until 3).foreach(batch => append(spark, paths.bronze, spec, batch * 10L, 10))
        }
        // Subject 3 lives in the first file, which the purge replaces by a file without its rows.
        subjectFiles <- erasure.captureMarkedFiles(spark, AnalyticsTableSchemas.markerFrame(spark, Vector(deleted)))
        noCompactionYet <- compaction.lastCompactionAt(spark)
        _ <- erasure.purgeMarkedSubjectRows(
          spark,
          paths.bronze,
          AnalyticsTableSchemas.markerFrame(spark, Vector(deleted))
        )
        purgedAt <- IO.realTimeInstant
        _ <- IO.sleep(1.second)
        // The next maintenance tick replaces the purge's output files together with the others.
        results <- compaction.compactDue(spark, Instant.now(), 1.hour)
        replaced <- execution(
          subjectFiles
            .filter(_.endsWith(".parquet"))
            .map(file => new java.io.File(new java.net.URI(file)))
            .filter(_.isFile)
        )
        optimizedAt <- compaction.lastCompactionAt(spark)
        _ = assertEquals(noCompactionYet, None)
        _ = assertEquals(results.find(_.table == "bronze_operational_events").map(_.filesAfter), Some(1L))
        _ = assert(optimizedAt.exists(_.isAfter(purgedAt)), s"OPTIMIZE $optimizedAt follows the purge $purgedAt")
        _ = assert(replaced.nonEmpty, "files that once held the subject's rows stay on storage until VACUUM")
        _ = assertEquals(
          ErasureCompletionRule.deltaDeadline(purgedAt, optimizedAt, horizons),
          optimizedAt.get.plusMillis(horizons.vacuumSafety.toMillis)
        )
        _ = assert(
          ErasureCompletionRule
            .deltaDeadline(purgedAt, optimizedAt, horizons)
            .isAfter(ErasureCompletionRule.deltaDeadline(purgedAt, horizons)),
          "the OPTIMIZE lengthens the horizon"
        )
      } yield ()
    }
  }

  test("AC-04.4 an OPTIMIZE that committed before its completion was recorded still counts toward the horizon") {
    withLakehouse { (spark, execution, paths) =>
      val spec = AnalyticsDeltaCatalog.Bronze
      val compaction = new DeltaTableCompaction[IO](paths, execution, org.typelevel.log4cats.noop.NoOpLogger[IO])
      for {
        _ <- execution {
          AnalyticsTableSchemas.createOrValidate(spark, paths.bronze, spec)
          (0 until 3).foreach(batch => append(spark, paths.bronze, spec, batch * 10L, 10))
        }
        _ <- compaction.compactDue(spark, start, 1.hour)
        recorded <- compaction.lastCompactionAt(spark)
        // A crash: the OPTIMIZE below commits, the process dies before it records anything. Many commits follow.
        _ <- IO.sleep(1.second)
        _ <- execution {
          val _ = spark.sql(
            s"ALTER TABLE ${DeltaTables.sqlIdentifier(paths.bronze)} SET TBLPROPERTIES " +
              s"('${DeltaTableCompaction.PendingProperty}' = '$start')"
          )
          (3 until 18).foreach(batch => append(spark, paths.bronze, spec, batch * 10L, 2))
          val _ = DeltaTables.forPath(spark, paths.bronze).optimize().executeCompaction()
          (18 until 40).foreach(batch => append(spark, paths.bronze, spec, batch * 10L, 2))
        }
        afterCrash <- compaction.lastCompactionAt(spark)
        _ = assert(recorded.isDefined, "the completed run is recorded")
        _ = assert(
          afterCrash.exists(at => recorded.exists(at.isAfter)),
          s"crashed OPTIMIZE seen: $recorded $afterCrash"
        )
      } yield ()
    }
  }

  test("AC-04.4 a manual OPTIMIZE long after the scheduled one is still the latest compaction") {
    withLakehouse { (spark, execution, paths) =>
      val spec = AnalyticsDeltaCatalog.Bronze
      val compaction = new DeltaTableCompaction[IO](paths, execution, org.typelevel.log4cats.noop.NoOpLogger[IO])
      for {
        _ <- execution {
          AnalyticsTableSchemas.createOrValidate(spark, paths.bronze, spec)
          (0 until 3).foreach(batch => append(spark, paths.bronze, spec, batch * 10L, 2))
        }
        _ <- compaction.compactDue(spark, start, 1.hour)
        scheduled <- compaction.lastCompactionAt(spark)
        _ <- IO.sleep(1.second)
        _ <- execution((100 until 115).foreach(batch => append(spark, paths.bronze, spec, batch * 10L, 2)))
        _ <- IO.sleep(1.second)
        manual <- execution {
          val _ = DeltaTables.forPath(spark, paths.bronze).optimize().executeCompaction()
          DeltaTables
            .forPath(spark, paths.bronze)
            .history()
            .filter(col("operation") === "OPTIMIZE")
            .agg(max(col("timestamp")))
            .head()
            .getTimestamp(0)
            .toInstant
        }
        _ <- execution((200 until 215).foreach(batch => append(spark, paths.bronze, spec, batch * 10L, 2)))
        found <- compaction.lastCompactionAt(spark)
        _ = assert(scheduled.isDefined)
        _ = assert(manual.isAfter(scheduled.get), s"$manual follows $scheduled")
        _ = assertEquals(found, Some(manual), "a bare OPTIMIZE beyond the newest ten commits is seen")
      } yield ()
    }
  }
}
