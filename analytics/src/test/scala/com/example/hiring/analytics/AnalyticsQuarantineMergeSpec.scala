package com.example.hiring.analytics.adapter.spark

import cats.effect.{IO, Resource}
import com.example.hiring.analytics.{AnalyticsTestOperationalConfig, AnalyticsTestSubjectPseudonymizer}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import io.delta.tables.DeltaTable
import munit.CatsEffectSuite
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, sha2}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import java.nio.file.Files
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class AnalyticsQuarantineMergeSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val condition = "target.quarantineId = source.quarantineId"
  private val expires = Instant.parse("2030-01-01T00:00:00Z")

  private def withLakehouse(check: (SparkSession, SparkExecution[IO], AnalyticsLakehousePaths) => IO[Unit]): IO[Unit] =
    SparkBlockingExecution.resource[IO].use { execution =>
      Resource
        .make(execution {
          val root = Files.createTempDirectory("analytics-quarantine-merge-")
          val spark = SparkSession
            .builder()
            .master("local[2]")
            .appName("AnalyticsQuarantineMergeSpec")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
          val paths =
            AnalyticsLakehousePaths.from(root.toUri.toString).toEither.fold(errors => fail(errors.toString), identity)
          (spark, root, paths)
        }) { case (spark, root, _) =>
          execution.blocking {
            try spark.stop()
            finally {
              val walk = Files.walk(root)
              try walk.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.delete)
              finally walk.close()
            }
          }
        }
        .use { case (spark, _, paths) =>
          execution.attachSparkContext(spark.sparkContext) *> check(spark, execution, paths)
        }
    }

  private def rows(spark: SparkSession, reason: String = "INVALID_OPERATIONAL_EVENT_ENVELOPE"): DataFrame =
    spark.createDataFrame(
      Vector(
        Row(
          "hiring.test.events",
          0,
          1L,
          "fingerprint",
          Vector.empty[String],
          "quarantine-1",
          reason,
          Timestamp.from(expires)
        )
      ).asJava,
      AnalyticsTableSchemas.struct(AnalyticsTableSchemas.quarantine)
    )

  test("proven empty projection keeps native MERGE but reduces source scans and Spark jobs") {
    withLakehouse { (spark, execution, paths) =>
      val writer = new DeltaBatchWriter[IO](paths, execution)
      for {
        input <- execution {
          val visits = spark.sparkContext.longAccumulator("quarantine-source-row-visits")
          val seed = rows(spark)
          val counted = spark.createDataFrame(seed.rdd.mapPartitions(_.map { row => visits.add(1L); row }), seed.schema)
          // Emptiness requires inspecting the source; it is not a constant false filter.
          val derived = counted.filter(col("offset") < lit(0L)).withColumn("payloadHash", sha2(col("payloadHash"), 256))
          (seed, derived, visits)
        }
        _ <- writer.merge(input._1, paths.quarantine, condition)
        measure = (frame: DataFrame) => {
          val group = UUID.randomUUID().toString
          val starts = new AtomicInteger(0)
          val listener = new SparkListener {
            override def onJobStart(event: SparkListenerJobStart): Unit =
              if (Option(event.properties).exists(_.getProperty("spark.jobGroup.id") == group)) {
                val _ = starts.incrementAndGet()
              }
          }
          val grouped = new SparkExecution[IO] {
            override def apply[A](work: => A): IO[A] = execution {
              spark.sparkContext.setJobGroup(group, "quarantine merge comparison")
              try work
              finally spark.sparkContext.clearJobGroup()
            }
            override def either[A](work: => Either[AnalyticsError, A]): IO[A] = apply(work).flatMap(IO.fromEither)
          }
          Resource
            .make(execution { input._3.reset(); spark.sparkContext.addSparkListener(listener) })(_ =>
              execution(spark.sparkContext.removeSparkListener(listener))
            )
            .use { _ =>
              for {
                versionBefore <- execution(
                  DeltaTable.forPath(spark, paths.quarantine).history(1).head().getAs[Long]("version")
                )
                _ <- new DeltaBatchWriter[IO](paths, grouped).merge(frame, paths.quarantine, condition)
                jobs <- execution(spark.sparkContext.statusTracker.getJobIdsForGroup(group).length)
                _ <- {
                  def awaitListener(left: Int): IO[Unit] =
                    if (starts.get() == jobs) IO.unit
                    else if (left == 0) IO(fail("Spark listener did not observe every completed merge job"))
                    else IO.sleep(10.millis) *> awaitListener(left - 1)
                  awaitListener(200)
                }
                scans <- execution(input._3.value.longValue())
                versionAfter <- execution(
                  DeltaTable.forPath(spark, paths.quarantine).history(1).head().getAs[Long]("version")
                )
              } yield (jobs, scans, versionAfter - versionBefore)
            }
        }
        baseline <- measure(input._2)
        optimized <- measure(input._2.limit(0))
        persisted <- execution(spark.read.format("delta").load(paths.quarantine).collect().toVector)
        _ <- IO {
          assert(baseline._2 > 0L, s"baseline must inspect its source: $baseline")
          assertEquals(optimized._2, 0L)
          assert(optimized._1 < baseline._1, s"native jobs baseline=$baseline optimized=$optimized")
          assertEquals(optimized._3, baseline._3)
          assertEquals(persisted.size, 1)
          assertEquals(persisted.head.getAs[String]("payloadHash"), "fingerprint")
          println(
            s"QUARANTINE_EMPTY_MERGE_COMPARISON baselineJobs=${baseline._1} baselineScans=${baseline._2} optimizedJobs=${optimized._1} optimizedScans=${optimized._2} nativeVersionDelta=${optimized._3} retainedRows=${persisted.size}"
          )
        }
      } yield ()
    }
  }

  test("empty native merge creates the quarantine target with privacy properties and exact schema") {
    withLakehouse { (spark, execution, paths) =>
      new DeltaBatchWriter[IO](paths, execution).merge(rows(spark).limit(0), paths.quarantine, condition) *> execution {
        val table = spark.read.format("delta").load(paths.quarantine)
        assert(AnalyticsTableSchemas.matches(table.schema, AnalyticsTableSchemas.quarantine))
        assertEquals(table.count(), 0L)
        val properties = DeltaTable
          .forPath(spark, paths.quarantine)
          .detail()
          .head()
          .getAs[scala.collection.Map[String, String]]("properties")
        assertEquals(properties.get("delta.dataSkippingNumIndexedCols"), Some("0"))
        assert(!table.columns.exists(Set("rawValue", "actorId", "payload", "subjectToken")))
      }
    }
  }

  test("empty source retains source schema rejection and native invalid-condition analysis") {
    withLakehouse { (spark, execution, paths) =>
      val writer = new DeltaBatchWriter[IO](paths, execution)
      for {
        wrongSchema <- writer.merge(rows(spark).drop("payloadHash").limit(0), paths.quarantine, condition).attempt
        invalidCondition <- writer
          .merge(rows(spark).limit(0), paths.quarantine, "target.absent = source.absent")
          .attempt
        _ <- IO {
          assert(wrongSchema.left.exists(_.isInstanceOf[AnalyticsError.DeltaSchemaMismatch]))
          assert(invalidCondition.isLeft)
        }
      } yield ()
    }
  }

  test("empty source retains existing target schema rejection") {
    withLakehouse { (spark, execution, paths) =>
      for {
        _ <- execution(rows(spark).drop("payloadHash").write.format("delta").save(paths.quarantine))
        result <- new DeltaBatchWriter[IO](paths, execution)
          .merge(rows(spark).limit(0), paths.quarantine, condition)
          .attempt
        _ <- IO(assert(result.left.exists(_.isInstanceOf[AnalyticsError.DeltaSchemaMismatch])))
      } yield ()
    }
  }

  test("nonempty malformed conflict and future quarantine merges retain their rows and deduplicate") {
    withLakehouse { (spark, execution, paths) =>
      val writer = new DeltaBatchWriter[IO](paths, execution)
      for {
        frame <- execution {
          Vector(
            "INVALID_OPERATIONAL_EVENT_ENVELOPE",
            "CONFLICTING_EVENT_ID",
            "EVENT_TIMESTAMP_TOO_FAR_IN_FUTURE"
          ).zipWithIndex
            .map { case (reason, index) => rows(spark, reason).withColumn("quarantineId", lit(s"quarantine-$index")) }
            .reduce(_.unionByName(_))
        }
        _ <- writer.merge(frame, paths.quarantine, condition) *> writer.merge(frame, paths.quarantine, condition)
        _ <- execution {
          val persisted = spark.read.format("delta").load(paths.quarantine)
          assertEquals(persisted.count(), 3L)
          assertEquals(
            persisted.select("quarantineReason").collect().map(_.getString(0)).toSet,
            Set("INVALID_OPERATIONAL_EVENT_ENVELOPE", "CONFLICTING_EVENT_ID", "EVENT_TIMESTAMP_TOO_FAR_IN_FUTURE")
          )
        }
      } yield ()
    }
  }

  test("mergeWhenFresh still rejects an empty source") {
    withLakehouse { (spark, execution, paths) =>
      new DeltaBatchWriter[IO](paths, execution)
        .mergeWhenFresh(rows(spark).limit(0), paths.quarantine, condition, () => expires.minusSeconds(1))
        .attempt
        .flatMap(result => IO(assertEquals(result, Left(AnalyticsError.LateFactReplayRejected))))
    }
  }

  test("batch quarantine uses measured zeros and preserves malformed and conflicting rows") {
    withLakehouse { (spark, execution, paths) =>
      val native = new DeltaBatchWriter[IO](paths, execution)
      val projections = scala.collection.mutable.ArrayBuffer.empty[Boolean]
      val recording = new DeltaWriter[IO] {
        override def merge(frame: DataFrame, path: String, condition: String): IO[Unit] =
          execution { projections += frame.queryExecution.optimizedPlan.maxRows.contains(0L) } *>
            native.merge(frame, path, condition)
        override def mergeWhenFresh(frame: DataFrame, path: String, condition: String, at: () => Instant): IO[Unit] =
          native.mergeWhenFresh(frame, path, condition, at)
        override def withExpiry(frame: DataFrame, now: Instant, days: Int): DataFrame =
          native.withExpiry(frame, now, days)
      }
      val keys = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(7))
      val stage = new AnalyticsBatchSilverStage[IO](
        paths,
        keys,
        execution,
        recording,
        new DeltaBatchReader[IO](execution),
        QuarantineIdentifier,
        AnalyticsTestOperationalConfig.operational.retention
      )
      val observed = Instant.parse("2026-10-02T12:00:00Z")
      def event(id: String, skill: String): String =
        com.example.hiring.analytics.AnalyticsOperationalEventFixtures.complete(
          s"""{"eventId":"$id","eventType":"JOB_CREATED","occurredAt":"$observed","aggregateType":"Job","aggregateId":"ac15a52e-59f3-33d6-9344-2a598ffab484","actorId":"d3d4e2cb-8e0c-3a66-8cc2-1379ac3a7686","payload":{"job":{"skills":["$skill"]}}}"""
        )
      def parsed(bodies: Vector[String], offsetBase: Long): DataFrame = {
        val raw = spark.createDataFrame(
          bodies.zipWithIndex.map { case (body, index) =>
            Row("hiring.test.events", 0, offsetBase + index, Timestamp.from(observed), body)
          }.asJava,
          StructType(
            Vector(
              StructField("topic", StringType, false),
              StructField("partition", IntegerType, false),
              StructField("offset", LongType, false),
              StructField("timestamp", TimestampType, false),
              StructField("value", StringType, true)
            )
          )
        )
        OperationalEventTransforms.parseKafkaRecords(raw)
      }
      for {
        clean <- execution(parsed(Vector(event("clean", "Scala")), 0L))
        markers <- execution(AnalyticsSubjectPrivacy.emptyMarkers(clean))
        first <- stage.separateQuarantine(spark, AnalyticsBronzeInput(clean, observed, 1L, 1L, 0L), markers, false)
        dirty <- execution(parsed(Vector("not-json", event("conflict", "Scala"), event("conflict", "Kotlin")), 10L))
        second <- stage.separateQuarantine(spark, AnalyticsBronzeInput(dirty, observed, 3L, 2L, 1L), markers, false)
        _ <- execution(assertEquals(projections.toVector, Vector(true, false)))
        suppressed <- execution(parsed(Vector("not-json", event("marked", "Scala")), 20L))
        activeMarkers <- execution {
          spark.createDataFrame(
            Vector(
              Row(AnalyticsTestSubjectPseudonymizer.tokenValue(keys, "d3d4e2cb-8e0c-3a66-8cc2-1379ac3a7686"))
            ).asJava,
            StructType(Vector(StructField("subjectToken", StringType, false)))
          )
        }
        third <- stage.separateQuarantine(
          spark,
          AnalyticsBronzeInput(suppressed, observed, 2L, 1L, 1L),
          activeMarkers,
          true
        )
        _ <- execution {
          assertEquals(first.quarantinedRecords, 0L)
          assertEquals(second.quarantinedRecords, 3L)
          assertEquals(third.suppressedRecords, 1L)
          val saved = spark.read.format("delta").load(paths.quarantine)
          assertEquals(saved.count(), 3L)
          assertEquals(saved.filter(col("quarantineReason") === "CONFLICTING_EVENT_ID").count(), 2L)
          assert(!saved.columns.contains("rawValue"))
        }
      } yield ()
    }
  }
}
