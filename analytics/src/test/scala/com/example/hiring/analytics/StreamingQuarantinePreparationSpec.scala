package com.example.hiring.analytics.adapter.spark

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.{
  AnalyticsTestOperationalConfig,
  AnalyticsTestSubjectPseudonymizer,
  TestAnalyticsLakehousePaths
}
import java.sql.Timestamp
import java.time.Instant
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.*
import munit.CatsEffectSuite
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.*
import org.apache.spark.storage.StorageLevel
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class StreamingQuarantinePreparationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 12.minutes
  private def withSpark(check: (SparkSession, SparkExecution[IO], Path) => IO[Unit]): IO[Unit] =
    SparkBlockingExecution.resource[IO].use { execution =>
      val resource = for {
        root <- Resource.make(IO.blocking(Files.createTempDirectory("hiring-quarantine-preparation-")))(path =>
          IO.blocking {
            val entries = Files.walk(path)
            try entries.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
            finally entries.close()
          }
        )
        spark <- Resource.make(execution {
          SparkSession
            .builder()
            .master("local[2]")
            .appName("StreamingQuarantinePreparationSpec")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.databricks.delta.snapshotPartitions", "2")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        })(spark => execution.blocking(spark.stop()))
        _ <- Resource.eval(execution.attachSparkContext(spark.sparkContext))
      } yield (spark, root)
      resource.use { case (spark, root) => check(spark, execution, root) }
    }

  private final class Work(fenceGroup: String) extends SparkListener {
    private var fenceJob: Option[Int] = None
    val fenceEnded = new CountDownLatch(1)
    override def onJobStart(event: SparkListenerJobStart): Unit = {
      if (Option(event.properties).exists(_.getProperty("spark.jobGroup.id") == fenceGroup))
        fenceJob = Some(event.jobId)
    }
    val jobs = new AtomicLong()
    val tasks = new AtomicLong()
    val rows = new AtomicLong()
    val bytes = new AtomicLong()
    override def onJobEnd(event: SparkListenerJobEnd): Unit = {
      jobs.incrementAndGet()
      if (fenceJob.contains(event.jobId)) fenceEnded.countDown()
    }
    override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
      tasks.incrementAndGet()
      Option(event.taskMetrics).foreach { metrics =>
        rows.addAndGet(metrics.inputMetrics.recordsRead)
        bytes.addAndGet(metrics.inputMetrics.bytesRead)
      }
      ()
    }
    def snapshot: Vector[Long] = Vector(jobs.get(), tasks.get(), rows.get(), bytes.get())
  }

  private def completed(spark: SparkSession, work: Work, group: String): Unit = {
    spark.sparkContext.setJobGroup(group, "Conflict metrics listener fence")
    try {
      // One identical tiny job in each variant establishes a public listener FIFO fence.
      assertEquals(spark.sparkContext.parallelize(Seq(1), 1).count(), 1L)
      assert(work.fenceEnded.await(10L, TimeUnit.SECONDS), "native listener fence was not observed")
    } finally spark.sparkContext.clearJobGroup()
  }

  private val at = Instant.parse("2026-10-04T12:00:00Z")
  private val keys = AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(19))

  private def parsed(spark: SparkSession, indices: Vector[Int], changed: Boolean): DataFrame = {
    val schema = StructType(
      Vector(
        StructField("topic", StringType, false),
        StructField("partition", IntegerType, false),
        StructField("offset", LongType, false),
        StructField("value", StringType, false)
      )
    )
    val rows = indices.zipWithIndex.map { case (index, offset) =>
      val actor = UUID.nameUUIDFromBytes(s"subject-$index".getBytes("UTF-8"))
      val skill = if (changed) "Changed" else "Scala"
      val body =
        com.example.hiring.analytics.AnalyticsOperationalEventFixtures.complete(
          s"""{"eventId":"event-$index","eventType":"JOB_CREATED","occurredAt":"$at","aggregateType":"Job","aggregateId":"job-$index","actorId":"$actor","payload":{"jobId":"job-$index","job":{"skills":["$skill"]}}}"""
        )
      Row("hiring.quarantine.test", 0, offset.toLong, body)
    }
    val raw = spark
      .createDataFrame(rows.asJava, schema)
      .withColumn("value", encode(col("value"), "UTF-8"))
      .withColumn("timestamp", lit(Timestamp.from(at)))
    OperationalEventTransforms.parseKafkaRecords(raw)
  }

  test("streaming quarantine resource preserves actual native outcomes and reduces repeated retained conflict work") {
    withSpark { (spark, execution, root) =>
      def run(mixed: Boolean, owned: Boolean): IO[(Vector[Long], Vector[String], Vector[String], Long, Long)] = {
        val paths = TestAnalyticsLakehousePaths.unsafe(root.resolve(s"$mixed-$owned").toUri.toString)
        val writer = new DeltaBatchWriter[IO](paths, execution)
        val stage = new AnalyticsBatchSilverStage[IO](
          paths,
          keys,
          execution,
          writer,
          new DeltaBatchReader[IO](execution),
          QuarantineIdentifier,
          AnalyticsTestOperationalConfig.operational.retention
        )
        for {
          seed <- execution(parsed(spark, (0 until 7500).toVector, false))
          markers <- execution(AnalyticsSubjectPrivacy.emptyMarkers(seed))
          silver <- execution.either(
            OperationalEventTransforms.silver(OperationalEventTransforms.validEvents(seed), keys, markers)
          )
          _ <- writer.merge(
            writer.withExpiry(
              silver.filter(
                !col("eventId").isin(
                  (6000 until 7500).map(i =>
                    com.example.hiring.analytics.AnalyticsOperationalEventFixtures.id(s"event-$i")
                  )*
                )
              ),
              at,
              30
            ),
            paths.silver,
            "target.eventId = source.eventId"
          )
          _ <- execution(
            spark
              .sql(s"ALTER TABLE delta.`${paths.silver}` SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '0')")
              .collect()
          )
          late <- execution(
            AnalyticsSubjectPrivacy
              .withSubjectToken(OperationalEventTransforms.validEvents(seed), keys)
              .filter(
                col("eventId").isin(
                  (6000 until 7500)
                    .map(i => com.example.hiring.analytics.AnalyticsOperationalEventFixtures.id(s"event-$i"))*
                )
              )
          )
          _ <- new AnalyticsLateFactStage[IO](paths, execution, writer).persistClosedDayFacts(late, markers, at)
          incoming <- execution {
            if (mixed)
              parsed(spark, ((0 until 490) ++ (6000 until 6500)).toVector, true)
                .unionByName(
                  parsed(spark, (0 until 10).toVector, false).withColumn("offset", col("offset") + lit(990L))
                )
            else parsed(spark, (10000 until 11000).toVector, false)
          }
          result <- SparkStreamingBatchStages.cacheAdmissionFrames(Vector(incoming), execution).use { _ =>
            execution(incoming.count()) *> {
              val group = "quarantine-resource-fence-" + UUID.randomUUID()
              val work = new Work(group)
              Resource
                .make(execution(spark.sparkContext.addSparkListener(work)))(_ =>
                  execution(spark.sparkContext.removeSparkListener(work))
                )
                .use { _ =>
                  val bronze = AnalyticsBronzeInput(incoming, at, 1000L, 1000L, 0L)
                  val scope =
                    if (owned) stage.quarantinePreparation(spark, bronze, markers, false)
                    else Resource.eval(stage.separateQuarantine(spark, bronze, markers, false))
                  scope.use { prepared =>
                    for {
                      ids <- execution(prepared.conflicts.collect().toVector.map(_.getString(0)).sorted)
                      _ <- IO {
                        assertEquals(prepared.conflictingEventIds, if (mixed) 990L else 0L);
                        assertEquals(prepared.quarantinedRecords, if (mixed) 1000L else 0L)
                      }
                      _ <- stage.mergeSilver(prepared, at)
                      rows <- execution {
                        val quarantine = spark.read.format("delta").load(paths.quarantine)
                        assert(AnalyticsTableSchemas.matches(quarantine.schema, AnalyticsTableSchemas.quarantine))
                        assert(!quarantine.columns.contains("rawValue"))
                        val values = quarantine.orderBy("offset").toJSON.collect().toVector
                        completed(spark, work, group)
                        (
                          work.snapshot,
                          ids,
                          values,
                          DeltaLogFactory.system(spark, paths.quarantine).update().version,
                          DeltaLogFactory.system(spark, paths.silver).update().version
                        )
                      }
                    } yield rows
                  }
                }
            }
          }
        } yield result
      }
      Vector(false, true).traverse_ { mixed =>
        for {
          baseline <- run(mixed, false)
          owned <- run(mixed, true)
          _ <- IO {
            assertEquals(owned._2, baseline._2)
            assertEquals(owned._3, baseline._3)
            assertEquals(owned._4, baseline._4)
            assertEquals(owned._5, baseline._5)
            println(
              s"QUARANTINE_RESOURCE_NATIVE mixed=$mixed baseline=${baseline._1.mkString(",")} owned=${owned._1.mkString(",")} markerJobsPerVariant=1"
            )
            assert(owned._1(2) < baseline._1(2), "owned conflict cache must reduce native source rows")
            assert(owned._1(3) < baseline._1(3), "owned conflict cache must reduce native source bytes")
          }
        } yield ()
      }
    }
  }

  test("actual quarantine preparation preserves malformed and active marker privacy parity") {
    withSpark { (spark, execution, root) =>
      def run(active: Boolean, owned: Boolean): IO[(Vector[Long], Vector[String], Vector[String], Long, Long)] = {
        val paths = TestAnalyticsLakehousePaths.unsafe(root.resolve(s"privacy-$active-$owned").toUri.toString)
        val writer = new DeltaBatchWriter[IO](paths, execution)
        val stage = new AnalyticsBatchSilverStage[IO](
          paths,
          keys,
          execution,
          writer,
          new DeltaBatchReader[IO](execution),
          QuarantineIdentifier,
          AnalyticsTestOperationalConfig.operational.retention
        )
        for {
          input <- execution {
            val valid = parsed(spark, Vector(100, 101), false)
            val malformed = parsed(spark, Vector(102), false)
              .withColumn("eventId", lit(null).cast(StringType))
              .withColumn("rawValue", lit("malformed-envelope"))
              .withColumn("offset", lit(2L))
            valid.unionByName(malformed)
          }
          markers <- execution {
            val tokens = AnalyticsSubjectPrivacy
              .withSubjectToken(OperationalEventTransforms.validEvents(input), keys)
              .filter(col("eventId") === "cdeb21df-44ce-3deb-868c-5f5af1e3b52c")
              .select("subjectToken")
            if (active) tokens else tokens.limit(0)
          }
          scope =
            if (owned) stage.quarantinePreparation(spark, AnalyticsBronzeInput(input, at, 3L, 2L, 1L), markers, active)
            else
              Resource.eval(
                stage.separateQuarantine(spark, AnalyticsBronzeInput(input, at, 3L, 2L, 1L), markers, active)
              )
          result <- scope.use { prepared =>
            stage.mergeSilver(prepared, at) *> execution {
              val silver = spark.read.format("delta").load(paths.silver)
              val quarantine = spark.read.format("delta").load(paths.quarantine)
              assert(AnalyticsTableSchemas.matches(quarantine.schema, AnalyticsTableSchemas.quarantine))
              assert(
                !quarantine.columns.exists(name => Set("actorId", "rawValue", "payload", "subjectToken").contains(name))
              )
              assertEquals(prepared.validRecords, 2L)
              assertEquals(prepared.suppressedRecords, if (active) 1L else 0L)
              assertEquals(prepared.quarantinedRecords, 1L)
              assertEquals(prepared.conflictingEventIds, 0L)
              val silverIds = silver.select("eventId").collect().toVector.map(_.getString(0)).sorted
              assertEquals(
                silverIds,
                if (active) Vector("258507d9-c11b-30ea-b41a-519e5d254cd0")
                else Vector("cdeb21df-44ce-3deb-868c-5f5af1e3b52c", "258507d9-c11b-30ea-b41a-519e5d254cd0").sorted
              )
              val quarantineRows = quarantine.toJSON.collect().toVector.sorted
              assertEquals(quarantineRows.size, if (active) 0 else 1)
              if (!active) {
                assertEquals(
                  quarantine.select("quarantineReason").first().getString(0),
                  "INVALID_OPERATIONAL_EVENT_ENVELOPE"
                )
                assert(quarantine.filter(col("expiresAt") <= lit(Timestamp.from(at))).count() == 0L)
              }
              (
                Vector(
                  prepared.validRecords,
                  prepared.suppressedRecords,
                  prepared.quarantinedRecords,
                  prepared.conflictingEventIds
                ),
                silverIds,
                quarantineRows,
                DeltaLogFactory.system(spark, paths.quarantine).update().version,
                DeltaLogFactory.system(spark, paths.silver).update().version
              )
            }
          }
        } yield result
      }
      Vector(false, true).traverse_ { active =>
        (run(active, false), run(active, true)).tupled.flatMap { case (baseline, owned) =>
          IO(assertEquals(owned, baseline))
        }
      }
    }
  }

  test("actual streaming quarantine conflict ownership releases after normal error and cancellation") {
    withSpark { (spark, execution, root) =>
      val paths = TestAnalyticsLakehousePaths.unsafe(root.toUri.toString)
      val writer = new DeltaBatchWriter[IO](paths, execution)
      val stage = new AnalyticsBatchSilverStage[IO](
        paths,
        keys,
        execution,
        writer,
        new DeltaBatchReader[IO](execution),
        QuarantineIdentifier,
        AnalyticsTestOperationalConfig.operational.retention
      )
      for {
        input <- execution(parsed(spark, Vector(100), false))
        markers <- execution(AnalyticsSubjectPrivacy.emptyMarkers(input))
        captured <- cats.effect.Ref.of[IO, Option[DataFrame]](None)
        scope = stage.quarantinePreparation(spark, AnalyticsBronzeInput(input, at, 1L, 1L, 0L), markers, false)
        _ <- scope.use { p =>
          captured.set(Some(p.conflicts)) *>
            SparkStreamingBatchStages.cacheAdmissionFrames(Vector(p.conflicts), execution).use(_ => IO.unit) *>
            execution(assertEquals(p.conflicts.storageLevel, StorageLevel.MEMORY_AND_DISK))
        }
        frame <- captured.get.map(_.get)
        _ <- execution(assertEquals(frame.storageLevel, StorageLevel.NONE))
        failed <- scope
          .use(p => captured.set(Some(p.conflicts)) *> IO.raiseError[Unit](new IllegalStateException("test failure")))
          .attempt
        _ <- IO(assert(failed.isLeft))
        errorFrame <- captured.get.map(_.get)
        _ <- execution(assertEquals(errorFrame.storageLevel, StorageLevel.NONE))
        ready <- cats.effect.Deferred[IO, Unit]
        fiber <- scope.use(p => captured.set(Some(p.conflicts)) *> ready.complete(()) *> IO.never[Unit]).start
        _ <- ready.get *> fiber.cancel *> fiber.join
        canceled <- captured.get.map(_.get)
        _ <- execution(assertEquals(canceled.storageLevel, StorageLevel.NONE))
      } yield ()
    }
  }
}
