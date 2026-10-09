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
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import com.example.hiring.analytics.domain.{AnalyticsEventTimePolicy, EventTimeAdmission}

final class StreamingOpenSilverSelectionSpec extends CatsEffectSuite {
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
            .appName("StreamingOpenSilverSelectionSpec")
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
    private var endFenceJob: Option[Int] = None
    val endFenceEnded = new CountDownLatch(1)
    val fenceEnded = new CountDownLatch(1)
    override def onJobStart(event: SparkListenerJobStart): Unit = {
      if (Option(event.properties).exists(_.getProperty("spark.jobGroup.id") == fenceGroup))
        fenceJob = Some(event.jobId)
      if (Option(event.properties).exists(_.getProperty("spark.jobGroup.id") == fenceGroup + "-end"))
        endFenceJob = Some(event.jobId)
    }
    val jobs = new AtomicLong()
    val tasks = new AtomicLong()
    val rows = new AtomicLong()
    val bytes = new AtomicLong()
    val shuffleRead = new AtomicLong()
    val shuffleWrite = new AtomicLong()
    val memorySpill = new AtomicLong()
    val diskSpill = new AtomicLong()
    override def onJobEnd(event: SparkListenerJobEnd): Unit = {
      jobs.incrementAndGet()
      if (fenceJob.contains(event.jobId)) fenceEnded.countDown()
      if (endFenceJob.contains(event.jobId)) endFenceEnded.countDown()
    }
    override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
      tasks.incrementAndGet()
      Option(event.taskMetrics).foreach { metrics =>
        rows.addAndGet(metrics.inputMetrics.recordsRead)
        bytes.addAndGet(metrics.inputMetrics.bytesRead)
        shuffleRead.addAndGet(metrics.shuffleReadMetrics.totalBytesRead)
        shuffleWrite.addAndGet(metrics.shuffleWriteMetrics.bytesWritten)
        memorySpill.addAndGet(metrics.memoryBytesSpilled)
        diskSpill.addAndGet(metrics.diskBytesSpilled)
      }
      ()
    }
    def snapshot: Vector[Long] = Vector(
      jobs.get(),
      tasks.get(),
      rows.get(),
      bytes.get(),
      shuffleRead.get(),
      shuffleWrite.get(),
      memorySpill.get(),
      diskSpill.get()
    )
  }

  private def completed(spark: SparkSession, work: Work, group: String, end: Boolean = false): Unit = {
    spark.sparkContext.setJobGroup(group, "OPEN Silver selection listener fence")
    try {
      // One identical tiny job in each variant establishes a public listener FIFO fence.
      assertEquals(spark.sparkContext.parallelize(Seq(1), 1).count(), 1L)
      assert(
        (if (end) work.endFenceEnded else work.fenceEnded).await(10L, TimeUnit.SECONDS),
        "native listener fence was not observed"
      )
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

  private def classified(frame: DataFrame, observed: Instant, watermark: Option[Instant]): DataFrame = {
    val observedAt = observed
    val prior = watermark
    val shape = StructType(
      Vector(StructField("status", StringType, false), StructField("effectiveTime", TimestampType, true))
    )
    val classify = udf(
      new org.apache.spark.sql.api.java.UDF1[Timestamp, Row] {
        override def call(value: Timestamp): Row = if (value == null) Row("FUTURE", null)
        else
          AnalyticsEventTimePolicy.admit(value.toInstant, observedAt, prior) match {
            case EventTimeAdmission.Admitted(time)   => Row("OPEN", Timestamp.from(time))
            case EventTimeAdmission.LateClosedDay(_) => Row("CLOSED", null)
            case EventTimeAdmission.TooFarInFuture   => Row("FUTURE", null)
          }
      },
      shape
    )
    frame.withColumn("_admission", classify(col("occurredAt")))
  }

  private def selection(
      prepared: AnalyticsPreparedEvents,
      direct: Boolean,
      observed: Instant = at,
      watermark: Option[Instant] = None
  ): DataFrame = {
    val open = classified(prepared.incomingSilver, observed, watermark).filter(col("_admission.status") === lit("OPEN"))
    val selected =
      if (direct) open
      else {
        val pairs = open.select("eventId", "eventFingerprint")
        prepared.incomingSilver
          .join(pairs, Seq("eventId", "eventFingerprint"), "inner")
          .join(prepared.conflicts, Seq("eventId"), "left_anti")
      }
    selected.select(AnalyticsTableSchemas.silver.map { case (name, _) => col(name) }*)
  }

  private def snapshot(spark: SparkSession, path: String): (StructType, Vector[String], Long) = {
    val frame = spark.read.format("delta").load(path)
    (frame.schema, frame.toJSON.collect().toVector.sorted, DeltaLogFactory.system(spark, path).update().version)
  }

  test("direct OPEN projection preserves normalized native Silver and quarantine outcomes") {
    withSpark { (spark, execution, root) =>
      def run(
          mixed: Boolean,
          direct: Boolean,
          trial: Int
      ): IO[(Vector[Long], Long, (StructType, Vector[String], Long), (StructType, Vector[String], Long))] = {
        val paths = TestAnalyticsLakehousePaths.unsafe(root.resolve(s"$mixed-$direct-$trial").toUri.toString)
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
          existing <- execution.either(
            OperationalEventTransforms.silver(OperationalEventTransforms.validEvents(seed), keys, markers)
          )
          _ <- writer.merge(writer.withExpiry(existing, at, 30), paths.silver, "target.eventId = source.eventId")
          _ <- execution(
            spark
              .sql(s"ALTER TABLE delta.`${paths.silver}` SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '0')")
              .collect()
          )
          input <- execution(
            if (mixed) parsed(spark, ((0 until 500) ++ (10000 until 10500)).toVector, true)
            else parsed(spark, (10000 until 11000).toVector, false)
          )
          group = "open-silver-selection-" + UUID.randomUUID()
          work = new Work(group)
          result <- Resource
            .make(execution(spark.sparkContext.addSparkListener(work)))(_ =>
              execution(spark.sparkContext.removeSparkListener(work))
            )
            .use { _ =>
              for {
                _ <- execution(completed(spark, work, group))
                initial = work.snapshot
                start <- IO.monotonic
                _ <- SparkStreamingBatchStages.cacheAdmissionFrames(Vector(input), execution).use { _ =>
                  stage
                    .quarantinePreparation(spark, AnalyticsBronzeInput(input, at, 1000L, 1000L, 0L), markers, false)
                    .use { prepared =>
                      execution(selection(prepared, direct)).flatMap { selected =>
                        execution {
                          val plan = selected.queryExecution.optimizedPlan.toString
                          println(
                            s"OPEN_SILVER_PLAN direct=$direct mixed=$mixed trial=$trial innerJoins=${"Join Inner".r.findAllIn(plan).size} leftAntiJoins=${"Join LeftAnti".r.findAllIn(plan).size}"
                          )
                        } *> stage.mergeSilver(prepared.copy(incomingSilver = selected), at).void
                      }
                    }
                }
                silver <- execution(snapshot(spark, paths.silver))
                quarantine <- execution(snapshot(spark, paths.quarantine))
                _ <- execution(completed(spark, work, group + "-end", end = true))
                finish <- IO.monotonic
              } yield (
                work.snapshot.zip(initial).map { case (end, start) => end - start },
                (finish - start).toNanos,
                silver,
                quarantine
              )
            }
        } yield result
      }
      Vector(false, true).traverse_ { mixed =>
        (0 until 3).toVector.traverse_ { trial =>
          val order = if (trial % 2 == 0) Vector(false, true) else Vector(true, false)
          order.traverse(direct => run(mixed, direct, trial).map(direct -> _)).flatMap { values =>
            IO {
              val original = values.find(entry => !entry._1).get._2
              val direct = values.find(_._1).get._2
              assertEquals(direct._3, original._3)
              assertEquals(direct._4, original._4)
              println(
                s"OPEN_SILVER_SELECTION mixed=$mixed trial=$trial baseline=${original._1.mkString(",")} direct=${direct._1.mkString(",")} baselineNanos=${original._2} directNanos=${direct._2} markerJobs=1 setupExcluded=true prepareWritesReadbackFinalizersIncluded=true"
              )
            }
          }
        }
      }
    }
  }
  test("normalized OPEN selection preserves duplicates privacy malformed future closed and prepared supersets") {
    withSpark { (spark, execution, root) =>
      def run(direct: Boolean): IO[((StructType, Vector[String], Long), (StructType, Vector[String], Long))] = {
        val paths = TestAnalyticsLakehousePaths.unsafe(root.resolve(s"edge-$direct").toUri.toString)
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
            val valid = parsed(spark, Vector(100, 101, 104, 105, 106, 107), false)
              .withColumn(
                "occurredAt",
                when(
                  col("eventId") === lit("a08ccb20-0a4d-322d-9bfa-96e87be08bfd"),
                  lit(Timestamp.from(at.plusSeconds(60)))
                )
                  .when(
                    col("eventId") === lit("b43e85e2-2e6a-3aff-9663-943b82c7a312"),
                    lit(Timestamp.from(at.plusSeconds(600)))
                  )
                  .when(
                    col("eventId") === lit("ef543f7d-3f40-3ea4-b421-63fe88093094"),
                    lit(Timestamp.from(at.minusSeconds(3 * 86400)))
                  )
                  .otherwise(col("occurredAt"))
              )
            val malformed = parsed(spark, Vector(103), false)
              .withColumn("eventId", lit(null).cast(StringType))
              .withColumn("rawValue", lit("invalid-envelope"))
              .withColumn("offset", lit(9L))
            valid
              .unionByName(parsed(spark, Vector(100), false).withColumn("offset", lit(6L)))
              .unionByName(parsed(spark, Vector(102), false).withColumn("offset", lit(7L)))
              .unionByName(parsed(spark, Vector(102), true).withColumn("offset", lit(8L)))
              .unionByName(malformed)
          }
          markers <- execution(
            AnalyticsSubjectPrivacy
              .withSubjectToken(OperationalEventTransforms.validEvents(input), keys)
              .filter(col("eventId") === lit("e8421b79-de4b-33d4-9323-d78939c470e2"))
              .select("subjectToken")
          )
          _ <- stage.quarantinePreparation(spark, AnalyticsBronzeInput(input, at, 10L, 9L, 1L), markers, true).use {
            prepared =>
              execution {
                // Actual normalizer retains one identical event100 and removes both conflicting event102s.
                assertEquals(
                  prepared.incomingSilver
                    .filter(col("eventId") === lit("cdeb21df-44ce-3deb-868c-5f5af1e3b52c"))
                    .count(),
                  1L
                )
                assertEquals(
                  prepared.incomingSilver
                    .filter(
                      col("eventId")
                        .isin("7aa3f2c1-61d5-3dbe-a9d8-df183b2e6e73", "e8421b79-de4b-33d4-9323-d78939c470e2")
                    )
                    .count(),
                  0L
                )
                val extra = prepared.incomingSilver
                  .filter(col("eventId") === lit("a08ccb20-0a4d-322d-9bfa-96e87be08bfd"))
                  .select("eventId")
                val superset = prepared.copy(conflicts = prepared.conflicts.unionByName(extra).distinct())
                val selected = selection(superset, direct, at, Some(at.minusSeconds(86400)))
                assertEquals(selected.schema, prepared.incomingSilver.schema)
                if (direct)
                  assertEquals(
                    selected
                      .filter(col("eventId") === lit("a08ccb20-0a4d-322d-9bfa-96e87be08bfd"))
                      .select("occurredAt")
                      .first()
                      .getAs[Timestamp](0)
                      .toInstant,
                    at.plusSeconds(60)
                  )
                superset.copy(incomingSilver = selected)
              }.flatMap(next => stage.mergeSilver(next, at).void)
          }
          rows <- execution {
            val silver = spark.read.format("delta").load(paths.silver)
            assertEquals(
              silver.select("eventId").collect().toVector.map(_.getString(0)).sorted,
              Vector("cdeb21df-44ce-3deb-868c-5f5af1e3b52c", "258507d9-c11b-30ea-b41a-519e5d254cd0").sorted
            )
            assertEquals(silver.filter(col("expiresAt") <= lit(Timestamp.from(at))).count(), 0L)
            val quarantine = spark.read.format("delta").load(paths.quarantine)
            assert(
              !quarantine.columns.exists(name => Set("actorId", "rawValue", "payload", "subjectToken").contains(name))
            )
            (snapshot(spark, paths.silver), snapshot(spark, paths.quarantine))
          }
        } yield rows
      }
      for { original <- run(false); direct <- run(true); _ <- IO(assertEquals(direct, original)) } yield ()
    }
  }

  test("empty normalized OPEN selection preserves native schema initialization") {
    withSpark { (spark, execution, root) =>
      def run(direct: Boolean): IO[((StructType, Vector[String], Long), (StructType, Vector[String], Long))] = {
        val paths = TestAnalyticsLakehousePaths.unsafe(root.resolve(s"empty-$direct").toUri.toString)
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
          input <- execution(parsed(spark, Vector.empty, false))
          markers <- execution(AnalyticsSubjectPrivacy.emptyMarkers(input))
          _ <- stage
            .quarantinePreparation(spark, AnalyticsBronzeInput(input, at, 0L, 0L, 0L), markers, false)
            .use(prepared =>
              execution(selection(prepared, direct))
                .flatMap(selected => stage.mergeSilver(prepared.copy(incomingSilver = selected), at).void)
            )
          rows <- execution((snapshot(spark, paths.silver), snapshot(spark, paths.quarantine)))
        } yield rows
      }
      for { original <- run(false); direct <- run(true); _ <- IO(assertEquals(direct, original)) } yield ()
    }
  }

}
