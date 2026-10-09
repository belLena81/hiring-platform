package com.example.hiring.analytics.adapter.spark

import cats.effect.{IO, Resource}
import cats.syntax.all.*
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

final class WatermarkCandidateSelectionSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 12.minutes
  private def withSpark(check: (SparkSession, SparkExecution[IO], Path) => IO[Unit]): IO[Unit] =
    SparkBlockingExecution.resource[IO].use { execution =>
      val resource = for {
        root <- Resource.make(IO.blocking(Files.createTempDirectory("hiring-watermark-selection-")))(path =>
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
            .appName("WatermarkCandidateSelectionSpec")
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
    private var finalFenceJob: Option[Int] = None
    val finalFenceEnded = new CountDownLatch(1)
    val fenceEnded = new CountDownLatch(1)
    override def onJobStart(event: SparkListenerJobStart): Unit = {
      if (Option(event.properties).exists(_.getProperty("spark.jobGroup.id") == fenceGroup))
        fenceJob = Some(event.jobId)
      if (Option(event.properties).exists(_.getProperty("spark.jobGroup.id") == fenceGroup + "-final"))
        finalFenceJob = Some(event.jobId)
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
      if (finalFenceJob.contains(event.jobId)) finalFenceEnded.countDown()
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

  private def completed(spark: SparkSession, work: Work, group: String, finalFence: Boolean = false): Unit = {
    spark.sparkContext.setJobGroup(group, "Input partitioning listener fence")
    try {
      // One identical tiny job in each variant establishes a public listener FIFO fence.
      assertEquals(spark.sparkContext.parallelize(Seq(1), 1).count(), 1L)
      assert(
        (if (finalFence) work.finalFenceEnded else work.fenceEnded).await(10L, TimeUnit.SECONDS),
        "native listener fence was not observed"
      )
    } finally spark.sparkContext.clearJobGroup()
  }

  import com.example.hiring.analytics.domain.{AnalyticsEventTimePolicy, EventTimeAdmission}
  private val at = Instant.parse("2026-10-03T12:00:00Z")

  private def facts(
      spark: SparkSession,
      count: Int,
      start: Int,
      skillsOnly: Boolean = false,
      distinctTimes: Boolean = false
  ): DataFrame =
    spark.createDataFrame(
      (start until start + count).map { index =>
        val job = skillsOnly || index % 3 == 0
        val statusChange = !job && index % 3 == 2
        val subject = s"subject-$index"
        Row(
          s"event-$index",
          if (job) "JOB_CREATED" else if (statusChange) "APPLICATION_STATUS_CHANGED" else "APPLICATION_CREATED",
          Timestamp.from(if (distinctTimes) at.plusSeconds((index - start).toLong) else at),
          if (job) "Job" else "Application",
          s"aggregate-$index",
          if (job) null else s"application-$index",
          s"job-$index",
          if (statusChange) "Accepted" else null,
          if (job) Seq(" Scala ") else Seq.empty[String],
          subject,
          Seq(subject),
          s"fingerprint-$index"
        )
      }.asJava,
      AnalyticsTableSchemas.struct(AnalyticsTableSchemas.silver)
    )

  private def visible(existing: DataFrame, candidates: DataFrame): Vector[Instant] =
    StreamingWatermarkAdmission
      .visibleCandidateEvents(existing, candidates, "effectiveTime")
      .select("effectiveTime")
      .distinct()
      .collect()
      .toVector
      .map(_.getAs[Timestamp](0).toInstant)
      .sorted

  private def candidates(
      incoming: DataFrame,
      selected: DataFrame,
      classified: DataFrame,
      existing: DataFrame,
      coordinates: DataFrame,
      borrow: Boolean,
      recovery: Boolean
  ): DataFrame = {
    val root = if (borrow && !recovery) classified else selected
    val pairs = existing.select("eventId", "eventFingerprint").distinct()
    val admitted = root
      .select("eventId", "eventFingerprint")
      .join(pairs, Seq("eventId", "eventFingerprint"), "inner")
      .select("eventId")
      .distinct()
    val sourceCoordinates = root.select("eventId", "topic", "partition", "offset", "eventFingerprint")
    val sameCoordinates = sourceCoordinates
      .join(coordinates, Seq("topic", "partition", "offset", "eventFingerprint"), "inner")
      .select("eventId")
      .distinct()
    val fresh = root
      .select("eventId")
      .distinct()
      .join(admitted, Seq("eventId"), "left_anti")
      .join(sameCoordinates, Seq("eventId"), "left_anti")
    val restart =
      if (recovery)
        root
          .select("eventId")
          .distinct()
          .join(sameCoordinates, Seq("eventId"), "inner")
          .join(admitted, Seq("eventId"), "left_anti")
      else root.limit(0).select("eventId")
    val ids = fresh.unionByName(restart).distinct()
    val open = classified
      .join(ids, Seq("eventId"), "inner")
      .filter(col("status") === lit("OPEN"))
      .dropDuplicates("eventId")
      .select("eventId", "effectiveTime")
    incoming.join(open, Seq("eventId"), "inner")
  }

  test("existing classified cache preserves native watermark candidate selection without a new cache") {
    withSpark { (spark, execution, root) =>
      def run(
          borrow: Boolean,
          mixed: Boolean,
          recovery: Boolean
      ): IO[(Vector[Long], Vector[Instant], Vector[String], Long)] = {
        val group = "watermark-selection-" + UUID.randomUUID()
        val work = new Work(group)
        Resource
          .make(execution(spark.sparkContext.addSparkListener(work)))(_ =>
            execution(spark.sparkContext.removeSparkListener(work))
          )
          .use { _ =>
            for {
              _ <- execution(completed(spark, work, group))
              initial = work.snapshot
              start <- IO.monotonic
              result <- Resource
                .eval(execution {
                  val existing = spark.read.format("delta").load(root.resolve("silver").toString)
                  val raw = spark.read.format("delta").load(root.resolve(if (mixed) "mixed" else "zero").toString)
                  val incoming = raw
                    .withColumn("topic", lit("hiring.watermark.test"))
                    .withColumn("partition", pmod(xxhash64(col("eventId")), lit(3)).cast(IntegerType))
                    .withColumn("offset", xxhash64(col("eventId")))
                  val conflicts = incoming
                    .alias("source")
                    .join(existing.alias("stored"), Seq("eventId"), "inner")
                    .filter(col("source.eventFingerprint") =!= col("stored.eventFingerprint"))
                    .select("eventId")
                    .distinct()
                  val selected = incoming.join(conflicts, Seq("eventId"), "left_anti")
                  val classified = classifyFrame(selected, at.plusSeconds(2000), Some(at.minusSeconds(86400)))
                  val coordinates = incoming
                    .filter(col("eventId").isin((7500 until 7510).map(i => s"event-$i")*))
                    .select("topic", "partition", "offset", "eventFingerprint")
                  (incoming, existing, conflicts, selected, classified, coordinates)
                })
                .flatMap { frames =>
                  // These are exactly the existing incoming/conflict/classification cache owners, no extra candidate cache.
                  SparkStreamingBatchStages
                    .cacheAdmissionFrames(Vector(frames._1, frames._3, frames._5), execution)
                    .as(frames)
                }
                .use { frames =>
                  execution {
                    val categories = frames._5
                      .filter(col("status").isin("FUTURE", "CLOSED"))
                      .select(col("status").as(SparkStreamingBatchStages.AdmissionQualityCategory))
                    val quality =
                      SparkStreamingBatchStages.measureClassifiedQuality(frames._1.limit(0), frames._3, categories)
                    assertEquals(quality.future, 0L)
                    val selected = candidates(frames._1, frames._4, frames._5, frames._2, frames._6, borrow, recovery)
                    val times = visible(frames._2, selected)
                    if (recovery) {
                      val original = candidates(frames._1, frames._4, frames._5, frames._2, frames._6, false, true)
                      assert(
                        selected.queryExecution.optimizedPlan.sameResult(original.queryExecution.optimizedPlan),
                        "recovery must retain the original selectedValid projection graph"
                      )
                    }
                    val candidatePlan = selected.queryExecution.executedPlan.toString
                    assert(
                      candidatePlan.contains("InMemoryTableScan") && candidatePlan.contains("status#"),
                      "complete candidate graph must consume the existing classifications cache"
                    )
                    val identities = selected
                      .select("eventId", "eventFingerprint", "topic", "partition", "offset", "effectiveTime")
                      .toJSON
                      .collect()
                      .toVector
                      .sorted
                    // Public explain output records cache substitution; it does not fabricate execution counters.
                    val keyRoot = if (borrow && !recovery) frames._5 else frames._4
                    val keyProjection = keyRoot.select("eventId", "eventFingerprint")
                    val optimized = keyProjection.queryExecution.optimizedPlan.toString
                    val executed = keyProjection.queryExecution.executedPlan.toString
                    if (borrow && !recovery) {
                      assert(executed.contains("InMemoryTableScan"))
                      assert(
                        optimized.contains("InMemoryRelation") && optimized.contains("status#"),
                        "existing classified cache must back candidate keys"
                      )
                    } else assert(!optimized.contains("status#"), "baseline key root must not borrow classified cache")
                    (times, identities)
                  }
                }
              _ <- execution(completed(spark, work, group + "-final", finalFence = true))
              finish <- IO.monotonic
            } yield (
              work.snapshot.zip(initial).map { case (end, start) => end - start },
              result._1,
              result._2,
              (finish - start).toNanos
            )
          }
      }
      for {
        _ <- execution {
          facts(spark, 7500, 0).write
            .format("delta")
            .option("delta.dataSkippingNumIndexedCols", "0")
            .save(root.resolve("silver").toString)
          facts(spark, 1000, 7500, distinctTimes = true).write
            .format("delta")
            .option("delta.dataSkippingNumIndexedCols", "0")
            .save(root.resolve("zero").toString)
          facts(spark, 100, 0, distinctTimes = true)
            .unionByName(facts(spark, 900, 7500, distinctTimes = true))
            .withColumn(
              "eventFingerprint",
              when(col("eventId").isin((0 until 100 by 7).map(i => s"event-$i")*), lit("different"))
                .otherwise(col("eventFingerprint"))
            )
            .write
            .format("delta")
            .option("delta.dataSkippingNumIndexedCols", "0")
            .save(root.resolve("mixed").toString)
        }
        _ <- Vector(false, true).traverse_ { mixed =>
          Vector(false, true).traverse_ { recovery =>
            (0 until 3).toVector.traverse_ { trial =>
              val order = if (trial % 2 == 0) Vector(false, true) else Vector(true, false)
              order.traverse(borrow => run(borrow, mixed, recovery).map(borrow -> _)).flatMap { values =>
                IO {
                  val before = values.find(entry => !entry._1).get._2
                  val after = values.find(_._1).get._2
                  assertEquals(after._2, before._2)
                  assertEquals(after._3, before._3)
                  val maximum = if (mixed) 900 else 1000
                  val first = if (recovery) 0 else 10
                  assertEquals(after._2, (first until maximum).map(index => at.plusSeconds(index.toLong)).toVector)
                  assertEquals(after._3.size, maximum - first)
                  println(
                    s"WATERMARK_SELECTION mixed=$mixed recovery=$recovery trial=$trial baseline=${before._1.mkString(",")} borrowed=${after._1.mkString(",")} baselineNanos=${before._4} borrowedNanos=${after._4} markerJobs=1 qualityIncluded=true extraCache=false"
                  )
                }
              }
            }
          }
        }
      } yield ()
    }
  }
  test("watermark visibility retains K9 K10 duplicate and empty candidate semantics") {
    withSpark { (spark, execution, _) =>
      execution {
        val empty = facts(spark, 0, 0, skillsOnly = true)
        val candidate = facts(spark, 1, 100, skillsOnly = true).withColumn("effectiveTime", col("occurredAt"))
        assertEquals(visible(facts(spark, 8, 0, skillsOnly = true), candidate), Vector.empty[Instant])
        assertEquals(visible(facts(spark, 9, 0, skillsOnly = true), candidate), Vector(at))
        assertEquals(visible(facts(spark, 9, 0, skillsOnly = true), candidate.unionByName(candidate)), Vector(at))
        assertEquals(visible(empty, candidate.limit(0)), Vector.empty[Instant])
      }
    }
  }

  test("existing classification cache cleanup preserves normal error cancellation and external ownership") {
    withSpark { (spark, execution, _) =>
      for {
        frame <- execution(facts(spark, 10, 0).withColumn("effectiveTime", col("occurredAt")))
        _ <- SparkStreamingBatchStages
          .cacheAdmissionFrames(Vector(frame), execution)
          .use(_ => execution(frame.count()).void)
        _ <- execution(assertEquals(frame.storageLevel, StorageLevel.NONE))
        failure <- SparkStreamingBatchStages
          .cacheAdmissionFrames(Vector(frame), execution)
          .use(_ => IO.raiseError[Unit](new IllegalStateException("fixture")))
          .attempt
        _ <- IO(assert(failure.isLeft))
        _ <- execution(assertEquals(frame.storageLevel, StorageLevel.NONE))
        started <- cats.effect.Deferred[IO, Unit]
        fiber <- SparkStreamingBatchStages
          .cacheAdmissionFrames(Vector(frame), execution)
          .use(_ => started.complete(()).void *> IO.never[Unit])
          .start
        _ <- started.get *> fiber.cancel *> fiber.join
        _ <- execution(assertEquals(frame.storageLevel, StorageLevel.NONE))
        _ <- Resource
          .make(execution(frame.persist(StorageLevel.MEMORY_AND_DISK)))(_ =>
            execution(frame.unpersist(blocking = true)).void
          )
          .use { _ =>
            SparkStreamingBatchStages.cacheAdmissionFrames(Vector(frame), execution).use(_ => IO.unit) *>
              execution(assertEquals(frame.storageLevel, StorageLevel.MEMORY_AND_DISK))
          }
      } yield ()
    }
  }

  private def classifyFrame(
      source: DataFrame,
      observed: Instant = at,
      watermark: Option[Instant] = Some(at.minusSeconds(86400))
  ): DataFrame = {
    val observedAt = observed
    val acceptedWatermark = watermark
    val schema = StructType(
      Vector(StructField("status", StringType, false), StructField("effectiveTime", TimestampType, true))
    )
    val classify = udf(
      new org.apache.spark.sql.api.java.UDF1[Timestamp, Row] {
        override def call(value: Timestamp): Row = if (value == null) Row("FUTURE", null)
        else
          AnalyticsEventTimePolicy.admit(value.toInstant, observedAt, acceptedWatermark) match {
            case EventTimeAdmission.Admitted(effective) => Row("OPEN", Timestamp.from(effective))
            case EventTimeAdmission.LateClosedDay(_)    => Row("CLOSED", null)
            case EventTimeAdmission.TooFarInFuture      => Row("FUTURE", null)
          }
      },
      schema
    )
    source
      .withColumn("_admission", classify(col("occurredAt")))
      .withColumn("status", col("_admission.status"))
      .withColumn("effectiveTime", col("_admission.effectiveTime"))
  }

  test("borrowed classifications retain future closed null duplicate and active marker exclusions") {
    withSpark { (spark, execution, _) =>
      for {
        frames <- execution {
          val base = facts(spark, 15, 100, skillsOnly = true)
            .withColumn(
              "occurredAt",
              when(col("eventId") === lit("event-100"), lit(Timestamp.from(at.plusSeconds(600))))
                .when(col("eventId") === lit("event-101"), lit(Timestamp.from(at.minusSeconds(3 * 86400))))
                .when(col("eventId") === lit("event-102"), lit(null).cast(TimestampType))
                .otherwise(col("occurredAt"))
            )
            .withColumn("topic", lit("hiring.watermark.test"))
            .withColumn("partition", lit(0))
            .withColumn("offset", xxhash64(col("eventId")))
          val duplicated = base.unionByName(base.filter(col("eventId") === lit("event-103")))
          val markers = base.filter(col("eventId") === lit("event-104")).select("subjectToken")
          val safe = AnalyticsSubjectPrivacy
            .excludeActiveDeletionMarkers(duplicated, markers)
            .fold(error => throw error, identity)
          val conflicts = safe.filter(col("eventId") === lit("event-105")).select("eventId")
          val selected = safe.join(conflicts, Seq("eventId"), "left_anti")
          val classified = classifyFrame(selected)
          val existing = facts(spark, 9, 0, skillsOnly = true)
          val coordinates = selected.limit(0).select("topic", "partition", "offset", "eventFingerprint")
          (safe, conflicts, selected, classified, existing, coordinates)
        }
        _ <- SparkStreamingBatchStages.cacheAdmissionFrames(Vector(frames._1, frames._2, frames._4), execution).use {
          _ =>
            execution {
              val categories = frames._4
                .filter(col("status").isin("FUTURE", "CLOSED"))
                .select(col("status").as(SparkStreamingBatchStages.AdmissionQualityCategory))
              val quality =
                SparkStreamingBatchStages.measureClassifiedQuality(frames._1.limit(0), frames._2, categories)
              assertEquals(quality, SparkStreamingBatchStages.AdmissionQuality(0L, 1L, 2L, 1L))
              val variants = Vector(false, true).map { borrow =>
                val selected = candidates(frames._1, frames._3, frames._4, frames._5, frames._6, borrow, false)
                assertEquals(
                  selected.schema,
                  candidates(frames._1, frames._3, frames._4, frames._5, frames._6, false, false).schema
                )
                val ids = selected.select("eventId").collect().toVector.map(_.getString(0)).sorted
                (ids, visible(frames._5, selected))
              }
              assertEquals(variants(1), variants(0))
              // This reduced helper fixture retains duplicate incoming Silver rows; the join preserves both.
              // Production OperationalEventTransforms.silver performs its own normalization separately.
              assertEquals(
                variants(1)._1,
                ((103 until 115).filterNot(i => i == 104 || i == 105).map(i => s"event-$i").toVector :+
                  "event-103").sorted
              )
              assertEquals(variants(1)._2, Vector(at))
              // Prepared-only conflicts are still applied by the untouched writing category path.
              val prepared =
                frames._2.unionByName(frames._1.filter(col("eventId") === lit("event-100")).select("eventId"))
              val writing = frames._4
                .filter(col("status").isin("FUTURE", "CLOSED"))
                .join(prepared, Seq("eventId"), "left_anti")
                .select(col("status").as(SparkStreamingBatchStages.AdmissionQualityCategory))
              assertEquals(
                SparkStreamingBatchStages.measureClassifiedQuality(frames._1.limit(0), prepared, writing).future,
                1L
              )
            }
        }
      } yield ()
    }
  }

  test("identical projected roots preserve framework and privacy schema failures") {
    withSpark { (spark, execution, _) =>
      execution {
        val source = facts(spark, 1, 100, skillsOnly = true)
        val markers = AnalyticsSubjectPrivacy.emptyMarkers(source)
        val missingTokens = source.drop("subjectTokens")
        val originalPrivacyError = AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(missingTokens, markers)
        assert(originalPrivacyError.isLeft)
        assertEquals(
          AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(classifyFrame(missingTokens), markers),
          originalPrivacyError
        )
        def failure(action: => Any): Option[Class[?]] = try { val _ = action; None }
        catch { case error: org.apache.spark.sql.AnalysisException => Some(error.getClass) }
        val invalid = source.drop("eventFingerprint")
        val classified = classifyFrame(invalid)
        val old = failure(invalid.select("eventId", "eventFingerprint").collect())
        assert(old.nonEmpty)
        assertEquals(failure(classified.select("eventId", "eventFingerprint").collect()), old)
      }
    }
  }

  test("policy effective timestamps clamp microseconds and recovery requires exact coordinate fingerprints") {
    withSpark { (spark, execution, _) =>
      execution {
        val observed = at.plusNanos(123456000L)
        val source = facts(spark, 2, 100, skillsOnly = true)
          .withColumn("occurredAt", lit(Timestamp.from(observed.plusSeconds(1))))
          .withColumn("topic", lit("hiring.watermark.test"))
          .withColumn("partition", lit(0))
          .withColumn("offset", xxhash64(col("eventId")))
        val classified = classifyFrame(source, observed, None)
        assertEquals(
          classified.select("effectiveTime").collect().toVector.map(_.getAs[Timestamp](0).toInstant),
          Vector(observed, observed)
        )
        val coordinates = source
          .select("topic", "partition", "offset", "eventFingerprint")
          .withColumn(
            "eventFingerprint",
            when(col("eventFingerprint") === lit("fingerprint-100"), lit("mismatch")).otherwise(col("eventFingerprint"))
          )
        val empty = source.limit(0)
        Vector(false, true).foreach { recovery =>
          val variants = Vector(false, true).map { borrow =>
            candidates(source, source, classified, empty, coordinates, borrow, recovery)
              .select("eventId")
              .collect()
              .toVector
              .map(_.getString(0))
              .sorted
          }
          assertEquals(variants(1), variants(0))
          assertEquals(variants(1), if (recovery) Vector("event-100", "event-101") else Vector("event-100"))
        }
        val unknown = classified.withColumn("status", lit("UNKNOWN"))
        assertEquals(candidates(source, source, unknown, empty, coordinates, true, true).count(), 0L)
      }
    }
  }

}
