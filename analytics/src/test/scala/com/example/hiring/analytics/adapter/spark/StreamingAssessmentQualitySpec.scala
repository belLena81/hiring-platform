package com.example.hiring.analytics.adapter.spark

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import munit.CatsEffectSuite
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.*
import org.apache.spark.storage.StorageLevel
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.UUID
import java.time.Instant
import java.sql.Timestamp
import com.example.hiring.analytics.domain.{AnalyticsEventTimePolicy, EventTimeAdmission}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.*
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class StreamingAssessmentQualitySpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 8.minutes
  private def withSpark(check: (SparkSession, SparkExecution[IO], Path) => IO[Unit]): IO[Unit] =
    SparkBlockingExecution.resource[IO].use { execution =>
      val resource = for {
        root <- Resource.make(IO.blocking(Files.createTempDirectory("hiring-admission-quality-")))(path =>
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
            .appName("StreamingAssessmentQualitySpec")
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
    val shuffleRead = new AtomicLong()
    val shuffleWrite = new AtomicLong()
    val memorySpill = new AtomicLong()
    val diskSpill = new AtomicLong()
    override def onJobEnd(event: SparkListenerJobEnd): Unit = {
      jobs.incrementAndGet()
      if (fenceJob.contains(event.jobId)) fenceEnded.countDown()
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

  private def completed(spark: SparkSession, work: Work, group: String): Unit = {
    spark.sparkContext.setJobGroup(group, "Admission quality listener fence")
    try {
      // One identical tiny job in each variant establishes a public listener FIFO fence.
      assertEquals(spark.sparkContext.parallelize(Seq(1), 1).count(), 1L)
      assert(work.fenceEnded.await(10L, TimeUnit.SECONDS), "native listener fence was not observed")
    } finally spark.sparkContext.clearJobGroup()
  }

  private def gcSnapshot: Vector[Long] = {
    val beans = java.lang.management.ManagementFactory.getGarbageCollectorMXBeans.asScala.toVector
    Vector(beans.map(_.getCollectionCount).filter(_ >= 0L).sum, beans.map(_.getCollectionTime).filter(_ >= 0L).sum)
  }

  private val category = SparkStreamingBatchStages.AdmissionQualityCategory
  private val at = Instant.parse("2026-09-20T12:00:00Z")
  private val watermark = Instant.parse("2026-09-19T00:00:00Z")
  private val schema = StructType(
    Vector(
      StructField(Columns.EventId, StringType, nullable = true),
      StructField("fingerprint", StringType, nullable = false),
      StructField("sourceOffset", LongType, nullable = false),
      StructField("occurredAt", TimestampType, nullable = true)
    )
  )

  private def incoming(spark: SparkSession, mixed: Boolean): DataFrame =
    spark.createDataFrame(
      (0 until 1000).map { index =>
        val id = if (index % 100 == 0) null else s"${if (mixed) "event" else "new-event"}-${index / 2}"
        val time = index % 4 match {
          case 0 => Timestamp.from(at)
          case 1 => Timestamp.from(at.plusSeconds(600))
          case 2 => Timestamp.from(at.minusSeconds(3 * 86400))
          case _ => null
        }
        Row(
          id,
          if (mixed && index % 5 == 0) "different" else "stored",
          if (mixed) index.toLong else index.toLong + 10000L,
          time
        )
      }.asJava,
      schema
    )

  private def classified(frame: DataFrame): DataFrame = {
    val observedAt = at
    val acceptedWatermark = Some(watermark)
    val classify = udf(
      new org.apache.spark.sql.api.java.UDF1[Timestamp, String] {
        override def call(timestamp: Timestamp): String = {
          if (timestamp == null) "FUTURE"
          else
            AnalyticsEventTimePolicy.admit(timestamp.toInstant, observedAt, acceptedWatermark) match {
              case EventTimeAdmission.Admitted(_)      => "OPEN"
              case EventTimeAdmission.LateClosedDay(_) => "CLOSED"
              case EventTimeAdmission.TooFarInFuture   => "FUTURE"
            }
        }
      },
      StringType
    )
    frame.withColumn("status", classify(col("occurredAt")))
  }

  private def aggregate(
      malformed: DataFrame,
      conflicts: DataFrame,
      facts: DataFrame,
      fused: Boolean
  ): SparkStreamingBatchStages.AdmissionQuality = {
    val selected = facts.filter(col("status").isin("FUTURE", "CLOSED"))
    val categories = (if (fused) selected else selected.join(conflicts, Seq(Columns.EventId), "left_anti"))
      .select(col("status").as(category))
    SparkStreamingBatchStages.measureClassifiedQuality(malformed, conflicts, categories)
  }

  test("assessment quality categories preserve prefiltered native retained-history counts") {
    withSpark { (spark, execution, root) =>
      val history = root.resolve("history").toString
      def run(fused: Boolean, mixed: Boolean): IO[(SparkStreamingBatchStages.AdmissionQuality, Vector[Long], Long)] = {
        val group = "admission-quality-" + UUID.randomUUID()
        val listener = new Work(group)
        Resource
          .make(execution(spark.sparkContext.addSparkListener(listener)))(_ =>
            execution(spark.sparkContext.removeSparkListener(listener))
          )
          .use { _ =>
            for {
              _ <- execution(completed(spark, listener, group))
              initial = listener.snapshot
              gcBefore <- IO(gcSnapshot)
              started <- IO.monotonic
              frames <- execution {
                val source = incoming(spark, mixed)
                val stored = spark.read.format("delta").load(history)
                val historical = source
                  .alias("incoming")
                  .join(stored.alias("stored"), Seq(Columns.EventId), "inner")
                  .filter(col("incoming.fingerprint") =!= col("stored.fingerprint"))
                  .select(Columns.EventId)
                val local = source
                  .groupBy(Columns.EventId)
                  .agg(countDistinct("fingerprint").as("variants"))
                  .filter(col("variants") > lit(1L))
                  .select(Columns.EventId)
                val coordinates = source
                  .alias("incoming")
                  .join(stored.alias("stored"), Seq("sourceOffset"), "inner")
                  .filter(col("incoming.fingerprint") =!= col("stored.coordinateFingerprint"))
                  .select(col("incoming.eventId").as(Columns.EventId))
                  .filter(col(Columns.EventId).isNotNull)
                val conflicts = historical.unionByName(local).unionByName(coordinates).distinct()
                (
                  source.filter(col("occurredAt").isNull),
                  conflicts,
                  classified(source.join(conflicts, Seq(Columns.EventId), "left_anti"))
                )
              }
              quality <- SparkStreamingBatchStages
                .cacheAdmissionFrames(Vector(frames._2, frames._3), execution)
                .use(_ => execution(aggregate(frames._1, frames._2, frames._3, fused)))
              _ <- execution {
                val lastGroup = group + "-last"
                val last = new Work(lastGroup)
                spark.sparkContext.addSparkListener(last)
                try completed(spark, last, lastGroup)
                finally spark.sparkContext.removeSparkListener(last)
              }
              ended <- IO.monotonic
              gcAfter <- IO(gcSnapshot)
              _ <- IO(
                println(
                  s"ASSESSMENT_QUALITY_GC fused=$fused mixed=$mixed collections=${gcAfter(0) - gcBefore(0)} millis=${gcAfter(1) - gcBefore(1)} scope=setup-through-final-fence"
                )
              )
            } yield (
              quality,
              listener.snapshot.zip(initial).map { case (end, start) => end - start },
              (ended - started).toNanos
            )
          }
      }
      for {
        original <- execution {
          spark
            .range(7500)
            .select(
              concat(lit("event-"), col("id")).as(Columns.EventId),
              lit("stored").as("fingerprint"),
              col("id").as("sourceOffset"),
              when(col("id") % lit(11L) === lit(0L), lit("coordinate-different"))
                .otherwise(lit("stored"))
                .as("coordinateFingerprint")
            )
            .write
            .format("delta")
            .option("delta.dataSkippingNumIndexedCols", "0")
            .save(history)
          (spark.read.format("delta").load(history).schema, DeltaLogFactory.system(spark, history).update().version)
        }
        _ <- Vector(false, true).traverse_ { mixed =>
          (0 until 3).toVector.traverse_ { trial =>
            val order = if (trial % 2 == 0) Vector(false, true) else Vector(true, false)
            order.traverse(fused => run(fused, mixed).map(fused -> _)).flatMap { variants =>
              val before = variants.find(entry => !entry._1).get._2
              val after = variants.find(_._1).get._2
              IO {
                assertEquals(after._1, before._1)
                // Source rows 250 nullable timestamps, 500 FUTURE and 250 CLOSED. Mixed conflicts
                // cover both repeated identities and statuses; derive explicit expected rows below.
                val source = (0 until 1000).toVector
                val blocked =
                  if (mixed) source.filter(i => i % 100 != 0 && (i % 5 == 0 || i % 11 == 0)).map(_ / 2).toSet
                  else Set.empty[Int]
                def retained(i: Int): Boolean = i % 100 == 0 || !blocked.contains(i / 2)
                val expected = SparkStreamingBatchStages.AdmissionQuality(
                  250L,
                  blocked.size.toLong,
                  source.count(i => retained(i) && (i % 4 == 1 || i % 4 == 3)).toLong,
                  source.count(i => retained(i) && i % 4 == 2).toLong
                )
                assertEquals(after._1, expected)
                println(
                  s"ASSESSMENT_QUALITY mixed=$mixed trial=$trial baselineWork=${before._2.mkString(",")} fusedWork=${after._2.mkString(",")} baselineNanos=${before._3} fusedNanos=${after._3} markerJobs=1 initialFenceExcluded=true setupIncluded=true"
                )
              }
            }
          }
        }
        _ <- execution {
          assertEquals(spark.read.format("delta").load(history).schema, original._1)
          assertEquals(DeltaLogFactory.system(spark, history).update().version, original._2)
          assertEquals(spark.read.format("delta").load(history).count(), 7500L)
        }
      } yield ()
    }
  }

  test("empty unknown nullable and repeated categories preserve four-branch count semantics") {
    withSpark { (spark, execution, _) =>
      execution {
        val shape =
          StructType(Vector(StructField(Columns.EventId, StringType, true), StructField("status", StringType, true)))
        val facts = spark.createDataFrame(
          Vector(
            Row("repeat", "CLOSED"),
            Row("repeat", "CLOSED"),
            Row(null, "FUTURE"),
            Row("open", "OPEN"),
            Row("unknown", "UNKNOWN"),
            Row("null", null)
          ).asJava,
          shape
        )
        val conflicts = facts.filter(col("status") === lit("OPEN")).select(Columns.EventId)
        val malformed = facts.filter(col("status").isNull)
        assertEquals(aggregate(malformed, conflicts, facts, true), aggregate(malformed, conflicts, facts, false))
        assertEquals(
          aggregate(malformed, conflicts, facts, true),
          SparkStreamingBatchStages.AdmissionQuality(1L, 1L, 1L, 2L)
        )
        val empty = facts.limit(0)
        assertEquals(
          aggregate(empty, empty.select(Columns.EventId), empty, true),
          SparkStreamingBatchStages.AdmissionQuality(0L, 0L, 0L, 0L)
        )
      }
    }
  }
  test("internal category shape validation matches the original category aggregate kernel") {
    withSpark { (spark, execution, _) =>
      execution {
        val empty = incoming(spark, false).limit(0)
        def baseline(categories: DataFrame): SparkStreamingBatchStages.AdmissionQuality = {
          val records = empty
            .select(lit("MALFORMED").as(category))
            .unionByName(empty.select(Columns.EventId).distinct().select(lit("CONFLICT").as(category)))
            .unionByName(categories.select(category))
          val result = records
            .agg(
              count(when(col(category) === lit("MALFORMED"), lit(1))),
              count(when(col(category) === lit("CONFLICT"), lit(1))),
              count(when(col(category) === lit("FUTURE"), lit(1))),
              count(when(col(category) === lit("CLOSED"), lit(1)))
            )
            .head()
          SparkStreamingBatchStages.AdmissionQuality(
            result.getLong(0),
            result.getLong(1),
            result.getLong(2),
            result.getLong(3)
          )
        }
        def result(
            action: => SparkStreamingBatchStages.AdmissionQuality
        ): Either[Class[?], SparkStreamingBatchStages.AdmissionQuality] =
          try Right(action)
          catch { case error: org.apache.spark.sql.AnalysisException => Left(error.getClass) }
        val shape = StructType(Vector(StructField(category, StringType, true)))
        val unknown = spark.createDataFrame(Vector(Row("UNKNOWN"), Row(null)).asJava, shape)
        Vector(empty.select(Columns.EventId), empty.select(array(lit("FUTURE")).as(category)), unknown).foreach {
          categories =>
            assertEquals(
              result(SparkStreamingBatchStages.measureClassifiedQuality(empty, empty, categories)),
              result(baseline(categories))
            )
        }
        assertEquals(baseline(unknown), SparkStreamingBatchStages.AdmissionQuality(0L, 0L, 0L, 0L))
      }
    }
  }

  test("four input API retains duplicate nullable empty counts and invalid conflict schema error class") {
    withSpark { (spark, execution, _) =>
      execution {
        val shape =
          StructType(Vector(StructField(Columns.EventId, StringType, true), StructField("status", StringType, true)))
        val facts = spark.createDataFrame(
          Vector(
            Row("repeat", "CLOSED"),
            Row("repeat", "CLOSED"),
            Row(null, "FUTURE"),
            Row("open", "OPEN"),
            Row("null", null)
          ).asJava,
          shape
        )
        val malformed = facts.filter(col("status").isNull)
        val conflicts = facts.filter(col("status") === lit("OPEN"))
        val future = facts.filter(col("status") === lit("FUTURE"))
        val closed = facts.filter(col("status") === lit("CLOSED"))
        assertEquals(
          AdmissionQualityBaseline.measure(malformed, conflicts, future, closed),
          SparkStreamingBatchStages.AdmissionQuality(1L, 1L, 1L, 2L)
        )
        val empty = facts.limit(0)
        assertEquals(
          AdmissionQualityBaseline.measure(empty, empty, empty, empty),
          SparkStreamingBatchStages.AdmissionQuality(0L, 0L, 0L, 0L)
        )
        def failure(action: => Any): Option[Class[?]] =
          try { val _ = action; None }
          catch { case error: org.apache.spark.sql.AnalysisException => Some(error.getClass) }
        val invalid = facts.select("status")
        // Exact original source boundary also requires EventId on the conflicting input.
        val originalFailure = failure {
          val records = malformed
            .select(lit("MALFORMED").as(category))
            .unionByName(invalid.select(Columns.EventId).distinct().select(lit("CONFLICT").as(category)))
            .unionByName(future.select(lit("FUTURE").as(category)))
            .unionByName(closed.select(lit("CLOSED").as(category)))
          records.count()
        }
        assert(originalFailure.nonEmpty)
        assertEquals(
          failure(AdmissionQualityBaseline.measure(malformed, invalid, future, closed)),
          originalFailure
        )
      }
    }
  }

  test("admission cache ownership releases on error cancellation and preserves an external owner") {
    withSpark { (spark, execution, _) =>
      for {
        frame <- execution(classified(incoming(spark, false)))
        failed <- SparkStreamingBatchStages
          .cacheAdmissionFrames(Vector(frame), execution)
          .use(_ => IO.raiseError[Unit](new IllegalStateException("fixture")))
          .attempt
        _ <- IO(assert(failed.isLeft))
        _ <- execution(assertEquals(frame.storageLevel, StorageLevel.NONE))
        started <- cats.effect.Deferred[IO, Unit]
        fiber <- SparkStreamingBatchStages
          .cacheAdmissionFrames(Vector(frame), execution)
          .use(_ => started.complete(()).void *> IO.never[Unit])
          .start
        _ <- started.get *> fiber.cancel *> fiber.join
        _ <- execution(assertEquals(frame.storageLevel, StorageLevel.NONE))
        _ <- Resource
          .make(execution(frame.persist(StorageLevel.MEMORY_AND_DISK)))(_ => execution(frame.unpersist()).void)
          .use { _ =>
            SparkStreamingBatchStages.cacheAdmissionFrames(Vector(frame), execution).use(_ => IO.unit) *>
              execution(assertEquals(frame.storageLevel, StorageLevel.MEMORY_AND_DISK))
          }
        _ <- execution(assertEquals(frame.storageLevel, StorageLevel.NONE))
      } yield ()
    }
  }
  test("ingestion retains prepared-only future and closed conflict exclusions") {
    withSpark { (spark, execution, _) =>
      execution {
        val shape =
          StructType(Vector(StructField(Columns.EventId, StringType, true), StructField("status", StringType, true)))
        val source = spark.createDataFrame(
          Vector(
            Row("fresh", "FUTURE"),
            Row("prepared-future", "FUTURE"),
            Row("prepared-closed", "CLOSED"),
            Row("retained", "CLOSED"),
            Row("retained", "CLOSED")
          ).asJava,
          shape
        )
        val fresh = source.filter(col(Columns.EventId) === lit("fresh")).select(Columns.EventId)
        val prepared = source
          .filter(col(Columns.EventId).isin("fresh", "prepared-future", "prepared-closed"))
          .select(Columns.EventId)
        val classifications = source.join(fresh, Seq(Columns.EventId), "left_anti")
        val empty = source.limit(0)
        assertEquals(
          aggregate(empty, fresh, classifications, true),
          SparkStreamingBatchStages.AdmissionQuality(0L, 1L, 1L, 3L)
        )
        // Writing keeps the original repeated anti-join against the prepared superset.
        assertEquals(
          aggregate(empty, prepared, classifications, false),
          SparkStreamingBatchStages.AdmissionQuality(0L, 3L, 0L, 2L)
        )
      }
    }
  }

}
