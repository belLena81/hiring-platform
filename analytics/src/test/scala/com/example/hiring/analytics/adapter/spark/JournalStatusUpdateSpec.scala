package com.example.hiring.analytics.adapter.spark

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import io.delta.tables.DeltaTable
import munit.CatsEffectSuite
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.*
import java.sql.Timestamp
import java.time.Instant
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.service.streaming.*
import com.example.hiring.analytics.TestAnalyticsLakehousePaths
import org.apache.spark.sql.functions.*
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class JournalStatusUpdateSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 8.minutes
  private def withSpark(check: (SparkSession, SparkExecution[IO], Path) => IO[Unit]): IO[Unit] =
    SparkBlockingExecution.resource[IO].use { execution =>
      val resource = for {
        root <- Resource.make(IO.blocking(Files.createTempDirectory("hiring-journal-status-update-")))(path =>
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
            .appName("JournalStatusUpdateSpec")
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
    spark.sparkContext.setJobGroup(group, "Journal status update listener fence")
    try {
      // One identical tiny job in each variant establishes a public listener FIFO fence.
      assertEquals(spark.sparkContext.parallelize(Seq(1), 1).count(), 1L)
      assert(work.fenceEnded.await(10L, TimeUnit.SECONDS), "native listener fence was not observed")
    } finally spark.sparkContext.clearJobGroup()
  }

  private def right[A](value: Either[String, A]): A = value.fold(fail(_), identity)
  private val lineage = right(StreamingLineage.from("journal-update"))
  private val at = Timestamp.from(Instant.parse("2026-10-04T12:00:00.123456Z"))
  private final case class Change(
      existing: String,
      targetLineage: String,
      batch: Long,
      status: String,
      candidate: Option[Timestamp],
      completed: Option[Timestamp]
  )

  private def oldMerge(spark: SparkSession, path: String, change: Change): Unit = {
    val schema = StructType(
      Vector(
        StructField("lineage", StringType, false),
        StructField("batchId", LongType, false),
        StructField("outcome", StringType, false),
        StructField("candidateWatermark", TimestampType, true),
        StructField("completedAt", TimestampType, true)
      )
    )
    val source = spark.createDataFrame(
      Vector(
        Row(change.targetLineage, change.batch, change.status, change.candidate.orNull, change.completed.orNull)
      ).asJava,
      schema
    )
    DeltaTable
      .forPath(spark, path)
      .as("target")
      .merge(
        source.as("source"),
        "target.lineage = source.lineage AND target.batchId = source.batchId AND target.outcome IN ('Prepared', 'IngestionCommitted')"
      )
      .whenMatched()
      .updateExpr(
        Map(
          "outcome" -> "source.outcome",
          "candidateWatermark" -> "source.candidateWatermark",
          "completedAt" -> "source.completedAt"
        ).asJava
      )
      .execute()
  }

  private def nativeUpdate(spark: SparkSession, path: String, change: Change): Unit =
    DeltaTable
      .forPath(spark, path)
      .update(
        col("lineage") === lit(change.targetLineage) && col("batchId") === lit(change.batch) &&
          col("outcome").isin("Prepared", "IngestionCommitted"),
        Map(
          "outcome" -> lit(change.status),
          "candidateWatermark" -> lit(change.candidate.orNull).cast(TimestampType),
          "completedAt" -> lit(change.completed.orNull).cast(TimestampType)
        ).asJava
      )

  private def seed(spark: SparkSession, execution: SparkExecution[IO], root: Path, change: Change): IO[String] = {
    val paths = TestAnalyticsLakehousePaths.unsafe(root.toUri.toString)
    val journal = new DeltaStreamingBatchJournal[IO](spark, paths, execution)
    val preparation = StreamingInputPreparation(
      StreamingBatchIdentity(lineage, right(StreamingBatchId.from(0L))),
      at.toInstant,
      None,
      right(RangeFingerprint.from("a" * 64)),
      Vector.empty,
      Vector.empty
    )
    journal.prepare(preparation) *> execution {
      val template = spark.read.format("delta").load(paths.streamingProgress)
      val row = template.first()
      val batchIndex = template.schema.fieldIndex("batchId")
      val outcomeIndex = template.schema.fieldIndex("outcome")
      val rows = (0 until 700).map(index =>
        Row.fromSeq(
          row.toSeq
            .updated(batchIndex, index.toLong)
            .updated(outcomeIndex, if (index == 699) change.existing else "Published")
        )
      )
      spark
        .createDataFrame(rows.asJava, template.schema)
        .write
        .format("delta")
        .mode("overwrite")
        .save(paths.streamingProgress)
      paths.streamingProgress
    }
  }

  // Kernel parity only; actual journal/coordinator concurrent terminal and cancellation guards
  // require the separate existing native journal, recovery and cancellation suites.
  test("native status update preserves full journal rows schemas and versions across guarded cases") {
    withSpark { (spark, execution, root) =>
      val cases = Vector(
        Change("Prepared", lineage.value, 699L, "IngestionCommitted", None, None),
        Change("IngestionCommitted", lineage.value, 699L, "Published", None, Some(at)),
        Change("IngestionCommitted", lineage.value, 699L, "Published", Some(at), Some(at)),
        Change("Prepared", "foreign", 699L, "IngestionCommitted", None, None),
        Change("QualityBlocked", lineage.value, 699L, "Published", Some(at), Some(at)),
        Change("Published", lineage.value, 699L, "Published", Some(at), Some(at)),
        Change("ErasurePending", lineage.value, 699L, "Published", Some(at), Some(at)),
        Change("Prepared", lineage.value, 699L, "Prepared", Some(at), Some(at)),
        Change("IngestionCommitted", lineage.value, 699L, "IngestionCommitted", None, None),
        Change("Prepared", lineage.value, 999L, "IngestionCommitted", None, None)
      )
      cases.zipWithIndex.traverse_ { case (change, index) =>
        for {
          original <- seed(spark, execution, root.resolve(s"original-$index"), change)
          proposed <- seed(spark, execution, root.resolve(s"update-$index"), change)
          _ <- execution {
            oldMerge(spark, original, change); nativeUpdate(spark, proposed, change)
            if (change.status == "Prepared" || change.status == "IngestionCommitted") {
              oldMerge(spark, original, change); nativeUpdate(spark, proposed, change)
              val clearing = change.copy(candidate = None, completed = None)
              oldMerge(spark, original, clearing); nativeUpdate(spark, proposed, clearing)
            }
          }
          _ <- execution {
            val a = spark.read.format("delta").load(original)
            val b = spark.read.format("delta").load(proposed)
            assertEquals(b.schema, a.schema)
            assertEquals(b.orderBy("batchId").toJSON.collect().toVector, a.orderBy("batchId").toJSON.collect().toVector)
            assertEquals(
              DeltaLogFactory.system(spark, proposed).update().version,
              DeltaLogFactory.system(spark, original).update().version
            )
          }
        } yield ()
      }
    }
  }

  test("native status update compares three alternated retained journal trials") {
    withSpark { (spark, execution, root) =>
      val change = Change("IngestionCommitted", lineage.value, 699L, "Published", Some(at), Some(at))
      def trial(update: Boolean, index: Int): IO[(Vector[Long], Long)] = for {
        path <- seed(spark, execution, root.resolve(s"trial-$index-$update"), change)
        group = "journal-update-fence-" + UUID.randomUUID()
        work = new Work(group)
        result <- Resource
          .make(execution(spark.sparkContext.addSparkListener(work)))(_ =>
            execution(spark.sparkContext.removeSparkListener(work))
          )
          .use { _ =>
            execution {
              val started = System.nanoTime()
              if (update) nativeUpdate(spark, path, change) else oldMerge(spark, path, change)
              completed(spark, work, group)
              (work.snapshot, System.nanoTime() - started)
            }
          }
      } yield result
      (0 until 3).toVector.traverse_ { index =>
        val pair =
          if (index % 2 == 0) (trial(false, index), trial(true, index)) else (trial(true, index), trial(false, index))
        for {
          first <- pair._1
          second <- pair._2
          _ <- IO {
            val baseline = if (index % 2 == 0) first else second
            val proposed = if (index % 2 == 0) second else first
            println(
              s"JOURNAL_STATUS_NATIVE trial=$index baselineWork=${baseline._1.mkString(",")} updateWork=${proposed._1.mkString(",")} baselineNanos=${baseline._2} updateNanos=${proposed._2} history=700 includesInitialization=false markerJobsPerVariant=1"
            )
          }
        } yield ()
      }
    }
  }
}
