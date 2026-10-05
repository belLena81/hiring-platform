package com.example.hiring.analytics.adapter.spark

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import io.delta.tables.DeltaTable
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

final class StreamingConflictVerificationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 8.minutes
  private def withSpark(check: (SparkSession, SparkExecution[IO], Path) => IO[Unit]): IO[Unit] =
    SparkBlockingExecution.resource[IO].use { execution =>
      val resource = for {
        root <- Resource.make(IO.blocking(Files.createTempDirectory("hiring-conflict-verification-")))(path =>
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
            .appName("StreamingConflictVerificationSpec")
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

  private def conflicts(incoming: DataFrame, stored: DataFrame): DataFrame =
    incoming
      .alias("incoming")
      .join(stored.alias("stored"), Seq("eventId"), "inner")
      .filter(col("incoming.fingerprint") =!= col("stored.fingerprint"))
      .select("eventId")
      .distinct()

  private def guard(fresh: DataFrame, prepared: DataFrame): Boolean =
    fresh.join(prepared, Seq("eventId"), "left_anti").limit(1).count() == 0L

  test("post Bronze conflict cache reduces retained Delta scans with identical native merge outcomes") {
    withSpark { (spark, execution, root) =>
      def run(cached: Boolean, mixed: Boolean): IO[(Vector[Long], Vector[String], Long)] = {
        val destination = root.resolve(s"result-$cached-$mixed").toString
        for {
          frames <- execution {
            val incoming = spark
              .range(1000)
              .select(
                concat(lit(if (mixed) "event-" else "new-event-"), col("id")).as("eventId"),
                lit("incoming").as("fingerprint")
              )
            val stored = spark.read.format("delta").load(root.resolve("stored").toString)
            val prepared = conflicts(incoming, stored)
            // These are independent fresh reads, as in post-Bronze verification.
            val fresh = conflicts(incoming, spark.read.format("delta").load(root.resolve("stored").toString))
            (incoming, fresh, prepared)
          }
          group = "conflict-verification-" + UUID.randomUUID()
          listener = new Work(group)
          result <- Resource
            .make(execution(spark.sparkContext.addSparkListener(listener)))(_ =>
              execution(spark.sparkContext.removeSparkListener(listener))
            )
            .use { _ =>
              val scope =
                if (cached) SparkStreamingBatchStages.cacheAdmissionFrames(Vector(frames._2, frames._3), execution).void
                else Resource.pure[IO, Unit](())
              scope.use { _ =>
                execution {
                  assert(guard(frames._2, frames._3))
                  val admitted = frames._1.join(frames._3, Seq("eventId"), "left_anti")
                  // Repeated consumers mirror quality/classifications/native sinks.
                  val quality = frames._1.join(frames._3, Seq("eventId"), "inner").count()
                  assertEquals(quality, if (mixed) 100L else 0L)
                  admitted.select("eventId").write.format("delta").save(destination)
                  DeltaTable
                    .forPath(spark, destination)
                    .as("target")
                    .merge(admitted.select("eventId").as("source"), "target.eventId = source.eventId")
                    .whenNotMatched()
                    .insertAll()
                    .execute()
                  val retained = spark.read
                    .format("delta")
                    .load(destination)
                    .select("eventId")
                    .collect()
                    .toVector
                    .map(_.getString(0))
                    .sorted
                  completed(spark, listener, group)
                  (listener.snapshot, retained, DeltaLogFactory.system(spark, destination).update().version)
                }
              }
            }
        } yield result
      }
      for {
        _ <- execution {
          spark
            .range(7500)
            .select(
              concat(lit("event-"), col("id")).as("eventId"),
              when(col("id") < 100, lit("different")).otherwise(lit("incoming")).as("fingerprint")
            )
            .write
            .format("delta")
            .option("delta.dataSkippingNumIndexedCols", "0")
            .save(root.resolve("stored").toString)
        }
        original <- run(false, true)
        cached <- run(true, true)
        _ <- IO {
          assertEquals(cached._2, original._2)
          assertEquals(cached._3, original._3)
          assertEquals(cached._2.size, 900)
          println(
            s"CONFLICT_VERIFICATION_NATIVE baseline=${original._1.mkString(",")} cached=${cached._1.mkString(",")} version=${cached._3} retained=${cached._2.size}"
          )
          assert(cached._1(2) < original._1(2), "cached verification must scan fewer native source rows")
          assert(cached._1(3) < original._1(3), "cached verification must read fewer native source bytes")
        }
        zeroOriginal <- run(false, false)
        zeroCached <- run(true, false)
        _ <- IO {
          assertEquals(zeroCached._2, zeroOriginal._2)
          assertEquals(zeroCached._3, zeroOriginal._3)
          println(
            s"CONFLICT_VERIFICATION_NATIVE_ZERO baseline=${zeroOriginal._1.mkString(",")} cached=${zeroCached._1.mkString(",")} version=${zeroCached._3} retained=${zeroCached._2.size}"
          )
          assert(zeroCached._1(2) < zeroOriginal._1(2), "zero-conflict cache must scan fewer retained source rows")
          assert(zeroCached._1(3) < zeroOriginal._1(3), "zero-conflict cache must read fewer retained source bytes")
        }
      } yield ()
    }
  }

  test("conflict verification helper rejects additional identities and caches release on every outcome") {
    withSpark { (spark, execution, _) =>
      for {
        frames <- execution {
          val incoming =
            spark.range(12).select(concat(lit("event-"), col("id")).as("eventId"), lit("incoming").as("fingerprint"))
          val silver = incoming.filter(col("eventId") === "event-1").withColumn("fingerprint", lit("silver"))
          val late = incoming.filter(col("eventId") === "event-2").withColumn("fingerprint", lit("late"))
          val coordinate = incoming.filter(col("eventId") === "event-3").withColumn("fingerprint", lit("coordinate"))
          val prepared = conflicts(incoming, silver.unionByName(late))
          val fresh = conflicts(incoming, silver.unionByName(late).unionByName(coordinate))
          (fresh, prepared)
        }
        result <- SparkStreamingBatchStages.cacheAdmissionFrames(Vector(frames._1, frames._2), execution).use { _ =>
          execution(guard(frames._1, frames._2))
        }
        _ <- IO(assert(!result))
        _ <- execution {
          assertEquals(frames._1.storageLevel, StorageLevel.NONE);
          assertEquals(frames._2.storageLevel, StorageLevel.NONE)
        }
        failure <- SparkStreamingBatchStages
          .cacheAdmissionFrames(Vector(frames._1, frames._2), execution)
          .use(_ => IO.raiseError[Unit](new IllegalStateException("test failure")))
          .attempt
        _ <- IO(assert(failure.isLeft))
        _ <- execution {
          assertEquals(frames._1.storageLevel, StorageLevel.NONE);
          assertEquals(frames._2.storageLevel, StorageLevel.NONE)
        }
        ready <- cats.effect.Deferred[IO, Unit]
        fiber <- SparkStreamingBatchStages
          .cacheAdmissionFrames(Vector(frames._1, frames._2), execution)
          .use(_ => ready.complete(()) *> IO.never[Unit])
          .start
        _ <- ready.get *> fiber.cancel *> fiber.join
        _ <- execution {
          assertEquals(frames._1.storageLevel, StorageLevel.NONE);
          assertEquals(frames._2.storageLevel, StorageLevel.NONE)
        }
        _ <- Resource
          .make(execution(frames._2.persist(StorageLevel.MEMORY_AND_DISK)))(_ =>
            execution(frames._2.unpersist(true)).void
          )
          .use { _ =>
            SparkStreamingBatchStages.cacheAdmissionFrames(Vector(frames._1, frames._2), execution).use(_ => IO.unit) *>
              execution {
                assertEquals(frames._1.storageLevel, StorageLevel.NONE);
                assertEquals(frames._2.storageLevel, StorageLevel.MEMORY_AND_DISK)
              }
          }
      } yield ()
    }
  }
}
