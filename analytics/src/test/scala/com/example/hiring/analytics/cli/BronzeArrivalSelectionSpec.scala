package com.example.hiring.analytics.cli

import com.example.hiring.analytics.adapter.spark.*

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import io.delta.tables.DeltaTable
import munit.CatsEffectSuite
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerTaskEnd}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.*
import org.apache.spark.sql.functions.*
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class BronzeArrivalSelectionSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 8.minutes
  private def withSpark(check: (SparkSession, SparkExecution[IO], Path) => IO[Unit]): IO[Unit] =
    SparkBlockingExecution.resource[IO].use { execution =>
      val resource = for {
        root <- Resource.make(IO.blocking(Files.createTempDirectory("hiring-bronze-arrival-")))(path =>
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
            .appName("BronzeArrivalSelectionSpec")
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
    spark.sparkContext.setJobGroup(group, "Bronze arrival listener fence")
    try {
      // One identical tiny job in each variant establishes a public listener FIFO fence.
      assertEquals(spark.sparkContext.parallelize(Seq(1), 1).count(), 1L)
      assert(work.fenceEnded.await(10L, TimeUnit.SECONDS), "native listener fence was not observed")
    } finally spark.sparkContext.clearJobGroup()
  }

  import HiringAnalyticsStreamingWorkloadMain.{Sample, Observations, pendingBronzeMinima, observeBronze}
  private val topic = "hiring.bronze.arrival"
  private val samples = (0 until 7500).map { index =>
    Sample(s"event-$index", index % 3, (index / 3).toLong * 2L, 10L, Some(20L))
  }.toVector

  private def seed(spark: SparkSession, path: String): Unit = {
    val rows = samples.map { sample =>
      Row.fromSeq(AnalyticsTableSchemas.bronze.map { case (name, _) =>
        name match {
          case "topic"     => topic
          case "partition" => sample.partition
          case "offset"    => sample.offset
          case "rawValue"  => "{}"
          case _           => null
        }
      })
    }
    spark
      .createDataFrame(rows.asJava, AnalyticsTableSchemas.struct(AnalyticsTableSchemas.bronze))
      .write
      .format("delta")
      .option("delta.dataSkippingNumIndexedCols", "0")
      .save(path)
  }

  private def read(spark: SparkSession, path: String, minima: Vector[(Int, Long)]): Vector[(Int, Long)] =
    if (!DeltaTable.isDeltaTable(spark, path)) Vector.empty
    else {
      val predicate = minima.foldLeft(lit(false)) { case (condition, (partition, offset)) =>
        condition || (col("partition") === partition && col("offset") >= offset)
      }
      spark.read
        .format("delta")
        .load(path)
        .filter(col("topic") === topic && predicate)
        .select("partition", "offset")
        .limit(15000)
        .collect()
        .toVector
        .map(row => row.getInt(0) -> row.getLong(1))
    }

  test("pending minima retain exact gaps partitions and ignore report completion") {
    assertEquals(pendingBronzeMinima(samples), Vector.empty)
    val selected =
      Vector(samples(9).copy(bronzeAt = None), samples(1).copy(bronzeAt = None, reportAt = Some(30L)), samples(4))
    assertEquals(pendingBronzeMinima(selected), Vector(0 -> 6L, 1 -> 0L))
    assertEquals(pendingBronzeMinima(selected ++ selected), pendingBronzeMinima(selected))
  }

  test("native pending selection preserves all arrivals with bounded decoded rows and no additional scan work") {
    withSpark { (spark, execution, root) =>
      val path = root.resolve("bronze").toString
      def measured(minima: Vector[(Int, Long)]): IO[(Vector[Long], Vector[(Int, Long)], Long)] = {
        val group = "bronze-arrival-fence-" + UUID.randomUUID()
        val work = new Work(group)
        Resource
          .make(execution(spark.sparkContext.addSparkListener(work)))(_ =>
            execution(spark.sparkContext.removeSparkListener(work))
          )
          .use { _ =>
            execution {
              val started = System.nanoTime()
              val coordinates = read(spark, path, minima)
              val elapsed = System.nanoTime() - started
              completed(spark, work, group)
              (work.snapshot, coordinates, elapsed)
            }
          }
      }
      def arrivals(initial: Vector[Sample], coordinates: Vector[(Int, Long)]): IO[(Vector[Sample], Observations)] =
        for {
          state <- cats.effect.Ref.of[IO, Vector[Sample]](initial)
          observations <- cats.effect.Ref.of[IO, Observations](Observations())
          _ <- observeBronze(state, observations, coordinates.toSet, 100L)
          result <- state.get
          metrics <- observations.get
        } yield (result, metrics)
      for {
        _ <- execution(seed(spark, path))
        _ <- Vector(0, 100, 500, 1000, 7500).traverse_ { pending =>
          Vector(false, true).traverse_ { scattered =>
            val indices =
              if (scattered) (0 until pending).map(index => index * 7500 / pending.max(1)).toSet
              else (7500 - pending until 7500).toSet
            val current = samples.zipWithIndex.map { case (sample, index) =>
              if (indices(index)) sample.copy(bronzeAt = None) else sample
            }
            val original = current.groupBy(_.partition).toVector.map { case (partition, values) =>
              partition -> values.map(_.offset).min
            }
            Vector(0, 1).traverse_ { trial =>
              val baselineRead = measured(original)
              val selectedRead = measured(pendingBronzeMinima(current))
              val pair = if (trial == 0) (baselineRead, selectedRead) else (selectedRead, baselineRead)
              for {
                first <- pair._1
                second <- pair._2
                baseline = if (trial == 0) first else second
                selected = if (trial == 0) second else first
                prior <- arrivals(current, baseline._2)
                after <- arrivals(current, selected._2)
                _ <- IO {
                  assertEquals(after, prior)
                  assert(after._1.forall(_.bronzeAt.nonEmpty))
                  assert(selected._2.size <= baseline._2.size)
                  if (pending < 7500 && !scattered) assert(selected._2.size < baseline._2.size)
                  assert(selected._1(0) <= baseline._1(0), "pending selection must not add native jobs")
                  assert(selected._1(2) <= baseline._1(2), "pending selection must not increase native input rows")
                  assert(selected._1(3) <= baseline._1(3), "pending selection must not increase native input bytes")
                  println(
                    s"BRONZE_ARRIVAL_NATIVE pending=$pending scattered=$scattered trial=$trial baselineWork=${baseline._1.mkString(",")} selectedWork=${selected._1.mkString(",")} baselineDecoded=${baseline._2.size} selectedDecoded=${selected._2.size} baselineNanos=${baseline._3} selectedNanos=${selected._3} markerJobsPerVariant=1"
                  )
                }
              } yield ()
            }
          }
        }
      } yield ()
    }
  }

  test("producer appends cannot acquire early arrivals and absent coordinates remain pending") {
    val first = Sample("first", 0, 10L, 10L)
    val second = Sample("new-partition", 1, 20L, 150L)
    for {
      state <- cats.effect.Ref.of[IO, Vector[Sample]](Vector(first))
      observations <- cats.effect.Ref.of[IO, Observations](Observations())
      captured <- state.get
      _ <- IO(assertEquals(pendingBronzeMinima(captured), Vector(0 -> 10L)))
      _ <- state.update(_ :+ second)
      _ <- observeBronze(state, observations, Set(0 -> 10L, 1 -> 20L), 100L)
      partial <- state.get
      _ <- IO { assertEquals(partial.head.bronzeAt, Some(100L)); assertEquals(partial(1).bronzeAt, None) }
      _ <- observeBronze(state, observations, Set.empty, 200L)
      missing <- state.get
      _ <- IO(assertEquals(missing, partial))
      _ <- observeBronze(state, observations, Set(1 -> 20L), 250L)
      complete <- state.get
      _ <- IO { assertEquals(complete.head.bronzeAt, Some(100L)); assertEquals(complete(1).bronzeAt, Some(250L)) }
    } yield ()
  }

  test("empty pending selection still resolves fresh native schema and missing table state") {
    withSpark { (spark, execution, root) =>
      execution {
        val missing = root.resolve("missing").toString
        assertEquals(read(spark, missing, Vector.empty), Vector.empty)
        seed(spark, missing)
        assertEquals(read(spark, missing, Vector.empty), Vector.empty)
        val appended = spark.read
          .format("delta")
          .load(missing)
          .limit(1)
          .withColumn("partition", lit(3))
          .withColumn("offset", lit(123L))
        appended.write.format("delta").mode("append").save(missing)
        assert(!read(spark, missing, Vector(0 -> 0L)).contains(3 -> 123L))
        assert(read(spark, missing, Vector(0 -> 0L, 3 -> 123L)).contains(3 -> 123L))
        DeltaTable.forPath(spark, missing).delete("partition = 0")
        assertEquals(read(spark, missing, Vector(0 -> 0L)), Vector.empty)
        val broken = root.resolve("broken").toString
        spark.range(1).write.format("delta").save(broken)
        intercept[org.apache.spark.sql.AnalysisException] { read(spark, broken, Vector.empty) }
      }
    }
  }
}
