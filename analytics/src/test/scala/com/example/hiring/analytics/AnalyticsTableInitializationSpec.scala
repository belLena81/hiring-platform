package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.AnalyticsTestSpark

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import io.delta.tables.DeltaTable
import munit.CatsEffectSuite
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class AnalyticsTableInitializationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 8.minutes
  private def withSpark(check: (SparkSession, SparkExecution[IO], Path) => IO[Unit]): IO[Unit] =
    SparkBlockingExecution.resource[IO].use { execution =>
      val resource = for {
        root <- Resource.make(IO.blocking(Files.createTempDirectory("hiring-table-initialization-")))(path =>
          IO.blocking {
            val entries = Files.walk(path)
            try entries.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
            finally entries.close()
          }
        )
        spark <- Resource.make(execution {
          AnalyticsTestSpark
            .builder("AnalyticsTableInitializationSpec")
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

  private val spec = AnalyticsDeltaCatalog.Silver

  /** The same creation through the native builder and ALTER statements, without the catalog helper. */
  private def original(spark: SparkSession, path: String, table: DeltaTableSpec = spec): Unit = {
    val location = SparkPhysicalLocation.resolve(path)
    val existed = DeltaTable.isDeltaTable(spark, location)
    val builder = DeltaTable.createIfNotExists(spark).location(location).addColumns(table.schema)
    table.properties.foldLeft(builder) { case (current, (key, value)) => current.property(key, value) }.execute()
    if (!existed)
      table.checks.foreach { check =>
        spark.sql(
          s"ALTER TABLE ${DeltaTables.sqlIdentifier(path)} ADD CONSTRAINT ${check.name} CHECK (${check.expression})"
        )
      }
    assert(AnalyticsTableSchemas.matches(spark.read.format("delta").load(location).schema, table.shape))
  }

  test("existing native Delta initialization compares repeated builder work and preserves schema and version") {
    withSpark { (spark, execution, root) =>
      val path = root.resolve("existing").toString
      def trial(optimized: Boolean): IO[(Long, Vector[Long])] = {
        val group = "table-initialization-" + UUID.randomUUID()
        val work = new Work(group)
        Resource
          .make(execution(spark.sparkContext.addSparkListener(work)))(_ =>
            execution(spark.sparkContext.removeSparkListener(work))
          )
          .use { _ =>
            execution {
              val started = System.nanoTime()
              (0 until 12).foreach { _ =>
                if (optimized) AnalyticsTableSchemas.createOrValidate(spark, path, spec)
                else original(spark, path)
              }
              val elapsed = System.nanoTime() - started
              completed(spark, work, group)
              (elapsed, work.snapshot)
            }
          }
      }
      for {
        _ <- execution(AnalyticsTableSchemas.createOrValidate(spark, path, spec))
        prior <- execution(DeltaLogFactory.system(spark, path).update().version)
        _ <- (0 until 3).toVector.traverse_ { index =>
          val pair = if (index % 2 == 0) (trial(false), trial(true)) else (trial(true), trial(false))
          for {
            first <- pair._1
            second <- pair._2
            _ <- IO {
              val baseline = if (index % 2 == 0) first else second
              val optimized = if (index % 2 == 0) second else first
              println(
                s"TABLE_INITIALIZATION_NATIVE trial=$index repetitions=12 baselineNanos=${baseline._1} optimizedNanos=${optimized._1} baselineWork=${baseline._2.mkString(",")} optimizedWork=${optimized._2.mkString(",")} markerJobsPerVariant=1"
              )
            }
          } yield ()
        }
        _ <- execution {
          assertEquals(DeltaLogFactory.system(spark, path).update().version, prior)
          assert(AnalyticsTableSchemas.matches(spark.read.format("delta").load(path).schema, spec.shape))
        }
      } yield ()
    }
  }

  test("creation raw properties incompatible schema and non Delta locations preserve native behavior") {
    withSpark { (spark, execution, root) =>
      for {
        _ <- execution {
          Vector(AnalyticsDeltaCatalog.Silver, AnalyticsDeltaCatalog.RunManifests).foreach { table =>
            val baseline = root.resolve(s"baseline-${table.name}").toString
            val optimized = root.resolve(s"optimized-${table.name}").toString
            original(spark, baseline, table)
            AnalyticsTableSchemas.createOrValidate(spark, optimized, table)
            val oldSnapshot = DeltaLogFactory.system(spark, baseline).update()
            val newSnapshot = DeltaLogFactory.system(spark, optimized).update()
            assertEquals(newSnapshot.version, oldSnapshot.version)
            assertEquals(newSnapshot.metadata.configuration, oldSnapshot.metadata.configuration)
            assertEquals(newSnapshot.metadata.schema, oldSnapshot.metadata.schema)
            assertEquals(
              newSnapshot.metadata.configuration.get(DeltaStatsPolicy.StatsColumnsProperty),
              Option.when(table.subjectAttributed)(DeltaStatsPolicy.render(table.statsColumns))
            )
          }
          val mismatch = root.resolve("mismatch").toString
          AnalyticsTableSchemas.createOrValidate(spark, mismatch, AnalyticsDeltaCatalog.Bronze)
          val prior = DeltaLogFactory.system(spark, mismatch).update().version
          intercept[com.example.hiring.analytics.errors.AnalyticsError.DeltaSchemaMismatch] {
            AnalyticsTableSchemas.createOrValidate(spark, mismatch, spec)
          }
          assertEquals(DeltaLogFactory.system(spark, mismatch).update().version, prior)
          val occupied = root.resolve("occupied").toString
          spark.range(1).write.parquet(occupied)
          val oldFailure = scala.util.Try(original(spark, occupied)).failed.toOption
          val newFailure =
            scala.util.Try(AnalyticsTableSchemas.createOrValidate(spark, occupied, spec)).failed.toOption
          assert(oldFailure.nonEmpty && newFailure.nonEmpty)
          assertEquals(newFailure.map(_.getClass), oldFailure.map(_.getClass))
        }
      } yield ()
    }
  }

  test("parallel initialization retains native create if absent arbitration") {
    withSpark { (spark, execution, root) =>
      val path = root.resolve("parallel").toString
      val create = IO.blocking(AnalyticsTableSchemas.createOrValidate(spark, path, spec))
      for {
        results <- (create.attempt, create.attempt).parTupled
        _ <- IO(assert(results._1.isRight || results._2.isRight))
        _ <- execution {
          AnalyticsTableSchemas.createOrValidate(spark, path, spec)
          assert(AnalyticsTableSchemas.matches(spark.read.format("delta").load(path).schema, spec.shape))
          assertEquals(
            DeltaLogFactory
              .system(spark, path)
              .update()
              .metadata
              .configuration
              .get(DeltaStatsPolicy.StatsColumnsProperty),
            Some(DeltaStatsPolicy.render(spec.statsColumns))
          )
          assertEquals(DeltaLogFactory.system(spark, path).update().version, spec.checks.size.toLong)
        }
      } yield ()
    }
  }
}
