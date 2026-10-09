package com.example.hiring.analytics.adapter.spark

import cats.effect.{IO, Resource}
import com.example.hiring.analytics.{AnalyticsTestSubjectPseudonymizer, TestAnalyticsLakehousePaths}
import munit.CatsEffectSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, encode, lit, sha2}
import org.apache.spark.sql.types.*
import java.nio.file.{Files, Path}
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class StreamingLateFactAdmissionSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val at = Instant.parse("2026-10-03T12:00:00Z")

  private def withSpark(check: (SparkSession, SparkExecution[IO], Path) => IO[Unit]): IO[Unit] =
    SparkBlockingExecution.resource[IO].use { execution =>
      val resource = for {
        root <- Resource.make(IO.blocking(Files.createTempDirectory("hiring-late-admission-")))(path =>
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
            .appName("StreamingLateFactAdmissionSpec")
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

  private def classified(spark: SparkSession, states: Vector[String]): DataFrame = {
    val raw = spark
      .createDataFrame(
        states.indices
          .map { index =>
            val event = com.example.hiring.analytics.AnalyticsOperationalEventFixtures.id(s"late-event-$index")
            val body =
              com.example.hiring.analytics.AnalyticsOperationalEventFixtures.complete(
                s"""{"eventId":"$event","eventType":"JOB_CREATED","occurredAt":"$at","aggregateType":"Job","aggregateId":"job-$index","actorId":"${UUID
                    .nameUUIDFromBytes(
                      event.getBytes("UTF-8")
                    )}","payload":{"jobId":"job-$index","job":{"skills":["Scala"]}}}"""
              )
            Row("hiring.late.admission", index % 3, index.toLong, body)
          }
          .toVector
          .asJava,
        StructType(
          Vector(
            StructField("topic", StringType, false),
            StructField("partition", IntegerType, false),
            StructField("offset", LongType, false),
            StructField("value", StringType, false)
          )
        )
      )
      .withColumn("value", encode(col("value"), "UTF-8"))
      .withColumn("timestamp", lit(Timestamp.from(at)))
    val parsed = OperationalEventTransforms.validEvents(OperationalEventTransforms.parseKafkaRecords(raw))
    val tokenized = AnalyticsSubjectPrivacy
      .withSubjectToken(parsed, AnalyticsTestSubjectPseudonymizer.fromSecret(Array.fill[Byte](32)(19)))
      .withColumn(Columns.EventFingerprint, sha2(col(Columns.RawValue), 256))
    val stateSchema = StructType(tokenized.schema.fields.toVector :+ StructField("admissionState", StringType, false))
    spark.createDataFrame(
      tokenized.rdd.map(row => Row.fromSeq(row.toSeq :+ states(row.getAs[Long]("offset").toInt))),
      stateSchema
    )
  }

  private def ids(spark: SparkSession, values: Vector[String]): DataFrame = spark.createDataFrame(
    values.map(Row(_)).asJava,
    StructType(Vector(StructField(Columns.EventId, StringType, false)))
  )

  /** Original source selection, retained verbatim as the component baseline. */
  private def original(closed: DataFrame, incoming: DataFrame, conflicts: DataFrame, existing: DataFrame): DataFrame =
    closed
      .join(
        incoming
          .select(Columns.EventId, Columns.EventFingerprint)
          .join(conflicts, Seq(Columns.EventId), "left_anti"),
        Seq(Columns.EventId, Columns.EventFingerprint),
        "inner"
      )
      .join(conflicts, Seq(Columns.EventId), "left_anti")
      .join(existing.select(Columns.EventId).distinct(), Seq(Columns.EventId), "left_anti")
      .drop(Columns.EventFingerprint)
      .dropDuplicates(Columns.Topic, Columns.Partition, Columns.Offset)

  private def grouped(spark: SparkSession, underlying: SparkExecution[IO], group: String): SparkExecution[IO] =
    new SparkExecution[IO] {
      private def run[A](work: => A): A = {
        spark.sparkContext.setJobGroup(group, "Late fact native component")
        try work
        finally spark.sparkContext.clearJobGroup()
      }
      override def apply[A](work: => A): IO[A] = underlying(run(work))
      override def either[A](work: => Either[com.example.hiring.analytics.errors.AnalyticsError, A]) =
        underlying.either(run(work))
    }

  test("known empty closed facts avoid source scans and jobs while native Delta version and retained rows match") {
    withSpark { (spark, execution, root) =>
      for {
        input <- execution(classified(spark, Vector.fill(12)("OPEN")))
        markers <- execution(AnalyticsSubjectPrivacy.emptyMarkers(input))
        conflicts <- execution(ids(spark, Vector.empty))
        existing <- execution(ids(spark, Vector("older-event")))
        visits <- execution(spark.sparkContext.longAccumulator("late-fact-source-row-visits"))
        _ <- SparkStreamingBatchStages.cacheAdmissionFrames(Vector(input), execution).use { _ =>
          for {
            quality <- execution {
              val closed = input.filter(col("admissionState") === lit("CLOSED"))
              val empty = ids(spark, Vector.empty)
              AdmissionQualityBaseline.measure(empty, empty, empty, closed)
            }
            _ <- IO(assertEquals(quality.closed, 0L))
            counted <- execution(
              spark.createDataFrame(input.rdd.mapPartitions(_.map { row => visits.add(1L); row }), input.schema)
            )
            closed <- execution(counted.filter(col("admissionState") === lit("CLOSED")))
            baselinePaths = TestAnalyticsLakehousePaths.unsafe(root.resolve("baseline").toUri.toString)
            optimizedPaths = TestAnalyticsLakehousePaths.unsafe(root.resolve("optimized").toUri.toString)
            baselineWriter = new DeltaBatchWriter[IO](baselinePaths, execution)
            optimizedWriter = new DeltaBatchWriter[IO](optimizedPaths, execution)
            _ <- new AnalyticsLateFactStage[IO](baselinePaths, execution, baselineWriter)
              .persistClosedDayFacts(input.limit(2), markers, at)
            _ <- new AnalyticsLateFactStage[IO](optimizedPaths, execution, optimizedWriter)
              .persistClosedDayFacts(input.limit(2), markers, at)
            prior <- execution(
              (
                DeltaLogFactory.system(spark, baselinePaths.lateFacts).update().version,
                DeltaLogFactory.system(spark, optimizedPaths.lateFacts).update().version,
                visits.value
              )
            )
            baselineGroup = "late-native-baseline-" + UUID.randomUUID()
            baselineExecution = grouped(spark, execution, baselineGroup)
            baselineSource <- execution(original(closed, input, conflicts, existing))
            _ <- new AnalyticsLateFactStage[IO](
              baselinePaths,
              baselineExecution,
              new DeltaBatchWriter[IO](baselinePaths, baselineExecution)
            ).persistClosedDayFacts(baselineSource, markers, at)
            baseline <- execution(
              (
                spark.sparkContext.statusTracker.getJobIdsForGroup(baselineGroup).length,
                visits.value - prior._3,
                DeltaLogFactory.system(spark, baselinePaths.lateFacts).update().version - prior._1
              )
            )
            optimizedGroup = "late-native-empty-" + UUID.randomUUID()
            optimizedExecution = grouped(spark, execution, optimizedGroup)
            optimizedSource <- execution(
              SparkStreamingBatchStages
                .closedDayFactsSource(closed, quality.closed) {
                  original(closed, input, conflicts, existing)
                }
                .drop(Columns.EventFingerprint)
                .dropDuplicates(Columns.Topic, Columns.Partition, Columns.Offset)
            )
            _ <- new AnalyticsLateFactStage[IO](
              optimizedPaths,
              optimizedExecution,
              new DeltaBatchWriter[IO](optimizedPaths, optimizedExecution)
            ).persistClosedDayFacts(optimizedSource, markers, at)
            optimized <- execution(
              (
                spark.sparkContext.statusTracker.getJobIdsForGroup(optimizedGroup).length,
                visits.value - prior._3 - baseline._2,
                DeltaLogFactory.system(spark, optimizedPaths.lateFacts).update().version - prior._2
              )
            )
            _ <- execution {
              val before = spark.read.format("delta").load(baselinePaths.lateFacts)
              val after = spark.read.format("delta").load(optimizedPaths.lateFacts)
              assertEquals(before.count(), 2L)
              assertEquals(after.count(), 2L)
              assertEquals(before.exceptAll(after).count(), 0L)
              assertEquals(after.exceptAll(before).count(), 0L)
              assertEquals(optimized._3, baseline._3)
              assert(baseline._2 > 0L, "baseline must actually inspect the source rows")
              assertEquals(optimized._2, 0L)
              assert(optimized._1 < baseline._1, s"native job work must decrease: $baseline -> $optimized")
              println(
                s"STREAMING_LATE_ADMISSION_COMPONENT baselineJobs=${baseline._1} optimizedJobs=${optimized._1} baselineSourceRows=${baseline._2} optimizedSourceRows=${optimized._2} baselineVersionDelta=${baseline._3} optimizedVersionDelta=${optimized._3} retainedRows=2"
              )
            }
          } yield ()
        }
      } yield ()
    }
  }

  test("nonempty closed selection keeps conflict exclusion and already admitted identity suppression") {
    withSpark { (spark, execution, _) =>
      execution {
        val input = classified(spark, Vector("OPEN", "CLOSED", "CLOSED", "CLOSED"))
        val closed = input.filter(col("admissionState") === lit("CLOSED"))
        val conflicts = ids(spark, Vector("d5a55669-448e-3e4c-97b6-7cf6a22fa6c0"))
        val existing = ids(spark, Vector("2d96683c-e07b-370c-bb91-6b7f5c66ebab"))
        val expected = original(closed, input, conflicts, existing)
        val baselineRows = expected.collect().toVector
        assertEquals(baselineRows.map(_.getAs[String](Columns.EventId)), Vector("c104d623-eef3-3b53-8936-c2e2f94fac23"))
        val selected = SparkStreamingBatchStages.closedDayFactsSource(closed, 2L)(expected)
        assert(selected eq expected, "nonempty selection must preserve the original admission plan")
        assertEquals(selected.schema, expected.schema)
        assertEquals(selected.collect().toVector, baselineRows)
      }
    }
  }
}
