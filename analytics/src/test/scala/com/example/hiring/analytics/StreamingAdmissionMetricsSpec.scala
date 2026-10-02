package com.example.hiring.analytics.adapter.spark

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import munit.CatsEffectSuite
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class StreamingAdmissionMetricsSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private def withSpark(check: (SparkSession, SparkExecution[IO]) => IO[Unit]): IO[Unit] =
    SparkBlockingExecution.resource[IO].use { execution =>
      Resource
        .make(execution {
          SparkSession
            .builder()
            .master("local[2]")
            .appName("StreamingAdmissionMetricsSpec")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        })(spark => execution.blocking(spark.stop()))
        .use { spark =>
          execution.attachSparkContext(spark.sparkContext) *> check(spark, execution)
        }
    }

  private def events(spark: SparkSession, identifiers: Vector[Option[String]]): DataFrame =
    spark.createDataFrame(
      identifiers.map(value => Row(value.orNull)).asJava,
      StructType(Vector(StructField(Columns.EventId, StringType, nullable = true)))
    )

  private def assertEquivalent(malformed: DataFrame, conflicts: DataFrame, future: DataFrame): Unit = {
    val expected = SparkStreamingBatchStages.AdmissionQuality(
      malformed.count(),
      conflicts.select(Columns.EventId).distinct().count(),
      future.count()
    )
    assertEquals(SparkStreamingBatchStages.measureQuality(malformed, conflicts, future), expected)
  }

  test("empty admission categories return three zero counts") {
    withSpark { (spark, execution) =>
      execution {
        val empty = events(spark, Vector.empty)
        assertEquivalent(empty, empty, empty)
        assertEquals(
          SparkStreamingBatchStages.measureQuality(empty, empty, empty),
          SparkStreamingBatchStages.AdmissionQuality(0L, 0L, 0L)
        )
      }
    }
  }

  test("mixed quality counts retain nullable malformed rows, distinct conflicts and repeated future rows") {
    withSpark { (spark, execution) =>
      execution {
        val malformed = events(spark, Vector(None, Some("malformed")))
        val conflicts = events(spark, Vector(Some("conflict"), Some("conflict"), Some("other-conflict")))
        val future = events(spark, Vector(Some("future"), Some("future"), Some("other-future")))
        assertEquivalent(malformed, conflicts, future)
        assertEquals(
          SparkStreamingBatchStages.measureQuality(malformed, conflicts, future),
          SparkStreamingBatchStages.AdmissionQuality(2L, 2L, 3L)
        )
      }
    }
  }

  test("future anti-join excludes conflicting identities without deduplicating remaining source rows") {
    withSpark { (spark, execution) =>
      execution {
        val empty = events(spark, Vector.empty)
        val conflicts = events(spark, Vector(Some("blocked"), Some("blocked")))
        val future = events(spark, Vector(Some("blocked"), Some("retained"), Some("retained")))
          .join(conflicts, Seq(Columns.EventId), "left_anti")
        assertEquivalent(empty, conflicts, future)
        assertEquals(
          SparkStreamingBatchStages.measureQuality(empty, conflicts, future),
          SparkStreamingBatchStages.AdmissionQuality(0L, 1L, 2L)
        )
      }
    }
  }

  test("known empty markers preserve privacy filtering and require no suppression count action") {
    withSpark { (spark, execution) =>
      execution {
        val tokenized = subjectEvents(spark)
        val safe = privacyFiltered(tokenized, Vector.empty)
        assertEquals(safe.count(), tokenized.count())
        val group = "empty-marker-suppression-count"
        spark.sparkContext.setJobGroup(group, "Known empty marker suppression")
        try {
          assertEquals(SparkStreamingBatchStages.deletionSuppressedCount(tokenized, safe, false), 0L)
          assertEquals(spark.sparkContext.statusTracker.getJobIdsForGroup(group).toVector, Vector.empty[Int])
        } finally spark.sparkContext.clearJobGroup()
      }
    }
  }

  test("active markers retain exact suppression across all associated subject tokens") {
    withSpark { (spark, execution) =>
      execution {
        val tokenized = subjectEvents(spark)
        val safe = privacyFiltered(tokenized, Vector("candidate-token", "search-result-token"))
        assertEquals(SparkStreamingBatchStages.deletionSuppressedCount(tokenized, safe, true), 2L)
        assertEquals(safe.select(Columns.EventId).collect().toVector.map(_.getString(0)), Vector("retained"))
      }
    }
  }

  private def subjectEvents(spark: SparkSession): DataFrame =
    spark.createDataFrame(
      Vector(
        Row("candidate", "actor-token", Seq("actor-token", "candidate-token")),
        Row("search-result", "searcher-token", Seq("searcher-token", "search-result-token")),
        Row("retained", "retained-token", Seq("retained-token"))
      ).asJava,
      StructType(
        Vector(
          StructField(Columns.EventId, StringType, nullable = false),
          StructField(Columns.SubjectToken, StringType, nullable = false),
          StructField(Columns.SubjectTokens, ArrayType(StringType, containsNull = false), nullable = false)
        )
      )
    )

  private def privacyFiltered(events: DataFrame, tokens: Vector[String]): DataFrame = {
    val markers = events.sparkSession.createDataFrame(
      tokens.map(Row(_)).asJava,
      StructType(Vector(StructField(Columns.SubjectToken, StringType, nullable = false)))
    )
    AnalyticsSubjectPrivacy
      .excludeActiveDeletionMarkers(events, markers)
      .fold(
        error => throw error,
        identity
      )
  }
}
