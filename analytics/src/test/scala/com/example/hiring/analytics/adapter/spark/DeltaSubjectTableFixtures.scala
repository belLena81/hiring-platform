package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.{AnalyticsAggregateType, AnalyticsEventType, SubjectToken}

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.*

import java.sql.Timestamp
import java.time.{Duration, Instant}
import scala.jdk.CollectionConverters.*

/** Synthetic rows for any catalog table, built from its declared columns. Every subject token carries the recognizable
  * prefix [[DeltaSubjectTableFixtures.TokenPrefix]] so a test can search a Delta log for leaked values.
  */
private[analytics] object DeltaSubjectTableFixtures {

  /** Every fixture token is a well-formed HMAC subject token, so it can also serve as a deletion marker. */
  val TokenPrefix = "test_"

  def token(sequence: Long): String =
    TokenPrefix + java.util.Base64.getUrlEncoder
      .withoutPadding()
      .encodeToString(java.security.MessageDigest.getInstance("SHA-256").digest(s"subject-$sequence".getBytes("UTF-8")))

  def marker(sequence: Long): SubjectToken =
    SubjectToken.fromHmac(token(sequence)).fold(error => sys.error(error), identity)
  val Epoch: Instant = Instant.parse("2026-01-01T00:00:00Z")

  /** `count` rows whose Kafka offset starts at `firstOffset` (partition 0) and whose other keys are unique per row. */
  def rows(spec: DeltaTableSpec, firstOffset: Long, count: Int): Vector[Row] =
    (0 until count).toVector.map { index =>
      val sequence = firstOffset + index
      Row.fromSeq(spec.columns.map(column => value(column, sequence)))
    }

  def frame(spark: SparkSession, spec: DeltaTableSpec, firstOffset: Long, count: Int): DataFrame =
    spark.createDataFrame(rows(spec, firstOffset, count).asJava, AnalyticsTableSchemas.struct(spec.shape))

  private def at(sequence: Long): Timestamp = Timestamp.from(Epoch.plusSeconds(sequence))

  private def value(column: DeltaColumn, sequence: Long): Any = column.name match {
    case Columns.Topic         => "hiring.events"
    case Columns.Partition     => 0
    case Columns.Offset        => sequence
    case Columns.SubjectToken  => token(sequence)
    case Columns.SubjectTokens => Seq(token(sequence), token(sequence + 1000000L))
    case Columns.EventType     => AnalyticsEventType.values.head.wire
    case Columns.AggregateType => AnalyticsAggregateType.values.head.wire
    case Columns.IngestedAt    => at(sequence)
    case Columns.ExpiresAt     => Timestamp.from(Epoch.plusSeconds(sequence).plus(Duration.ofDays(30)))
    case Columns.NewStatus | Columns.JobSkills | Columns.TerminalAt => null
    case _                                                          =>
      column.dataType match {
        case StringType    => s"${column.name}-$sequence"
        case IntegerType   => 0
        case LongType      => sequence
        case TimestampType => at(sequence)
        case _: ArrayType  => null
        case other         => throw new IllegalArgumentException(s"no fixture value for ${column.name}: $other")
      }
  }
}
