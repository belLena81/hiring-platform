package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.SubjectToken
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.*

import scala.jdk.CollectionConverters.*

/** Column order and types shared by projections, Delta initialization, and persisted-schema checks. */
private[analytics] object AnalyticsTableSchemas {
  type Shape = Vector[(String, DataType)]

  val silver: Shape = Vector(
    Columns.EventId -> StringType,
    Columns.EventType -> StringType,
    Columns.OccurredAt -> TimestampType,
    Columns.AggregateType -> StringType,
    Columns.AggregateId -> StringType,
    Columns.ApplicationId -> StringType,
    Columns.JobId -> StringType,
    Columns.NewStatus -> StringType,
    Columns.JobSkills -> ArrayType(StringType),
    Columns.SubjectToken -> StringType,
    Columns.SubjectTokens -> ArrayType(StringType, containsNull = false),
    Columns.EventFingerprint -> StringType
  )
  val expiry: Shape = Vector(Columns.IngestedAt -> TimestampType, Columns.ExpiresAt -> TimestampType)

  /** Closed-day facts retain only normalized, pseudonymized fields needed for bounded replay. */
  val lateFacts: Shape = Vector(
    Columns.EventId -> StringType,
    Columns.EventFingerprint -> StringType,
    Columns.EventType -> StringType,
    Columns.Topic -> StringType,
    Columns.Partition -> IntegerType,
    Columns.Offset -> LongType,
    Columns.OccurredAt -> TimestampType,
    Columns.AggregateType -> StringType,
    Columns.AggregateId -> StringType,
    Columns.ApplicationId -> StringType,
    Columns.JobId -> StringType,
    Columns.NewStatus -> StringType,
    Columns.JobSkills -> ArrayType(StringType),
    Columns.SubjectToken -> StringType,
    Columns.SubjectTokens -> ArrayType(StringType, containsNull = false),
    Columns.AdmissionReason -> StringType,
    Columns.IngestedAt -> TimestampType,
    Columns.ExpiresAt -> TimestampType
  )
  val funnel: Shape = Vector(
    Columns.Day -> TimestampType,
    Columns.Created -> LongType,
    Columns.Accepted -> LongType,
    Columns.Declined -> LongType,
    Columns.Interview -> LongType,
    Columns.Hired -> LongType,
    Columns.Rejected -> LongType
  )
  val timeToHire: Shape = Vector(
    Columns.P50Hours -> DoubleType,
    Columns.P75Hours -> DoubleType,
    Columns.P90Hours -> DoubleType,
    Columns.P95Hours -> DoubleType,
    Columns.EligibleCount -> LongType,
    Columns.ExcludedCount -> LongType
  )
  val skills: Shape = Vector(Columns.Day -> TimestampType, Columns.Skill -> StringType, Columns.Postings -> LongType)
  val manifests: Shape = Vector(
    "runId" -> StringType,
    Columns.Topic -> StringType,
    Columns.Partition -> IntegerType,
    "startOffset" -> LongType,
    "endOffsetExclusive" -> LongType,
    "status" -> StringType,
    "updatedAt" -> StringType
  )
  val quarantine: Shape = Vector(
    Columns.Topic -> StringType,
    Columns.Partition -> IntegerType,
    Columns.Offset -> LongType,
    "payloadHash" -> StringType,
    Columns.SubjectTokens -> ArrayType(StringType, containsNull = false),
    "quarantineId" -> StringType,
    "quarantineReason" -> StringType,
    Columns.ExpiresAt -> TimestampType
  )
  val bronze: Shape = Vector(
    Columns.Topic -> StringType,
    Columns.Partition -> IntegerType,
    Columns.Offset -> LongType,
    Columns.KafkaTimestamp -> TimestampType,
    Columns.RawValue -> StringType,
    Columns.EventId -> StringType,
    Columns.EventType -> StringType,
    Columns.OccurredAt -> TimestampType,
    Columns.AggregateType -> StringType,
    Columns.AggregateId -> StringType,
    Columns.ActorId -> StringType,
    Columns.Payload -> OperationalEventTransforms.payloadSchema,
    Columns.SubjectToken -> StringType,
    Columns.SubjectTokens -> ArrayType(StringType, containsNull = false)
  ) ++ expiry

  /** The shape a merge target must have, and whether it holds raw records (no data-skipping statistics). */
  def targetOf(paths: AnalyticsLakehousePaths, path: String): (Shape, Boolean) =
    Map(paths.bronze -> bronze, paths.quarantine -> quarantine, paths.lateFacts -> lateFacts)
      .get(path)
      .fold((silver ++ expiry, false))(_ -> true)

  def struct(shape: Shape): StructType = StructType(shape.map { case (name, dataType) =>
    StructField(name, dataType, nullable = true)
  })

  /** Single non-null `subjectToken` column holding the given deletion-marker tokens. */
  def markerFrame(spark: SparkSession, tokens: Vector[SubjectToken]): DataFrame =
    spark.createDataFrame(
      tokens.map(token => Row(token.value)).asJava,
      StructType(Seq(StructField(Columns.SubjectToken, StringType, nullable = false)))
    )

  def matches(actual: StructType, shape: Shape): Boolean =
    actual.fields.toVector.map(field => field.name -> field.dataType.simpleString) ==
      shape.map { case (name, dataType) => name -> dataType.simpleString }

  def createOrValidate(spark: SparkSession, path: String, shape: Shape, raw: Boolean = false): Unit = {
    val location = SparkPhysicalLocation.resolve(path)
    if (!DeltaTables.exists(spark, path)) {
      val builder = DeltaTable.createIfNotExists(spark).location(location).addColumns(struct(shape))
      val _ = (if (raw) builder.property("delta.dataSkippingNumIndexedCols", "0") else builder).execute()
    }
    val actual = DeltaTables.read(spark, path).schema
    if (!matches(actual, shape))
      throw AnalyticsError.DeltaSchemaMismatch(path)
  }
}
