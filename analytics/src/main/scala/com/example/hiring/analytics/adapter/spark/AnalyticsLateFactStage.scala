package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AnalyticsEventTimePolicy
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

import cats.effect.Async
import cats.syntax.all.*
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.*

import java.sql.Timestamp
import java.time.Instant

/** Persists privacy-safe records whose UTC day was already closed at the previous durable watermark.
  *
  * The caller supplies already validated and pseudonymized operational events and the stable observation time from the
  * batch preparation. The stage rechecks active deletion markers, stores no raw envelope or direct subject identifier,
  * and keeps the original 30-day expiry across retries by deriving it from that stable observation time.
  */
private[analytics] final class AnalyticsLateFactStage[F[_]: Async](
    paths: AnalyticsLakehousePaths,
    execution: SparkExecution[F],
    deltaWriter: DeltaWriter[F]
) {
  private val retentionDays = AnalyticsEventTimePolicy.LateFactRetentionDays.toLong
  private val closedDayReason = "CLOSED_DAY"

  /** `closedDayEvents` must contain only valid event rows already pseudonymized by
    * [[AnalyticsSubjectPrivacy.withSubjectToken]]. The caller owns event-time admission and must select records whose
    * UTC day end is less than or equal to the previous durable watermark. `observedAt` must come from the durable batch
    * preparation, so callback retries produce the same expiry. This method never changes report datasets or watermark.
    */
  def persistClosedDayFacts(
      closedDayEvents: DataFrame,
      activeMarkerTokens: DataFrame,
      observedAt: Instant
  ): F[Unit] =
    for {
      normalized <- execution.either(normalize(closedDayEvents, activeMarkerTokens, observedAt))
      _ <- deltaWriter.merge(
        normalized,
        paths.lateFacts,
        "target.topic = source.topic AND target.partition = source.partition AND target.offset = source.offset"
      )
    } yield ()

  private def normalize(
      events: DataFrame,
      markerTokens: DataFrame,
      observedAt: Instant
  ): Either[AnalyticsError, DataFrame] = {
    val required = lateInputShape.map(_._1).toSet
    val missing = (required -- events.columns.toSet).toVector.sorted
    if (missing.nonEmpty) Left(AnalyticsError.InvalidLateFactSchema(s"missing fields: ${missing.mkString(", ")}"))
    else if (
      !AnalyticsTableSchemas.matches(
        events.select(lateInputShape.map { case (name, _) => col(name) }*).schema,
        lateInputShape
      )
    ) Left(AnalyticsError.InvalidLateFactSchema("required fields have incompatible types"))
    else {
      AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(events, markerTokens).map { safe =>
        safe
          .withColumn(Columns.EventFingerprint, sha2(col(Columns.RawValue), 256))
          .withColumn(Columns.ApplicationId, lower(col(Columns.PayloadApplicationId)))
          .withColumn(Columns.JobId, lower(col(Columns.PayloadJobId)))
          .withColumn(Columns.NewStatus, col(Columns.PayloadNewStatus))
          .withColumn(Columns.JobSkills, col(Columns.PayloadJobSkills))
          .withColumn(Columns.AdmissionReason, lit(closedDayReason))
          .withColumn(Columns.IngestedAt, lit(Timestamp.from(observedAt)))
          .withColumn(Columns.ExpiresAt, lit(Timestamp.from(observedAt.plus(java.time.Duration.ofDays(retentionDays)))))
          // This projection retains only the normalized fields required by the existing report transforms. No raw
          // value, payload, actor, or direct subject identity is retained.
          .select(AnalyticsTableSchemas.lateFacts.map { case (name, _) => col(name) }*)
          .dropDuplicates(Columns.Topic, Columns.Partition, Columns.Offset)
      }
    }
  }

  private val lateInputShape: AnalyticsTableSchemas.Shape = Vector(
    Columns.EventId -> org.apache.spark.sql.types.StringType,
    Columns.RawValue -> org.apache.spark.sql.types.StringType,
    Columns.EventType -> org.apache.spark.sql.types.StringType,
    Columns.Topic -> org.apache.spark.sql.types.StringType,
    Columns.Partition -> org.apache.spark.sql.types.IntegerType,
    Columns.Offset -> org.apache.spark.sql.types.LongType,
    Columns.OccurredAt -> org.apache.spark.sql.types.TimestampType,
    Columns.AggregateType -> org.apache.spark.sql.types.StringType,
    Columns.AggregateId -> org.apache.spark.sql.types.StringType,
    Columns.Payload -> OperationalEventTransforms.payloadSchema,
    Columns.SubjectToken -> org.apache.spark.sql.types.StringType,
    Columns.SubjectTokens -> org.apache.spark.sql.types.ArrayType(
      org.apache.spark.sql.types.StringType,
      containsNull = false
    )
  )
}
