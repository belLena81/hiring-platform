package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.config.AnalyticsPositiveInt.*
import com.example.hiring.analytics.config.AnalyticsRetentionSettings
import com.example.hiring.analytics.domain.SubjectPseudonymizer
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

import cats.effect.Async
import cats.syntax.all.*
import io.github.iltotore.iron.*
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.*

import java.sql.Timestamp
import java.time.Instant

private final case class AnalyticsBatchConflictCounts(
    safeValidRecords: Long,
    conflictingEventIds: Long,
    conflictingRecords: Long
)

/** Validates incoming events, quarantines malformed/conflicting rows, and merges Silver facts. */
private[analytics] final class AnalyticsBatchSilverStage[F[_]: Async](
    paths: AnalyticsLakehousePaths,
    pseudonymizer: SubjectPseudonymizer,
    execution: SparkExecution[F],
    deltaWriter: DeltaWriter[F],
    deltaReader: DeltaReader[F],
    quarantineId: QuarantineId,
    retention: AnalyticsRetentionSettings
) {
  private val blocking = execution

  def separateQuarantine(
      spark: SparkSession,
      bronze: AnalyticsBronzeInput,
      markerTokens: DataFrame,
      activeMarkersPresent: Boolean
  ): F[AnalyticsPreparedEvents] = {
    val parsed = bronze.frame
    for {
      valid <- blocking(OperationalEventTransforms.validEvents(parsed))
      pseudonymizedValid <- blocking(AnalyticsSubjectPrivacy.withSubjectToken(valid, pseudonymizer))
      safeValid <- blocking.either(
        AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(pseudonymizedValid, markerTokens)
      )
      malformed <- blocking(
        deltaWriter.withExpiry(
          AnalyticsSubjectPrivacy
            .withSubjectToken(OperationalEventTransforms.malformedEvents(parsed), pseudonymizer)
            .withColumn("quarantineId", quarantineId())
            .withColumn("quarantineReason", lit("INVALID_OPERATIONAL_EVENT_ENVELOPE")),
          bronze.startedAt,
          retention.quarantineDays.value
        )
      )
      malformedToPersist <- if (activeMarkersPresent) blocking(malformed.limit(0)) else Async[F].pure(malformed)
      newConflicts <- blocking(OperationalEventTransforms.conflictingEventIds(safeValid))
      incomingSilver <- blocking.either(OperationalEventTransforms.silver(safeValid, pseudonymizer, markerTokens))
      incomingSilverSchema <- blocking(incomingSilver.schema)
      storedSilver <- deltaReader.readOrEmpty(spark, paths.silver, incomingSilverSchema)
      storedLateFacts <- deltaReader.readOrEmpty(
        spark,
        paths.lateFacts,
        AnalyticsTableSchemas.struct(AnalyticsTableSchemas.lateFacts)
      )
      storedLateFactFingerprints <- blocking.either {
        Either.cond(
          AnalyticsTableSchemas.matches(storedLateFacts.schema, AnalyticsTableSchemas.lateFacts),
          storedLateFacts
            .filter(col(Columns.ExpiresAt).isNull || col(Columns.ExpiresAt) > lit(Timestamp.from(bronze.startedAt)))
            .select("eventId", "eventFingerprint"),
          AnalyticsError.InvalidLateFactSchema("persisted dataset does not match its declared schema")
        )
      }
      historicalConflicts <- blocking(
        safeValid
          .select("eventId", "rawValue")
          .withColumn("incomingFingerprint", sha2(col("rawValue"), 256))
          .join(
            storedSilver
              .select("eventId", "eventFingerprint")
              .unionByName(storedLateFactFingerprints)
              .withColumnRenamed("eventFingerprint", "storedFingerprint"),
            Seq("eventId"),
            "inner"
          )
          .filter(col("incomingFingerprint") =!= col("storedFingerprint"))
          .select("eventId")
          .distinct()
      )
      conflicts <- blocking(newConflicts.unionByName(historicalConflicts).distinct())
      conflictCounts <- blocking {
        val row = safeValid
          .alias("safe")
          .join(
            conflicts.alias("conflicts"),
            col("safe.eventId") === col("conflicts.eventId"),
            "left"
          )
          .agg(
            count(lit(1)).as("safeValidRecords"),
            countDistinct(col("conflicts.eventId")).as("conflictingEventIds"),
            count(col("conflicts.eventId")).as("conflictingRecords")
          )
          .head()
        AnalyticsBatchConflictCounts(
          row.getLong(0),
          row.getLong(1),
          row.getLong(2)
        )
      }
      suppressedRecords = bronze.validRecords - conflictCounts.safeValidRecords
      conflictQuarantine <- blocking(
        deltaWriter.withExpiry(
          AnalyticsSubjectPrivacy
            .withSubjectToken(safeValid.join(conflicts, Seq("eventId"), "inner"), pseudonymizer)
            .withColumn("quarantineId", quarantineId())
            .withColumn("quarantineReason", lit("CONFLICTING_EVENT_ID")),
          bronze.startedAt,
          retention.quarantineDays.value
        )
      )
      quarantine <- blocking(
        malformedToPersist
          .unionByName(conflictQuarantine)
          .withColumn("payloadHash", sha2(col("rawValue"), 256))
          .drop("rawValue", "actorId", "payload", "subjectToken")
          .select(
            "topic",
            "partition",
            "offset",
            "payloadHash",
            "subjectTokens",
            "quarantineId",
            "quarantineReason",
            "expiresAt"
          )
      )
      // Keep native target creation, schema validation and MERGE analysis even when measured input is empty.
      quarantineSource <- blocking {
        if (bronze.malformedRecords == 0L && conflictCounts.conflictingRecords == 0L) quarantine.limit(0)
        else quarantine
      }
      _ <- deltaWriter.merge(quarantineSource, paths.quarantine, "target.quarantineId = source.quarantineId")
    } yield AnalyticsPreparedEvents(
      incomingSilver,
      conflicts,
      bronze.validRecords,
      suppressedRecords,
      bronze.malformedRecords + conflictCounts.conflictingRecords,
      conflictCounts.conflictingEventIds
    )
  }

  def mergeSilver(prepared: AnalyticsPreparedEvents, startedAt: Instant): F[DataFrame] =
    for {
      silver <- blocking(
        deltaWriter.withExpiry(
          prepared.incomingSilver.join(prepared.conflicts, Seq("eventId"), "left_anti"),
          startedAt,
          retention.silverDays.value
        )
      )
      _ <- deltaWriter.merge(silver, paths.silver, "target.eventId = source.eventId")
    } yield silver
}
