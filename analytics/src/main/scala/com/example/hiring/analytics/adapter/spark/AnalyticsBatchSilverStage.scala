package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.config.AnalyticsPositiveInt.*

import cats.effect.Async
import cats.syntax.all.*
import io.github.iltotore.iron.*
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.*

import java.time.Instant

/** Validates incoming events, quarantines malformed/conflicting rows, and merges Silver facts. */
private[spark] final class AnalyticsBatchSilverStage[F[_]: Async](ports: SilverStagePorts[F]) {
  import ports.*
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
      safeValidRecords <- blocking(safeValid.count())
      suppressedRecords = bronze.validRecords - safeValidRecords
      newConflicts <- blocking(OperationalEventTransforms.conflictingEventIds(safeValid))
      incomingSilver <- blocking.either(OperationalEventTransforms.silver(safeValid, pseudonymizer, markerTokens))
      incomingSilverSchema <- blocking(incomingSilver.schema)
      storedSilver <- deltaReader.readOrEmpty(spark, paths.silver, incomingSilverSchema)
      historicalConflicts <- blocking(
        safeValid
          .select("eventId", "rawValue")
          .withColumn("incomingFingerprint", sha2(col("rawValue"), 256))
          .join(
            storedSilver
              .select("eventId", "eventFingerprint")
              .withColumnRenamed("eventFingerprint", "storedFingerprint"),
            Seq("eventId"),
            "inner"
          )
          .filter(col("incomingFingerprint") =!= col("storedFingerprint"))
          .select("eventId")
          .distinct()
      )
      conflicts <- blocking(newConflicts.unionByName(historicalConflicts).distinct())
      conflictingEventIds <- blocking(conflicts.count())
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
      conflictingRecords <- blocking(conflictQuarantine.count())
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
      _ <- deltaWriter.merge(quarantine, paths.quarantine, "target.quarantineId = source.quarantineId")
    } yield AnalyticsPreparedEvents(
      incomingSilver,
      conflicts,
      bronze.validRecords,
      suppressedRecords,
      bronze.malformedRecords + conflictingRecords,
      conflictingEventIds
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
