package com.example.hiring.analytics.adapter.spark

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.config.{AnalyticsPositiveInt, AnalyticsRetentionSettings}
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.{
  AnalyticsLakehousePaths,
  AnalyticsReportPublicationReceipt,
  AnalyticsReportPublisher,
  AnalyticsReportReservation
}
import com.example.hiring.analytics.service.streaming.*
import io.delta.tables.DeltaTable
import com.example.hiring.analytics.config.AnalyticsPositiveInt.*
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*
import org.apache.spark.storage.StorageLevel

import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.Instant
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Adapter for one callback's parsed Kafka frame. The Resource owns its Spark cache through ingestion and publication.
  */
private[analytics] object SparkStreamingBatchStages {

  /** Own only incoming, source-bounded frames for one admission phase; never reuse them after a sink mutation. */
  private[analytics] def cacheAdmissionFrames[F[_]: Async](
      frames: Vector[DataFrame],
      execution: SparkExecution[F]
  ): Resource[F, Vector[DataFrame]] =
    frames.traverse(frame =>
      Resource
        .make(execution {
          val owned = frame.storageLevel == StorageLevel.NONE
          if (owned) frame.persist(StorageLevel.MEMORY_AND_DISK)
          (frame, owned)
        }) { case (cached, owned) =>
          if (owned) execution(cached.unpersist(blocking = true)).void else Async[F].unit
        }
        .map(_._1)
    )

  private[analytics] final case class AdmissionQuality(malformed: Long, conflicts: Long, future: Long, closed: Long)

  /** One aggregate action preserves malformed/future/closed row counts and distinct conflicting event IDs. */
  private[analytics] def measureQuality(
      malformedEvents: DataFrame,
      conflictingEvents: DataFrame,
      futureEvents: DataFrame,
      closedEvents: DataFrame
  ): AdmissionQuality = {
    val category = "_hiringAdmissionQuality"
    val records = malformedEvents
      .select(lit("MALFORMED").as(category))
      .unionByName(conflictingEvents.select(Columns.EventId).distinct().select(lit("CONFLICT").as(category)))
      .unionByName(futureEvents.select(lit("FUTURE").as(category)))
      .unionByName(closedEvents.select(lit("CLOSED").as(category)))
    val measured = records
      .agg(
        count(when(col(category) === lit("MALFORMED"), lit(1))),
        count(when(col(category) === lit("CONFLICT"), lit(1))),
        count(when(col(category) === lit("FUTURE"), lit(1))),
        count(when(col(category) === lit("CLOSED"), lit(1)))
      )
      .head()
    AdmissionQuality(measured.getLong(0), measured.getLong(1), measured.getLong(2), measured.getLong(3))
  }

  private[analytics] val AdmissionQualityCategory = "_hiringAdmissionQuality"

  private[analytics] def measureClassifiedQuality(
      malformedEvents: DataFrame,
      conflictingEvents: DataFrame,
      eventTimeCategories: DataFrame
  ): AdmissionQuality = {
    val category = AdmissionQualityCategory
    val records = malformedEvents
      .select(lit("MALFORMED").as(category))
      .unionByName(conflictingEvents.select(Columns.EventId).distinct().select(lit("CONFLICT").as(category)))
      .unionByName(eventTimeCategories.select(category))
    val measured = records
      .agg(
        count(when(col(category) === lit("MALFORMED"), lit(1))),
        count(when(col(category) === lit("CONFLICT"), lit(1))),
        count(when(col(category) === lit("FUTURE"), lit(1))),
        count(when(col(category) === lit("CLOSED"), lit(1)))
      )
      .head()
    AdmissionQuality(measured.getLong(0), measured.getLong(1), measured.getLong(2), measured.getLong(3))
  }

  /** The count is freshly measured in this admission scope. Keep normalization and native Delta writes at the caller.
    */
  private[analytics] def closedDayFactsSource(
      closedEvents: DataFrame,
      measuredClosedRows: Long
  )(notAlreadyAdmitted: => DataFrame): DataFrame =
    if (measuredClosedRows == 0L) closedEvents.limit(0) else notAlreadyAdmitted

  /** The caller supplies the exact marker vector used to construct the privacy-filtered frame. */
  private[analytics] def deletionSuppressedCount(
      tokenized: DataFrame,
      safe: DataFrame,
      activeMarkersPresent: Boolean
  ): Long = if (activeMarkersPresent) tokenized.count() - safe.count() else 0L

  def resource[F[_]: Async](
      spark: SparkSession,
      parsedEvents: DataFrame,
      paths: AnalyticsLakehousePaths,
      pseudonymizer: SubjectPseudonymizer,
      execution: SparkExecution[F],
      ingestionStage: AnalyticsBatchIngestionStage[F],
      silverStage: AnalyticsBatchSilverStage[F],
      lateFactStage: AnalyticsLateFactStage[F],
      deltaWriter: DeltaWriter[F],
      deltaReader: DeltaReader[F],
      reportPublisher: AnalyticsReportPublisher[F],
      retention: AnalyticsRetentionSettings,
      configureTables: F[Unit],
      applyActiveDeletions: Vector[SubjectToken] => F[Unit],
      authorizePublication: F[Unit]
  ): Resource[F, StreamingBatchStages[F]] =
    Resource
      .make(execution(parsedEvents.persist(StorageLevel.MEMORY_AND_DISK)))(frame =>
        execution(frame.unpersist(blocking = true)).void
      )
      .map(frame =>
        new LiveSparkStreamingBatchStages(
          spark,
          frame,
          paths,
          pseudonymizer,
          execution,
          ingestionStage,
          silverStage,
          lateFactStage,
          deltaWriter,
          deltaReader,
          reportPublisher,
          retention,
          configureTables,
          applyActiveDeletions,
          authorizePublication
        )
      )
}

private final class LiveSparkStreamingBatchStages[F[_]: Async](
    spark: SparkSession,
    parsedEvents: DataFrame,
    paths: AnalyticsLakehousePaths,
    pseudonymizer: SubjectPseudonymizer,
    execution: SparkExecution[F],
    ingestionStage: AnalyticsBatchIngestionStage[F],
    silverStage: AnalyticsBatchSilverStage[F],
    lateFactStage: AnalyticsLateFactStage[F],
    deltaWriter: DeltaWriter[F],
    deltaReader: DeltaReader[F],
    reportPublisher: AnalyticsReportPublisher[F],
    retention: AnalyticsRetentionSettings,
    configureTables: F[Unit],
    applyActiveDeletions: Vector[SubjectToken] => F[Unit],
    authorizePublication: F[Unit]
) extends StreamingBatchStages[F] {
  private val F = Async[F]
  private val AdmissionColumn = "_hiringEventTimeAdmission"
  private val EffectiveTimeColumn = "_hiringEffectiveEventTime"
  private val AdmissionStatusField = "status"
  private val EffectiveTimeField = "effectiveTime"

  private final case class AdmissionFacts(
      safeValidEvents: DataFrame,
      incomingSilver: DataFrame,
      conflicts: DataFrame,
      classifications: DataFrame,
      existingFacts: DataFrame,
      existingReportFacts: DataFrame,
      currentBatchCoordinates: DataFrame,
      malformedCount: Long,
      conflictingEventCount: Long,
      futureCount: Long,
      closedCount: Long,
      deletionSuppressedCount: Long,
      newlyAdmittedOpenFacts: DataFrame
  )

  override def reservePublication(
      preparation: StreamingInputPreparation,
      revision: Long
  ): F[AnalyticsReportReservation] =
    for {
      _ <- authorizePublication
      runId <- F.fromEither(streamRunId(preparation.identity, revision))
      rangeFingerprint <- F.fromEither(streamRangeFingerprint(preparation, revision))
      reservation <- reportPublisher.reservePinned(runId, rangeFingerprint, preparation.observedAt)
    } yield reservation

  override def publicationReceipt(
      preparation: StreamingInputPreparation,
      decision: StreamingDecisionRevision
  )(using cats.Applicative[F]): F[AnalyticsReportPublicationReceipt] =
    authorizePublication *> reportPublisher.publicationReceipt(decision.publicationReservation)

  override def assess(
      preparation: StreamingInputPreparation,
      activeTokens: Vector[SubjectToken],
      isRecoveryAttempt: Boolean
  ): F[StreamingIngestionResult] =
    withMarkers(activeTokens).flatMap { markerFrame =>
      analyze(preparation, markerFrame, activeTokens.nonEmpty, isRecoveryAttempt).use { facts =>
        if (facts.deletionSuppressedCount > 0L) F.pure(StreamingIngestionResult.ErasurePending)
        else if (facts.malformedCount > 0L || facts.conflictingEventCount > 0L || facts.futureCount > 0L)
          F.pure(StreamingIngestionResult.QualityBlocked)
        else
          unsuppressedEventTimes(facts).map(StreamingIngestionResult.Ready.apply)
      }
    }

  override def ingest(
      preparation: StreamingInputPreparation,
      activeTokens: Vector[SubjectToken],
      assessment: StreamingIngestionResult,
      decision: StreamingDecisionRevision,
      isRecoveryAttempt: Boolean
  ): F[Unit] =
    for {
      _ <- validateDecision(preparation, activeTokens, decision)
      _ <- if (activeTokens.nonEmpty) applyActiveDeletions(activeTokens) else F.unit
      markerFrame <- withMarkers(activeTokens)
      bronze <- ingestionStage.persistParsed(
        spark,
        parsedEvents,
        markerFrame,
        configureTables,
        F.pure(preparation.observedAt),
        _ => F.unit
      )
      _ <- silverStage.quarantinePreparation(spark, bronze, markerFrame, activeTokens.nonEmpty).use { prepared =>
        classifyForWriting(preparation, markerFrame, activeTokens.nonEmpty, prepared, isRecoveryAttempt).use {
          admission =>
            for {
              _ <- persistFutureQuarantine(admission, preparation.observedAt)
              lateEvents <- lateFactsNotAlreadyAdmitted(admission)
              _ <- lateFactStage.persistClosedDayFacts(lateEvents, markerFrame, preparation.observedAt)
              openEvents <- openSilverFacts(preparation, prepared)
              _ <- silverStage.mergeSilver(prepared.copy(incomingSilver = openEvents), preparation.observedAt)
              _ <- ensureAssessmentOutcome(assessment, admission)
            } yield ()
        }
      }
    } yield ()

  override def publish(
      preparation: StreamingInputPreparation,
      decision: StreamingDecisionRevision,
      activeTokens: Vector[SubjectToken]
  ): F[StreamingPublicationResult] =
    if (activeTokens.nonEmpty) F.pure(StreamingPublicationResult.ErasurePending)
    else
      for {
        _ <- validateDecision(preparation, activeTokens, decision)
        _ <- authorizePublication
        receipt <- reportPublisher.publicationReceipt(decision.publicationReservation)
        result <- receipt match {
          case AnalyticsReportPublicationReceipt.Superseded        => F.pure(StreamingPublicationResult.Superseded)
          case AnalyticsReportPublicationReceipt.CurrentGeneration => F.pure(StreamingPublicationResult.Published)
          case AnalyticsReportPublicationReceipt.Absent            =>
            for {
              report <- rebuildAndExtract(preparation.observedAt)
              _ <- authorizePublication
              expiresAt = preparation.observedAt.plusMillis(retention.publishedSnapshotDays.value.toLong * 86400000L)
              result <- reportPublisher
                .publish(decision.publicationReservation, report, expiresAt)
                .as(StreamingPublicationResult.Published)
                .handleErrorWith {
                  case AnalyticsError.GuardedErasurePublicationRejected => F.pure(StreamingPublicationResult.Superseded)
                  case error                                            => F.raiseError(error)
                }
            } yield result
        }
      } yield result

  private def analyze(
      preparation: StreamingInputPreparation,
      markerFrame: DataFrame,
      activeMarkersPresent: Boolean,
      isRecoveryAttempt: Boolean
  ): Resource[F, AdmissionFacts] =
    privacySafeSource(markerFrame).flatMap { case (tokenized, safe) =>
      Resource
        .eval(execution.either(OperationalEventTransforms.silver(safe, pseudonymizer, markerFrame)))
        .flatMap(cacheFrame)
        .flatMap { incomingSilver =>
          Resource
            .eval(for {
              storedSilver <- deltaReader.readOrEmpty(spark, paths.silver, incomingSilver.schema)
              storedLate <- deltaReader.readOrEmpty(
                spark,
                paths.lateFacts,
                AnalyticsTableSchemas.struct(AnalyticsTableSchemas.lateFacts)
              )
              storedBronze <- deltaReader.readOrEmpty(
                spark,
                paths.bronze,
                AnalyticsTableSchemas.struct(AnalyticsTableSchemas.bronze)
              )
              facts <- execution.either(
                buildAdmissionFacts(
                  preparation,
                  safe,
                  tokenized,
                  incomingSilver,
                  storedSilver,
                  storedLate,
                  storedBronze,
                  activeMarkersPresent,
                  isRecoveryAttempt
                )
              )
            } yield facts)
            .flatMap(cacheAdmission)
            .evalMap(facts => measureAdmissionFacts(facts, assessmentEventTimeCategories(facts)))
        }
    }

  /** Register the safe source cache before Silver's tokenizer captures its physical RDD. */
  private def privacySafeSource(markerFrame: DataFrame): Resource[F, (DataFrame, DataFrame)] =
    Resource
      .eval(for {
        valid <- execution(OperationalEventTransforms.validEvents(parsedEvents))
        tokenized <- execution(AnalyticsSubjectPrivacy.withSubjectToken(valid, pseudonymizer))
        safe <- execution.either(AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(tokenized, markerFrame))
      } yield (tokenized, safe))
      .flatMap { case (tokenized, safe) =>
        cacheFrame(safe).as((tokenized, safe))
      }

  private def cacheFrame(frame: DataFrame): Resource[F, DataFrame] =
    SparkStreamingBatchStages.cacheAdmissionFrames(Vector(frame), execution).as(frame)

  private def cacheAdmission(facts: AdmissionFacts): Resource[F, AdmissionFacts] =
    SparkStreamingBatchStages
      .cacheAdmissionFrames(
        Vector(facts.conflicts, facts.classifications),
        execution
      )
      .as(facts)

  /** Assess and ingest must compare the same measured quality evidence; unmeasured defaults are not outcomes. */
  private def assessmentEventTimeCategories(facts: AdmissionFacts): DataFrame =
    facts.classifications
      .filter(col(s"$AdmissionColumn.$AdmissionStatusField").isin("FUTURE", "CLOSED"))
      .select(col(s"$AdmissionColumn.$AdmissionStatusField").as(SparkStreamingBatchStages.AdmissionQualityCategory))

  private def ingestionEventTimeCategories(facts: AdmissionFacts): DataFrame =
    facts.classifications
      .filter(col(s"$AdmissionColumn.$AdmissionStatusField").isin("FUTURE", "CLOSED"))
      .join(facts.conflicts, Seq(Columns.EventId), "left_anti")
      .select(col(s"$AdmissionColumn.$AdmissionStatusField").as(SparkStreamingBatchStages.AdmissionQualityCategory))

  private def measureAdmissionFacts(facts: AdmissionFacts, eventTimeCategories: => DataFrame): F[AdmissionFacts] =
    execution {
      val malformed = parsedEvents.filter(!OperationalEventTransforms.isValidEvent)
      val conflicts = facts.safeValidEvents
        .select(Columns.EventId)
        .join(facts.conflicts, Seq(Columns.EventId), "inner")
      val measured = SparkStreamingBatchStages.measureClassifiedQuality(malformed, conflicts, eventTimeCategories)
      facts.copy(
        malformedCount = measured.malformed,
        conflictingEventCount = measured.conflicts,
        futureCount = measured.future,
        closedCount = measured.closed
      )
    }

  private def buildAdmissionFacts(
      preparation: StreamingInputPreparation,
      safe: DataFrame,
      tokenized: DataFrame,
      incomingSilver: DataFrame,
      storedSilver: DataFrame,
      storedLate: DataFrame,
      storedBronze: DataFrame,
      activeMarkersPresent: Boolean,
      isRecoveryAttempt: Boolean
  ): Either[AnalyticsError, AdmissionFacts] = {
    val validSilver = AnalyticsTableSchemas.matches(incomingSilver.schema, AnalyticsTableSchemas.silver)
    val lateShapeValid = AnalyticsTableSchemas.matches(storedLate.schema, AnalyticsTableSchemas.lateFacts)
    val silverShapeValid = AnalyticsTableSchemas.matches(storedSilver.schema, AnalyticsTableSchemas.silver) ||
      AnalyticsTableSchemas.matches(storedSilver.schema, AnalyticsTableSchemas.silver ++ AnalyticsTableSchemas.expiry)
    val bronzeShapeValid = AnalyticsTableSchemas.matches(storedBronze.schema, AnalyticsTableSchemas.bronze)
    if (!validSilver) Left(AnalyticsError.InvalidSilverSchema)
    else if (!lateShapeValid)
      Left(AnalyticsError.InvalidLateFactSchema("persisted dataset does not match its declared schema"))
    else if (!silverShapeValid) Left(AnalyticsError.InvalidSilverSchema)
    else if (!bronzeShapeValid)
      Left(
        AnalyticsError.LakehouseFailure(
          new IllegalStateException("persisted Bronze dataset has an incompatible schema")
        )
      )
    else {
      val localConflicts = OperationalEventTransforms.conflictingEventIds(safe)
      val activeSilver = withoutExpired(storedSilver, preparation.observedAt)
      val activeLate = withoutExpired(storedLate, preparation.observedAt)
      val persistedFingerprints = activeSilver
        .select(Columns.EventId, Columns.EventFingerprint)
        .unionByName(activeLate.select(Columns.EventId, Columns.EventFingerprint))
        .distinct()
      val historicalConflicts = safe
        .select(Columns.EventId, Columns.RawValue)
        .withColumn("incomingFingerprint", sha2(col(Columns.RawValue), 256))
        .join(
          persistedFingerprints.withColumnRenamed(Columns.EventFingerprint, "storedFingerprint"),
          Seq(Columns.EventId),
          "inner"
        )
        .filter(col("incomingFingerprint") =!= col("storedFingerprint"))
        .select(Columns.EventId)
        .distinct()
      val coordinateConflicts = safe
        .select(Columns.EventId, Columns.Topic, Columns.Partition, Columns.Offset, Columns.RawValue)
        .withColumn("incomingFingerprint", sha2(col(Columns.RawValue), 256))
        .join(
          storedBronze
            .select(Columns.Topic, Columns.Partition, Columns.Offset, Columns.RawValue)
            .withColumn("storedCoordinateFingerprint", sha2(col(Columns.RawValue), 256)),
          Seq(Columns.Topic, Columns.Partition, Columns.Offset),
          "inner"
        )
        .filter(col("incomingFingerprint") =!= col("storedCoordinateFingerprint"))
        .select(Columns.EventId)
        .distinct()
      val conflicts = localConflicts.unionByName(historicalConflicts).unionByName(coordinateConflicts).distinct()
      val incomingPairs = incomingSilver.select(Columns.EventId, Columns.EventFingerprint)
      val selectedValid = safe
        .withColumn(Columns.EventFingerprint, sha2(col(Columns.RawValue), 256))
        .join(incomingPairs, Seq(Columns.EventId, Columns.EventFingerprint), "inner")
        .join(conflicts, Seq(Columns.EventId), "left_anti")
      val classified = withEventTimeAdmission(selectedValid, preparation)
      val existingFingerprints = persistedFingerprints.distinct()
      val watermarkFacts = if (isRecoveryAttempt) selectedValid else classified
      val alreadyAdmitted = watermarkFacts
        .select(Columns.EventId, Columns.EventFingerprint)
        .join(existingFingerprints, Seq(Columns.EventId, Columns.EventFingerprint), "inner")
        .select(Columns.EventId)
        .distinct()
      val previouslyWrittenCoordinates = storedBronze
        .select(Columns.Topic, Columns.Partition, Columns.Offset, Columns.RawValue)
        .withColumn(Columns.EventFingerprint, sha2(col(Columns.RawValue), 256))
        .select(Columns.Topic, Columns.Partition, Columns.Offset, Columns.EventFingerprint)
        .distinct()
      val sourceCoordinates = watermarkFacts
        .select(Columns.EventId, Columns.Topic, Columns.Partition, Columns.Offset, Columns.EventFingerprint)
      val sameBatchCoordinates = sourceCoordinates
        .join(
          previouslyWrittenCoordinates,
          Seq(Columns.Topic, Columns.Partition, Columns.Offset, Columns.EventFingerprint),
          "inner"
        )
        .select(Columns.EventId)
        .distinct()
      val idsWithPriorBronzeCoordinate = sourceCoordinates
        .join(
          previouslyWrittenCoordinates,
          Seq(Columns.Topic, Columns.Partition, Columns.Offset, Columns.EventFingerprint),
          "inner"
        )
        .select(Columns.EventId)
        .distinct()
      val newByIdentity = watermarkFacts
        .select(Columns.EventId)
        .distinct()
        .join(alreadyAdmitted, Seq(Columns.EventId), "left_anti")
        .join(idsWithPriorBronzeCoordinate, Seq(Columns.EventId), "left_anti")
      val newBySameBatchCoordinate =
        if (isRecoveryAttempt)
          watermarkFacts
            .select(Columns.EventId)
            .distinct()
            .join(sameBatchCoordinates, Seq(Columns.EventId), "inner")
            .join(alreadyAdmitted, Seq(Columns.EventId), "left_anti")
        else watermarkFacts.limit(0).select(Columns.EventId)
      val newlyAdmittedIds = newByIdentity.unionByName(newBySameBatchCoordinate).distinct()
      val newlyAdmitted = classified
        .join(newlyAdmittedIds, Seq(Columns.EventId), "inner")
        .filter(col(s"$AdmissionColumn.$AdmissionStatusField") === lit("OPEN"))
        .dropDuplicates(Columns.EventId)
      Right(
        AdmissionFacts(
          safe,
          incomingSilver,
          conflicts,
          classified,
          persistedFingerprints,
          activeSilver,
          previouslyWrittenCoordinates,
          0L,
          0L,
          0L,
          0L,
          SparkStreamingBatchStages.deletionSuppressedCount(tokenized, safe, activeMarkersPresent),
          newlyAdmitted
        )
      )
    }
  }

  /** Candidate watermark inputs must contribute to at least one visible K-suppressed report group. */
  private def unsuppressedEventTimes(facts: AdmissionFacts): F[Vector[Instant]] = execution {
    val candidates = facts.incomingSilver
      .join(
        facts.newlyAdmittedOpenFacts.select(Columns.EventId, EffectiveTimeColumn),
        Seq(Columns.EventId),
        "inner"
      )
    StreamingWatermarkAdmission
      .visibleCandidateEvents(
        facts.existingReportFacts,
        candidates,
        EffectiveTimeColumn
      )
      .select(col(EffectiveTimeColumn))
      .filter(col(EffectiveTimeColumn).isNotNull)
      .distinct()
      .collect()
      .toVector
      .flatMap(row => Option(row.getAs[Timestamp](0)).map(_.toInstant))
  }

  private def classifyForWriting(
      preparation: StreamingInputPreparation,
      markerFrame: DataFrame,
      activeMarkersPresent: Boolean,
      prepared: AnalyticsPreparedEvents,
      isRecoveryAttempt: Boolean
  ): Resource[F, AdmissionFacts] =
    privacySafeSource(markerFrame).flatMap { case (tokenized, safe) =>
      // Quarantine preparation already built this projection from the same source/keys/markers after Bronze.
      // Historical admission remains fresh: reload all persisted facts and compare the full conflict set below.
      cacheFrame(prepared.incomingSilver).flatMap { incomingSilver =>
        Resource
          .eval(for {
            storedSilver <- deltaReader.readOrEmpty(spark, paths.silver, incomingSilver.schema)
            storedLate <- deltaReader.readOrEmpty(
              spark,
              paths.lateFacts,
              AnalyticsTableSchemas.struct(AnalyticsTableSchemas.lateFacts)
            )
            storedBronze <- deltaReader.readOrEmpty(
              spark,
              paths.bronze,
              AnalyticsTableSchemas.struct(AnalyticsTableSchemas.bronze)
            )
            facts <- execution.either(
              buildAdmissionFacts(
                preparation,
                safe,
                tokenized,
                incomingSilver,
                storedSilver,
                storedLate,
                storedBronze,
                activeMarkersPresent,
                isRecoveryAttempt
              )
            )
          } yield facts)
          .flatMap { facts =>
            SparkStreamingBatchStages
              .cacheAdmissionFrames(Vector(facts.conflicts, prepared.conflicts), execution)
              .evalMap { _ =>
                execution.either(
                  Either.cond(
                    facts.conflicts.join(prepared.conflicts, Seq(Columns.EventId), "left_anti").limit(1).count() == 0L,
                    facts.copy(conflicts = prepared.conflicts),
                    AnalyticsError.LakehouseFailure(
                      new IllegalStateException("streaming admission conflict set changed during ingestion")
                    )
                  )
                )
              }
          }
          .flatMap(cacheAdmission)
          .evalMap(facts => measureAdmissionFacts(facts, ingestionEventTimeCategories(facts)))
      }
    }

  private def persistFutureQuarantine(facts: AdmissionFacts, observedAt: Instant): F[Unit] =
    execution {
      val future = facts.classifications
        .filter(col(s"$AdmissionColumn.$AdmissionStatusField") === lit("FUTURE"))
        .join(facts.conflicts, Seq(Columns.EventId), "left_anti")
        .withColumn("payloadHash", sha2(col(Columns.RawValue), 256))
        .withColumn(
          "quarantineId",
          concat(
            lit("future:"),
            sha2(
              to_json(
                struct(
                  col(Columns.Topic).as("topic"),
                  col(Columns.Partition).as("partition"),
                  col(Columns.Offset).as("offset")
                )
              ),
              256
            )
          )
        )
        .withColumn("quarantineReason", lit("EVENT_TIMESTAMP_TOO_FAR_IN_FUTURE"))
        .withColumn(
          Columns.ExpiresAt,
          lit(
            Timestamp.from(
              observedAt.plus(
                java.time.Duration.ofDays(
                  retention.quarantineDays.value.toLong
                )
              )
            )
          )
        )
        // The serialized envelope is used only to derive a fingerprint and is removed before Delta persistence.
        .drop(Columns.RawValue, Columns.ActorId, Columns.Payload, Columns.SubjectToken)
        .select(AnalyticsTableSchemas.quarantine.map { case (name, _) => col(name) }*)
      // The fresh admission measurement proves this projection empty; the native writer still validates it.
      if (facts.futureCount == 0L) future.limit(0) else future
    }.flatMap { future =>
      deltaWriter.merge(
        future,
        paths.quarantine,
        "target.quarantineId = source.quarantineId"
      )
    }

  private def lateFactsNotAlreadyAdmitted(facts: AdmissionFacts): F[DataFrame] = execution {
    val closed = facts.classifications.filter(col(s"$AdmissionColumn.$AdmissionStatusField") === lit("CLOSED"))
    SparkStreamingBatchStages
      .closedDayFactsSource(closed, facts.closedCount) {
        val eligiblePairs = facts.incomingSilver
          .select(Columns.EventId, Columns.EventFingerprint)
          .join(facts.conflicts, Seq(Columns.EventId), "left_anti")
        val existingFactIds = facts.existingFacts.select(Columns.EventId).distinct()
        closed
          .join(eligiblePairs, Seq(Columns.EventId, Columns.EventFingerprint), "inner")
          .join(facts.conflicts, Seq(Columns.EventId), "left_anti")
          .join(existingFactIds, Seq(Columns.EventId), "left_anti")
      }
      .drop(AdmissionColumn, EffectiveTimeColumn, Columns.EventFingerprint)
      .dropDuplicates(Columns.Topic, Columns.Partition, Columns.Offset)
  }

  private def openSilverFacts(
      preparation: StreamingInputPreparation,
      prepared: AnalyticsPreparedEvents
  ): F[DataFrame] = execution {
    withEventTimeAdmission(prepared.incomingSilver, preparation)
      .filter(col(s"$AdmissionColumn.$AdmissionStatusField") === lit("OPEN"))
      .select(AnalyticsTableSchemas.silver.map { case (name, _) => col(name) }*)
  }

  private def withEventTimeAdmission(frame: DataFrame, preparation: StreamingInputPreparation): DataFrame = {
    val schema = StructType(
      Vector(
        StructField(AdmissionStatusField, StringType, nullable = false),
        StructField(EffectiveTimeField, TimestampType, nullable = true)
      )
    )
    val observedAt = preparation.observedAt
    val previousWatermark = preparation.priorWatermark
    val classify = udf(
      (timestamp: Timestamp) => {
        if (timestamp == null) Row("FUTURE", null)
        else
          AnalyticsEventTimePolicy.admit(timestamp.toInstant, observedAt, previousWatermark) match {
            case EventTimeAdmission.Admitted(effective) => Row("OPEN", Timestamp.from(effective))
            case EventTimeAdmission.LateClosedDay(_)    => Row("CLOSED", null)
            case EventTimeAdmission.TooFarInFuture      => Row("FUTURE", null)
          }
      },
      schema
    )
    frame
      .withColumn(AdmissionColumn, classify(col(Columns.OccurredAt)))
      .withColumn(EffectiveTimeColumn, col(s"$AdmissionColumn.$EffectiveTimeField"))
  }

  private def withoutExpired(frame: DataFrame, observedAt: Instant): DataFrame =
    if (frame.columns.contains(Columns.ExpiresAt))
      frame.filter(col(Columns.ExpiresAt).isNull || col(Columns.ExpiresAt) > lit(Timestamp.from(observedAt)))
    else frame

  private def withMarkers(tokens: Vector[SubjectToken]): F[DataFrame] = execution {
    val schema = StructType(Vector(StructField(Columns.SubjectToken, StringType, nullable = false)))
    spark.createDataFrame(tokens.map(token => Row(token.value)).asJava, schema)
  }

  private def validateDecision(
      preparation: StreamingInputPreparation,
      tokens: Vector[SubjectToken],
      decision: StreamingDecisionRevision
  ): F[Unit] = {
    val expectedMarkerFingerprint = AnalyticsDigest.sha256Hex(
      tokens.map(_.value).sorted.mkString("\n").getBytes(StandardCharsets.UTF_8)
    )
    F.raiseWhen(
      decision.identity != preparation.identity || decision.deletionMarkerFingerprint != expectedMarkerFingerprint
    )(AnalyticsError.InvalidConfiguration("streaming batch decision does not match its input or deletion markers"))
  }

  private def ensureAssessmentOutcome(
      assessment: StreamingIngestionResult,
      facts: AdmissionFacts
  ): F[Unit] = {
    val shouldBeQualityBlocked = facts.malformedCount > 0L || facts.conflictingEventCount > 0L || facts.futureCount > 0L
    val shouldBeErasurePending = facts.deletionSuppressedCount > 0L
    val valid = assessment match {
      case StreamingIngestionResult.Ready(_)       => !shouldBeQualityBlocked && !shouldBeErasurePending
      case StreamingIngestionResult.QualityBlocked => shouldBeQualityBlocked
      case StreamingIngestionResult.ErasurePending => shouldBeErasurePending
    }
    F.raiseWhen(!valid)(
      AnalyticsError.InvalidConfiguration("streaming admission outcome changed before durable ingestion")
    )
  }

  private def rebuildAndExtract(asOf: Instant): F[AnalyticsReportOutput] =
    execution(DeltaTable.isDeltaTable(spark, SparkPhysicalLocation.resolve(paths.silver))).flatMap {
      case true =>
        execution(spark.read.format("delta").load(SparkPhysicalLocation.resolve(paths.silver)))
          .flatMap(AnalyticsGoldStage.rebuild(paths, _, execution)) *>
          AnalyticsGoldStage.extract(spark, paths, asOf, execution)
      case false =>
        AnalyticsGoldStage.clear(spark, paths, execution) *>
          AnalyticsGoldStage.extract(spark, paths, asOf, execution)
    }

  private def streamRunId(identity: StreamingBatchIdentity, revision: Long): Either[AnalyticsError, RunId] = {
    val digest = AnalyticsDigest.sha256Hex(
      s"${identity.lineage.value}\n${identity.batchId.value}\n$revision".getBytes(StandardCharsets.UTF_8)
    )
    RunId.from(s"stream-$digest").leftMap(AnalyticsError.InvalidConfiguration.apply)
  }

  private def streamRangeFingerprint(
      preparation: StreamingInputPreparation,
      revision: Long
  ): Either[AnalyticsError, RangeFingerprint] = {
    val canonical = Vector(
      preparation.identity.lineage.value,
      preparation.identity.batchId.value.toString,
      preparation.inputFingerprint.value,
      revision.toString
    ).mkString("\n")
    RangeFingerprint
      .from(AnalyticsDigest.sha256Hex(canonical.getBytes(StandardCharsets.UTF_8)))
      .leftMap(AnalyticsError.InvalidConfiguration.apply)
  }

}

private[analytics] object StreamingWatermarkAdmission {

  /** Return only candidate events that contribute to a report group surviving K=10 suppression. */
  def visibleCandidateEvents(
      existingReportFacts: DataFrame,
      candidates: DataFrame,
      effectiveTimeColumn: String
  ): DataFrame = {
    val reportColumns = AnalyticsTableSchemas.silver.map(_._1)
    val allFacts = existingReportFacts
      .select(reportColumns.map(col)*)
      .unionByName(
        candidates.select(reportColumns.map(col)*)
      )
    val visibleFunnelDays = HiringGoldTransforms
      .wideFunnelDay(allFacts)
      .fold(
        error => throw error,
        identity
      )
    val funnelCandidates = candidates
      .filter(
        col(Columns.EventType).isin(
          AnalyticsEventType.ApplicationCreated.wire,
          AnalyticsEventType.ApplicationStatusChanged.wire
        )
      )
      .withColumn(Columns.Day, date_trunc(Columns.Day, col(Columns.OccurredAt)))
      .join(visibleFunnelDays.select(Columns.Day), Seq(Columns.Day), "inner")
      .select(col(effectiveTimeColumn))
    val visibleSkills = allFacts
      .filter(col(Columns.EventType) === lit(AnalyticsEventType.JobCreated.wire))
      .withColumn("_day", date_trunc("day", col(Columns.OccurredAt)))
      .withColumn("_rawSkill", explode(col(Columns.JobSkills)))
      .withColumn("_skill", lower(trim(col("_rawSkill"))))
      .filter(length(col("_skill")) > lit(0))
      .groupBy(col("_day"), col("_skill"))
      .agg(countDistinct(col(Columns.SubjectToken)).as("_contributors"))
      .filter(col("_contributors") >= lit(AnalyticsRetention.MinimumContributors))
    val skillCandidates = candidates
      .filter(col(Columns.EventType) === lit(AnalyticsEventType.JobCreated.wire))
      .withColumn("_day", date_trunc("day", col(Columns.OccurredAt)))
      .withColumn("_rawSkill", explode(col(Columns.JobSkills)))
      .withColumn("_skill", lower(trim(col("_rawSkill"))))
      .join(visibleSkills.select("_day", "_skill"), Seq("_day", "_skill"), "inner")
      .select(col(effectiveTimeColumn))
    funnelCandidates.unionByName(skillCandidates).filter(col(effectiveTimeColumn).isNotNull).distinct()
  }
}
