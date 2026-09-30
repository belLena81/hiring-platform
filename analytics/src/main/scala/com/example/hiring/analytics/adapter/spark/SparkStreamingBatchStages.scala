package com.example.hiring.analytics.adapter.spark

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.config.{AnalyticsPositiveInt, AnalyticsRetentionSettings}
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.{
  AnalyticsLakehousePaths,
  AnalyticsReportPublicationReceipt,
  AnalyticsReportPublisher
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
      deletionSuppressedCount: Long,
      newlyAdmittedOpenFacts: DataFrame
  )

  override def publicationReceipt(
      preparation: StreamingInputPreparation,
      decision: StreamingDecisionRevision
  )(using cats.Applicative[F]): F[AnalyticsReportPublicationReceipt] =
    for {
      _ <- authorizePublication
      runId <- F.fromEither(streamRunId(decision))
      rangeFingerprint <- F.fromEither(streamRangeFingerprint(preparation, decision))
      reservation <- reportPublisher.reserve(runId, rangeFingerprint, preparation.observedAt)
      receipt <- reportPublisher.publicationReceipt(reservation)
    } yield receipt

  override def assess(
      preparation: StreamingInputPreparation,
      activeTokens: Vector[SubjectToken],
      isRecoveryAttempt: Boolean
  ): F[StreamingIngestionResult] =
    withMarkers(activeTokens).flatMap { markerFrame =>
      analyze(preparation, markerFrame, isRecoveryAttempt).flatMap { facts =>
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
      prepared <- silverStage.separateQuarantine(spark, bronze, markerFrame, activeTokens.nonEmpty)
      admission <- classifyForWriting(preparation, markerFrame, prepared, isRecoveryAttempt)
      _ <- persistFutureQuarantine(admission, preparation.observedAt)
      lateEvents <- lateFactsNotAlreadyAdmitted(admission)
      _ <- lateFactStage.persistClosedDayFacts(lateEvents, markerFrame, preparation.observedAt)
      openEvents <- openSilverFacts(preparation, admission, prepared)
      _ <- silverStage.mergeSilver(prepared.copy(incomingSilver = openEvents), preparation.observedAt)
      _ <- ensureAssessmentOutcome(assessment, admission)
    } yield ()

  override def publish(
      preparation: StreamingInputPreparation,
      decision: StreamingDecisionRevision,
      activeTokens: Vector[SubjectToken]
  ): F[StreamingPublicationResult] =
    if (activeTokens.nonEmpty) F.pure(StreamingPublicationResult.ErasurePending)
    else
      for {
        _ <- authorizePublication
        report <- rebuildAndExtract(preparation.observedAt)
        runId <- F.fromEither(streamRunId(decision))
        rangeFingerprint <- F.fromEither(streamRangeFingerprint(preparation, decision))
        reservation <- reportPublisher.reserve(
          runId,
          rangeFingerprint,
          preparation.observedAt
        )
        expiresAt = preparation.observedAt.plusMillis(retention.publishedSnapshotDays.value.toLong * 86400000L)
        result <- reportPublisher
          .publish(reservation, report, expiresAt)
          .as(StreamingPublicationResult.Published)
          .handleErrorWith {
            case AnalyticsError.GuardedErasurePublicationRejected =>
              F.pure(StreamingPublicationResult.ErasurePending)
            case error => F.raiseError(error)
          }
      } yield result

  private def analyze(
      preparation: StreamingInputPreparation,
      markerFrame: DataFrame,
      isRecoveryAttempt: Boolean
  ): F[AdmissionFacts] =
    for {
      valid <- execution(OperationalEventTransforms.validEvents(parsedEvents))
      tokenized <- execution(AnalyticsSubjectPrivacy.withSubjectToken(valid, pseudonymizer))
      safe <- execution.either(AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(tokenized, markerFrame))
      incomingSilver <- execution.either(OperationalEventTransforms.silver(safe, pseudonymizer, markerFrame))
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
          isRecoveryAttempt
        )
      )
      result <- execution {
        val malformed = parsedEvents.filter(!OperationalEventTransforms.isValidEvent).count()
        val conflicts = facts.safeValidEvents
          .select(Columns.EventId)
          .join(facts.conflicts, Seq(Columns.EventId), "inner")
          .distinct()
          .count()
        val future = facts.classifications
          .filter(col(s"$AdmissionColumn.$AdmissionStatusField") === lit("FUTURE"))
          .join(facts.conflicts, Seq(Columns.EventId), "left_anti")
          .count()
        val deleted = tokenized.count() - safe.count()
        facts.copy(
          malformedCount = malformed,
          conflictingEventCount = conflicts,
          futureCount = future,
          deletionSuppressedCount = deleted
        )
      }
    } yield result

  private def buildAdmissionFacts(
      preparation: StreamingInputPreparation,
      safe: DataFrame,
      tokenized: DataFrame,
      incomingSilver: DataFrame,
      storedSilver: DataFrame,
      storedLate: DataFrame,
      storedBronze: DataFrame,
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
      val alreadyAdmitted = selectedValid
        .select(Columns.EventId, Columns.EventFingerprint)
        .join(existingFingerprints, Seq(Columns.EventId, Columns.EventFingerprint), "inner")
        .select(Columns.EventId)
        .distinct()
      val previouslyWrittenCoordinates = storedBronze
        .select(Columns.Topic, Columns.Partition, Columns.Offset, Columns.RawValue)
        .withColumn(Columns.EventFingerprint, sha2(col(Columns.RawValue), 256))
        .select(Columns.Topic, Columns.Partition, Columns.Offset, Columns.EventFingerprint)
        .distinct()
      val sourceCoordinates = selectedValid
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
      val newByIdentity = selectedValid
        .select(Columns.EventId)
        .distinct()
        .join(alreadyAdmitted, Seq(Columns.EventId), "left_anti")
        .join(idsWithPriorBronzeCoordinate, Seq(Columns.EventId), "left_anti")
      val newBySameBatchCoordinate =
        if (isRecoveryAttempt)
          selectedValid
            .select(Columns.EventId)
            .distinct()
            .join(sameBatchCoordinates, Seq(Columns.EventId), "inner")
            .join(alreadyAdmitted, Seq(Columns.EventId), "left_anti")
        else selectedValid.limit(0).select(Columns.EventId)
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
          tokenized.count() - safe.count(),
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
      prepared: AnalyticsPreparedEvents,
      isRecoveryAttempt: Boolean
  ): F[AdmissionFacts] =
    for {
      valid <- execution(OperationalEventTransforms.validEvents(parsedEvents))
      tokenized <- execution(AnalyticsSubjectPrivacy.withSubjectToken(valid, pseudonymizer))
      safe <- execution.either(AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(tokenized, markerFrame))
      incomingSilver <- execution.either(OperationalEventTransforms.silver(safe, pseudonymizer, markerFrame))
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
          isRecoveryAttempt
        )
      )
      _ <- execution.either(
        Either.cond(
          facts.conflicts.join(prepared.conflicts, Seq(Columns.EventId), "left_anti").limit(1).count() == 0L,
          (),
          AnalyticsError.LakehouseFailure(
            new IllegalStateException("streaming admission conflict set changed during ingestion")
          )
        )
      )
    } yield facts.copy(conflicts = prepared.conflicts)

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
      future
    }.flatMap { future =>
      deltaWriter.merge(
        future,
        paths.quarantine,
        "target.quarantineId = source.quarantineId"
      )
    }

  private def lateFactsNotAlreadyAdmitted(facts: AdmissionFacts): F[DataFrame] = execution {
    val eligiblePairs = facts.incomingSilver
      .select(Columns.EventId, Columns.EventFingerprint)
      .join(facts.conflicts, Seq(Columns.EventId), "left_anti")
    val existingFactIds = facts.existingFacts.select(Columns.EventId).distinct()
    facts.classifications
      .filter(col(s"$AdmissionColumn.$AdmissionStatusField") === lit("CLOSED"))
      .join(eligiblePairs, Seq(Columns.EventId, Columns.EventFingerprint), "inner")
      .join(facts.conflicts, Seq(Columns.EventId), "left_anti")
      .join(existingFactIds, Seq(Columns.EventId), "left_anti")
      .drop(AdmissionColumn, EffectiveTimeColumn, Columns.EventFingerprint)
      .dropDuplicates(Columns.Topic, Columns.Partition, Columns.Offset)
  }

  private def openSilverFacts(
      preparation: StreamingInputPreparation,
      facts: AdmissionFacts,
      prepared: AnalyticsPreparedEvents
  ): F[DataFrame] = execution {
    val openPairs = withEventTimeAdmission(prepared.incomingSilver, preparation)
      .filter(col(s"$AdmissionColumn.$AdmissionStatusField") === lit("OPEN"))
      .select(Columns.EventId, Columns.EventFingerprint)
    prepared.incomingSilver
      .join(openPairs, Seq(Columns.EventId, Columns.EventFingerprint), "inner")
      .join(prepared.conflicts, Seq(Columns.EventId), "left_anti")
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
    execution(DeltaTable.isDeltaTable(spark, paths.silver)).flatMap {
      case true =>
        execution(spark.read.format("delta").load(paths.silver))
          .flatMap(AnalyticsGoldStage.rebuild(paths, _, execution)) *>
          AnalyticsGoldStage.extract(spark, paths, asOf, execution)
      case false =>
        AnalyticsGoldStage.clear(spark, paths, execution) *>
          AnalyticsGoldStage.extract(spark, paths, asOf, execution)
    }

  private def streamRunId(decision: StreamingDecisionRevision): Either[AnalyticsError, RunId] = {
    val digest = identityDigest(decision)
    RunId.from(s"stream-$digest").leftMap(AnalyticsError.InvalidConfiguration.apply)
  }

  private def streamRangeFingerprint(
      preparation: StreamingInputPreparation,
      decision: StreamingDecisionRevision
  ): Either[AnalyticsError, RangeFingerprint] = {
    val canonical = Vector(
      preparation.identity.lineage.value,
      preparation.identity.batchId.value.toString,
      preparation.inputFingerprint.value,
      decision.revision.toString,
      decision.deletionMarkerFingerprint,
      decision.candidateWatermark.fold("")(_.toString)
    ).mkString("\n")
    RangeFingerprint
      .from(AnalyticsDigest.sha256Hex(canonical.getBytes(StandardCharsets.UTF_8)))
      .leftMap(AnalyticsError.InvalidConfiguration.apply)
  }

  private def identityDigest(decision: StreamingDecisionRevision): String =
    AnalyticsDigest.sha256Hex(
      s"${decision.identity.lineage.value}\n${decision.identity.batchId.value}\n${decision.revision}"
        .getBytes(StandardCharsets.UTF_8)
    )
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
