package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import com.example.hiring.analytics.service.streaming.*

import cats.syntax.all.*
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.*

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal
import com.example.hiring.analytics.service.batch.AnalyticsReportReservation

/** Delta-backed journal for immutable input evidence, retry decisions, and publication progress. */
private[analytics] final class DeltaStreamingBatchJournal[F[_]](
    spark: SparkSession,
    paths: AnalyticsLakehousePaths,
    execution: SparkExecution[F]
) extends StreamingBatchJournal[F] {
  import DeltaStreamingBatchJournal.*

  override def load(identity: StreamingBatchIdentity): F[Option[StreamingJournalState]] =
    execution.either {
      withProgress(Option.empty[StreamingJournalState]) {
        readProgress(identity) match {
          case None      => Right(None)
          case Some(row) =>
            for {
              progress <- decodeProgress(row)
              decision <- latestDecision(identity)
              candidate <- readOptionalInstant(row, CandidateWatermarkColumn).leftMap(_ => malformedProgress)
              _ <- Either.cond(
                progress.terminalOutcome != Some(StreamingTerminalOutcome.Published) ||
                  decision.exists(_.candidateWatermark == candidate),
                (),
                malformedProgress
              )
            } yield Some(progress.copy(latestDecision = decision))
        }
      }
    }

  override def latestWatermark(lineage: StreamingLineage): F[Option[Instant]] =
    execution.either(withProgress(Option.empty[Instant])(latestPublishedWatermark(lineage)))

  override def hasLineageState(lineage: StreamingLineage): F[Boolean] =
    execution.either {
      withProgress(false) {
        Right(
          DeltaTables
            .read(spark, paths.streamingProgress)
            .filter(col(LineageColumn) === lit(lineage.value))
            .limit(1)
            .take(1)
            .nonEmpty
        )
      }
    }

  override def reconciliationStates(
      lineage: StreamingLineage,
      retainedBatchIds: Set[StreamingBatchId]
  ): F[Vector[StreamingJournalState]] =
    execution.either {
      withProgress(Vector.empty[StreamingJournalState]) {
        val retained =
          if (retainedBatchIds.isEmpty) lit(false) else col(BatchIdColumn).isin(retainedBatchIds.toSeq.map(_.value)*)
        val unfinished = col(OutcomeColumn).isin(PreparedName, IngestionCommittedName)
        val rows = DeltaTables
          .read(spark, paths.streamingProgress)
          .filter(col(LineageColumn) === lit(lineage.value) && (retained || unfinished))
          .limit(retainedBatchIds.size + 2)
          .collect()
          .toVector
        Either.cond(rows.size <= retainedBatchIds.size + 1, rows, malformedProgress).flatMap(_.traverse(decodeProgress))
      }
    }

  /** Candidate-filtered across every lineage and every retained decision revision. */
  def referencedPublicationRunIds(candidates: Set[RunId]): F[Set[RunId]] = execution.either {
    if (candidates.isEmpty) Right(Set.empty)
    else
      withDecisions(Set.empty[RunId]) {
        val stored = DeltaTables.read(spark, paths.streamingDecisions)
        if (
          stored
            .filter(
              col(PublicationRunIdColumn).isNull || col(PublicationRunIdColumn).rlike("^[\\p{javaWhitespace}]*$")
            )
            .limit(1)
            .count() != 0
        )
          Left(malformedProgress)
        else
          stored
            .filter(col(PublicationRunIdColumn).isin(candidates.toVector.map(_.value)*))
            .select(col(PublicationRunIdColumn))
            .distinct()
            .limit(candidates.size)
            .collect()
            .toVector
            .traverse(row => RunId.from(row.getString(0)).leftMap(_ => malformedProgress))
            .map(_.toSet)
      }
  }

  /** Call under the lakehouse mutex, after reading Spark's retained offset/commit IDs. Never edits Spark logs. */
  def prune(
      lineage: StreamingLineage,
      retainedBatchIds: Set[StreamingBatchId],
      observedAt: Instant,
      retention: FiniteDuration
  ): F[Unit] = execution.either {
    if (retention.toMillis <= 0L)
      Left(AnalyticsError.InvalidConfiguration("streaming progress retention must be positive"))
    else
      withProgress(()) {
        withDecisions(())(Right(())).flatMap { _ =>
          val progress = DeltaTables
            .read(spark, paths.streamingProgress)
            .filter(col(LineageColumn) === lit(lineage.value))
          val malformed = progress
            .filter(
              col(OutcomeColumn).isNull || !col(OutcomeColumn).isin(
                PreparedName,
                IngestionCommittedName,
                QualityBlockedName,
                ErasurePendingName,
                PublishedName
              )
            )
            .limit(1)
            .count() > 0L
          if (malformed) Left(malformedProgress)
          else {
            val unfinished = col(OutcomeColumn).isin(PreparedName, IngestionCommittedName)
            val terminal = progress.filter(!unfinished)
            val latestTerminal = terminal.orderBy(col(BatchIdColumn).desc).limit(1).select(col(BatchIdColumn))
            val latestWatermark = progress
              .filter(col(OutcomeColumn) === lit(PublishedName) && col(CandidateWatermarkColumn).isNotNull)
              .orderBy(col(BatchIdColumn).desc)
              .limit(1)
              .select(col(BatchIdColumn))
            val sparkRetained =
              if (retainedBatchIds.isEmpty) lit(false)
              else col(BatchIdColumn).isin(retainedBatchIds.toVector.map(_.value)*)
            val recent = col(CompletedAtColumn).isNull || col(CompletedAtColumn) > lit(
              Timestamp.from(observedAt.minusMillis(retention.toMillis))
            )
            val protectedIds = progress
              .filter(unfinished || sparkRetained || recent)
              .select(col(BatchIdColumn))
              .union(latestTerminal)
              .union(latestWatermark)
              .distinct()
            val obsolete = progress
              .join(protectedIds, Seq(BatchIdColumn), "left_anti")
              .select(col(LineageColumn), col(BatchIdColumn))
            // Materialize selection before mutating the source table; a crash can only leave extra decision records.
            val cached = obsolete.persist()
            try {
              if (cached.limit(1).count() > 0L) {
                val _ = DeltaTables
                  .mergeOn(spark, paths.streamingProgress, cached, IdentityKeys)
                  .whenMatched()
                  .delete()
                  .execute()
              }
              withDecisions(()) {
                val remaining = DeltaTables
                  .read(spark, paths.streamingProgress)
                  .select(col(LineageColumn), col(BatchIdColumn))
                val orphaned = DeltaTables
                  .read(spark, paths.streamingDecisions)
                  .filter(col(LineageColumn) === lit(lineage.value))
                  .select(col(LineageColumn), col(BatchIdColumn))
                  .distinct()
                  .join(remaining, Seq(LineageColumn, BatchIdColumn), "left_anti")
                if (orphaned.limit(1).count() > 0L) {
                  val _ = DeltaTables
                    .mergeOn(spark, paths.streamingDecisions, orphaned, IdentityKeys)
                    .whenMatched()
                    .delete()
                    .execute()
                }
                Right(())
              }
            } finally { cached.unpersist(); () }
          }
        }
      }
  }

  private def latestPublishedWatermark(lineage: StreamingLineage): Either[AnalyticsError, Option[Instant]] = {
    val row = DeltaTables
      .read(spark, paths.streamingProgress)
      .filter(
        col(LineageColumn) === lit(lineage.value) &&
          col(OutcomeColumn) === lit(PublishedName) &&
          col(CandidateWatermarkColumn).isNotNull
      )
      .orderBy(col(BatchIdColumn).desc)
      .limit(1)
      .collect()
      .headOption
    row.traverse(readOptionalInstant(_, CandidateWatermarkColumn).leftMap(_ => malformedProgress)).map(_.flatten)
  }

  override def prepare(preparation: StreamingInputPreparation): F[Unit] =
    execution.either {
      ensureProgressTable()
      readProgress(preparation.identity) match {
        case Some(row) => verifyPreparation(row, preparation)
        case None      =>
          val frame = spark.createDataFrame(Vector(preparationRow(preparation)).asJava, ProgressSchema)
          DeltaTables
            .mergeOn(spark, paths.streamingProgress, frame, IdentityKeys)
            .whenNotMatched()
            .insertAll()
            .execute()
          readProgress(preparation.identity).toRight(preparationConflict).flatMap(verifyPreparation(_, preparation))
      }
    }

  override def markIngestionCommitted(identity: StreamingBatchIdentity): F[Unit] =
    execution.either {
      ensureProgressTable()
      val current = readProgress(identity).toRight(preparationConflict)
      current.flatMap { row =>
        val status = row.getAs[String](OutcomeColumn)
        if (status == IngestionCommittedName) Right(())
        else if (status != PreparedName) Left(progressConflict)
        else {
          updateProgress(identity, IngestionCommittedName, None, None)
          Either.cond(
            readProgress(identity).exists(_.getAs[String](OutcomeColumn) == IngestionCommittedName),
            (),
            progressConflict
          )
        }
      }
    }

  override def appendDecision(decision: StreamingDecisionRevision): F[Unit] =
    execution.either {
      ensureProgressTable()
      ensureDecisionTable()
      val progress = readProgress(decision.identity).toRight(preparationConflict)
      progress.flatMap { row =>
        val status = row.getAs[String](OutcomeColumn)
        if (status != PreparedName && status != IngestionCommittedName) Left(progressConflict)
        else if (!validDecision(decision, row)) Left(malformedProgress)
        else {
          readDecision(decision.identity, decision.revision).flatMap {
            case Some(existing) if existing == decision => Right(())
            case Some(_)                                => Left(progressConflict)
            case None                                   =>
              latestDecision(decision.identity).flatMap { latest =>
                val expectedRevision = latest match {
                  case None                                          => Some(0L)
                  case Some(value) if value.revision < Long.MaxValue => Some(value.revision + 1L)
                  case _                                             => None
                }
                if (!expectedRevision.contains(decision.revision)) Left(progressConflict)
                else {
                  val frame = spark.createDataFrame(Vector(decisionRow(decision)).asJava, DecisionSchema)
                  DeltaTables
                    .mergeOn(spark, paths.streamingDecisions, frame, IdentityKeys :+ RevisionColumn)
                    .whenNotMatched()
                    .insertAll()
                    .execute()
                  readDecision(decision.identity, decision.revision).flatMap { persisted =>
                    Either.cond(persisted.contains(decision), (), progressConflict)
                  }
                }
              }
          }
        }
      }
    }

  override def complete(
      identity: StreamingBatchIdentity,
      outcome: StreamingTerminalOutcome,
      completedAt: Instant
  ): F[Unit] =
    execution.either {
      ensureProgressTable()
      if (outcome == StreamingTerminalOutcome.Published) Left(progressConflict)
      else
        readProgress(identity) match {
          case None                                                                    => Left(preparationConflict)
          case Some(row) if row.getAs[String](OutcomeColumn) == outcome.toString       => Right(())
          case Some(row) if row.getAs[String](OutcomeColumn) == IngestionCommittedName =>
            updateProgress(identity, outcome.toString, None, Some(Timestamp.from(nowMicros(completedAt))))
            Either.cond(
              readProgress(identity).exists(_.getAs[String](OutcomeColumn) == outcome.toString),
              (),
              progressConflict
            )
          case Some(_) => Left(progressConflict)
        }
    }

  override def commitPublished(decision: StreamingDecisionRevision, completedAt: Instant): F[Unit] =
    execution.either {
      ensureProgressTable()
      ensureDecisionTable()
      for {
        row <- readProgress(decision.identity).toRight(preparationConflict)
        _ <- Either.cond(validDecision(decision, row), (), malformedProgress)
        persistedDecision <- readDecision(decision.identity, decision.revision).flatMap(_.toRight(progressConflict))
        _ <- Either.cond(persistedDecision == decision, (), progressConflict)
        prior <- latestPublishedWatermark(decision.identity.lineage)
        _ <- Either.cond(
          decision.candidateWatermark.forall(candidate => prior.forall(watermark => !candidate.isBefore(watermark))),
          (),
          progressConflict
        )
        result <- row.getAs[String](OutcomeColumn) match {
          case PublishedName =>
            Either.cond(
              readOptionalInstant(row, CandidateWatermarkColumn).contains(decision.candidateWatermark),
              (),
              progressConflict
            )
          case IngestionCommittedName =>
            updateProgress(
              decision.identity,
              PublishedName,
              decision.candidateWatermark.map(value => Timestamp.from(nowMicros(value))),
              Some(Timestamp.from(nowMicros(completedAt)))
            )
            Either.cond(
              readProgress(decision.identity).exists { persisted =>
                persisted.getAs[String](OutcomeColumn) == PublishedName &&
                readOptionalInstant(persisted, CandidateWatermarkColumn).contains(decision.candidateWatermark)
              },
              (),
              progressConflict
            )
          case _ => Left(progressConflict)
        }
      } yield result
    }

  private def latestDecision(
      identity: StreamingBatchIdentity
  ): Either[AnalyticsError, Option[StreamingDecisionRevision]] =
    withDecisions(Option.empty[StreamingDecisionRevision]) {
      DeltaTables
        .read(spark, paths.streamingDecisions)
        .filter(identityFilter(identity))
        .orderBy(col(RevisionColumn).desc)
        .limit(1)
        .collect()
        .headOption
        .traverse(decodeDecision)
    }

  private def readProgress(identity: StreamingBatchIdentity): Option[Row] =
    DeltaTables
      .read(spark, paths.streamingProgress)
      .filter(identityFilter(identity))
      .limit(1)
      .collect()
      .headOption

  private def readDecision(
      identity: StreamingBatchIdentity,
      revision: Long
  ): Either[AnalyticsError, Option[StreamingDecisionRevision]] =
    withDecisions(Option.empty[StreamingDecisionRevision]) {
      DeltaTables
        .read(spark, paths.streamingDecisions)
        .filter(identityFilter(identity) && col(RevisionColumn) === lit(revision))
        .limit(1)
        .collect()
        .headOption
        .traverse(decodeDecision)
    }

  private def decodeProgress(row: Row): Either[AnalyticsError, StreamingJournalState] =
    Either
      .catchNonFatal(decodeProgressFields(row))
      .leftMap(error =>
        AnalyticsError.InvalidConfiguration(
          s"streaming batch journal stored field conversion failed: ${error.getClass.getSimpleName}"
        )
      )
      .flatMap(identity)

  private def decodeProgressFields(row: Row): Either[AnalyticsError, StreamingJournalState] = {
    val decoded = for {
      lineage <- StreamingLineage.from(row.getAs[String](LineageColumn)).leftMap(_ => malformedProgress)
      batchValue <- readRequiredLong(row, BatchIdColumn).toRight(malformedProgress)
      batchId <- StreamingBatchId.from(batchValue).leftMap(_ => malformedProgress)
      observedAt <- readRequiredInstant(row, ObservedAtColumn).toRight(malformedProgress)
      priorWatermark <- readOptionalInstant(row, PriorWatermarkColumn).leftMap(_ => malformedProgress)
      inputFingerprint <- RangeFingerprint
        .from(row.getAs[String](InputFingerprintColumn))
        .leftMap(_ => malformedProgress)
      sourceEndOffsets <- decodeRows(row, SourceEndOffsetsColumn)(decodeEndOffset)
      offsets <- decodeRows(row, DeliveredOffsetsColumn)(decodeOffset)
      status <- Option(row.getAs[String](OutcomeColumn)).toRight(malformedProgress)
      state <- status match {
        case PreparedName           => Right((false, None))
        case IngestionCommittedName => Right((true, None))
        case QualityBlockedName     => Right((true, Some(StreamingTerminalOutcome.QualityBlocked)))
        case ErasurePendingName     => Right((true, Some(StreamingTerminalOutcome.ErasurePending)))
        case PublishedName          => Right((true, Some(StreamingTerminalOutcome.Published)))
        case _                      => Left(malformedProgress)
      }
      completedAt <- readOptionalInstant(row, CompletedAtColumn).leftMap(_ => malformedProgress)
      candidate <- readOptionalInstant(row, CandidateWatermarkColumn).leftMap(_ => malformedProgress)
      _ <- status match {
        case PreparedName | IngestionCommittedName =>
          Either.cond(completedAt.isEmpty && candidate.isEmpty, (), malformedProgress)
        case QualityBlockedName | ErasurePendingName =>
          Either.cond(completedAt.nonEmpty && candidate.isEmpty, (), malformedProgress)
        case PublishedName => Either.cond(completedAt.nonEmpty, (), malformedProgress)
        case _             => Left(malformedProgress)
      }
    } yield StreamingJournalState(
      StreamingInputPreparation(
        StreamingBatchIdentity(lineage, batchId),
        observedAt,
        priorWatermark,
        inputFingerprint,
        sourceEndOffsets,
        offsets
      ),
      state._1,
      None,
      state._2
    )
    decoded
  }

  private def decodeRows[A](row: Row, column: String)(
      decode: Row => Either[AnalyticsError, A]
  ): Either[AnalyticsError, Vector[A]] =
    Option(row.getAs[scala.collection.Seq[Row]](column))
      .toRight(malformedProgress)
      .flatMap(_.toVector.traverse { element =>
        Option(element).toRight(malformedProgress).flatMap(decode)
      })

  private def decodeOffset(row: Row): Either[AnalyticsError, StreamingPartitionSummary] =
    for {
      topic <- Option(row.getAs[String](Columns.Topic)).toRight(malformedProgress)
      partition <- readRequiredInt(row, Columns.Partition).toRight(malformedProgress)
      minimum <- readRequiredLong(row, "minimumDeliveredOffset").toRight(malformedProgress)
      maximum <- readRequiredLong(row, "maximumDeliveredOffset").toRight(malformedProgress)
      count <- readRequiredLong(row, "deliveredRecordCount").toRight(malformedProgress)
      summary <- StreamingPartitionSummary
        .from(topic, partition, minimum, maximum, count)
        .toEither
        .leftMap(_ => malformedProgress)
    } yield summary

  private def decodeEndOffset(row: Row): Either[AnalyticsError, StreamingPartitionEndOffset] =
    for {
      topic <- Option(row.getAs[String](Columns.Topic)).toRight(malformedProgress)
      partition <- readRequiredInt(row, Columns.Partition).toRight(malformedProgress)
      end <- readRequiredLong(row, "endOffset").toRight(malformedProgress)
      offset <- StreamingPartitionEndOffset.from(topic, partition, end).toEither.leftMap(_ => malformedProgress)
    } yield offset

  private def decodeDecision(row: Row): Either[AnalyticsError, StreamingDecisionRevision] =
    Either
      .catchNonFatal(decodeDecisionFields(row))
      .leftMap(cause => malformedProgress.copy(underlying = Some(cause)))
      .flatMap(identity)

  private def decodeDecisionFields(row: Row): Either[AnalyticsError, StreamingDecisionRevision] =
    for {
      lineage <- StreamingLineage.from(row.getAs[String](LineageColumn)).leftMap(_ => malformedProgress)
      batchValue <- readRequiredLong(row, BatchIdColumn).toRight(malformedProgress)
      batchId <- StreamingBatchId.from(batchValue).leftMap(_ => malformedProgress)
      revision <- readRequiredLong(row, RevisionColumn).filter(_ >= 0L).toRight(malformedProgress)
      markerFingerprint <- Option(row.getAs[String](MarkerFingerprintColumn))
        .filter(_.matches("[0-9a-f]{64}"))
        .toRight(malformedProgress)
      candidate <- readOptionalInstant(row, CandidateWatermarkColumn).leftMap(_ => malformedProgress)
      runId <- RunId.from(row.getAs[String](PublicationRunIdColumn)).leftMap(_ => malformedProgress)
      range <- RangeFingerprint
        .from(row.getAs[String](PublicationRangeFingerprintColumn))
        .leftMap(_ => malformedProgress)
      generation <- readRequiredLong(row, PublicationGenerationColumn).filter(_ >= 0L).toRight(malformedProgress)
      publicationRevision <- readRequiredLong(row, PublicationRevisionColumn).filter(_ >= 0L).toRight(malformedProgress)
    } yield StreamingDecisionRevision(
      StreamingBatchIdentity(lineage, batchId),
      revision,
      markerFingerprint,
      candidate,
      AnalyticsReportReservation(runId, range, generation, publicationRevision)
    )

  private def validDecision(decision: StreamingDecisionRevision, progress: Row): Boolean =
    try {
      decision.identity.lineage.value == progress.getAs[String](LineageColumn) &&
      decision.identity.batchId.value == progress.getAs[Long](BatchIdColumn) &&
      decision.revision >= 0L && decision.deletionMarkerFingerprint.matches("[0-9a-f]{64}") &&
      decision.publicationReservation.generation >= 0L && decision.publicationReservation.revision >= 0L &&
      readOptionalInstant(progress, PriorWatermarkColumn).exists(
        _.forall(prior => decision.candidateWatermark.forall(candidate => !candidate.isBefore(prior)))
      )
    } catch { case NonFatal(_) => false }

  private def verifyPreparation(row: Row, preparation: StreamingInputPreparation): Either[AnalyticsError, Unit] =
    decodeProgress(row).flatMap { stored =>
      val expected = preparation.copy(
        observedAt = nowMicros(preparation.observedAt),
        priorWatermark = preparation.priorWatermark.map(nowMicros),
        sourceEndOffsets = sortedEndOffsets(preparation),
        deliveredOffsets = sortedOffsets(preparation)
      )
      Either.cond(stored.preparation == expected, (), preparationConflict)
    }

  private def sortedOffsets(preparation: StreamingInputPreparation): Vector[StreamingPartitionSummary] =
    preparation.deliveredOffsets.sortBy(summary =>
      (AnalyticsTopic.unwrap(summary.topic), AnalyticsPartition.unwrap(summary.partition))
    )

  private def sortedEndOffsets(preparation: StreamingInputPreparation): Vector[StreamingPartitionEndOffset] =
    preparation.sourceEndOffsets.sortBy(offset =>
      (AnalyticsTopic.unwrap(offset.topic), AnalyticsPartition.unwrap(offset.partition))
    )

  private def preparationRow(preparation: StreamingInputPreparation): Row =
    Row(
      preparation.identity.lineage.value,
      preparation.identity.batchId.value,
      Timestamp.from(nowMicros(preparation.observedAt)),
      preparation.priorWatermark.map(value => Timestamp.from(nowMicros(value))).orNull,
      preparation.inputFingerprint.value,
      sortedEndOffsets(preparation).map { offset =>
        Row(
          AnalyticsTopic.unwrap(offset.topic),
          AnalyticsPartition.unwrap(offset.partition),
          AnalyticsOffset.unwrap(offset.offset)
        )
      },
      sortedOffsets(preparation).map { summary =>
        Row(
          AnalyticsTopic.unwrap(summary.topic),
          AnalyticsPartition.unwrap(summary.partition),
          AnalyticsOffset.unwrap(summary.minimumDeliveredOffset),
          AnalyticsOffset.unwrap(summary.maximumDeliveredOffset),
          summary.deliveredRecordCount
        )
      },
      PreparedName,
      null,
      null
    )

  private def decisionRow(decision: StreamingDecisionRevision): Row =
    Row(
      decision.identity.lineage.value,
      decision.identity.batchId.value,
      decision.revision,
      decision.deletionMarkerFingerprint,
      decision.candidateWatermark.map(value => Timestamp.from(nowMicros(value))).orNull,
      decision.publicationReservation.runId.value,
      decision.publicationReservation.rangeFingerprint.value,
      decision.publicationReservation.generation,
      decision.publicationReservation.revision
    )

  private def updateProgress(
      identity: StreamingBatchIdentity,
      status: String,
      candidateWatermark: Option[Timestamp],
      completedAt: Option[Timestamp]
  ): Unit = {
    DeltaTables
      .forPath(spark, paths.streamingProgress)
      .update(
        identityFilter(identity) && col(OutcomeColumn).isin(PreparedName, IngestionCommittedName),
        Map(
          OutcomeColumn -> lit(status),
          CandidateWatermarkColumn -> lit(candidateWatermark.orNull).cast(TimestampType),
          CompletedAtColumn -> lit(completedAt.orNull).cast(TimestampType)
        ).asJava
      )
  }

  private def ensureProgressTable(): Unit =
    AnalyticsTableSchemas.createOrValidate(spark, paths.streamingProgress, ProgressShape)

  private def ensureDecisionTable(): Unit =
    AnalyticsTableSchemas.createOrValidate(spark, paths.streamingDecisions, DecisionShape)

  private def withProgress[A](absent: A)(present: => Either[AnalyticsError, A]): Either[AnalyticsError, A] =
    withTable(paths.streamingProgress, ProgressShape, absent)(present)

  private def withDecisions[A](absent: A)(present: => Either[AnalyticsError, A]): Either[AnalyticsError, A] =
    withTable(paths.streamingDecisions, DecisionShape, absent)(present)

  /** A missing table yields `absent`; an existing path that is not a Delta table with the shape is malformed. */
  private def withTable[A](path: String, shape: AnalyticsTableSchemas.Shape, absent: A)(
      present: => Either[AnalyticsError, A]
  ): Either[AnalyticsError, A] = {
    val tablePath = new org.apache.hadoop.fs.Path(SparkPhysicalLocation.resolve(path))
    val fileSystem = tablePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(tablePath)) Right(absent)
    else if (
      !DeltaTables.exists(spark, path) || !AnalyticsTableSchemas.matches(DeltaTables.read(spark, path).schema, shape)
    )
      Left(malformedProgress)
    else present
  }

  private def readRequiredInstant(row: Row, name: String): Option[Instant] =
    Either
      .catchNonFatal(Option(row.getAs[Timestamp](name)).map(_.toInstant.truncatedTo(ChronoUnit.MICROS)))
      .toOption
      .flatten

  private def readRequiredLong(row: Row, name: String): Option[Long] =
    Option(row.getAs[java.lang.Long](name)).map(_.longValue())

  private def readRequiredInt(row: Row, name: String): Option[Int] =
    Option(row.getAs[java.lang.Integer](name)).map(_.intValue())

  private def readOptionalInstant(row: Row, name: String): Either[Unit, Option[Instant]] =
    Either
      .catchNonFatal(Option(row.getAs[Timestamp](name)).map(_.toInstant.truncatedTo(ChronoUnit.MICROS)))
      .leftMap(_ => ())

  private def identityFilter(identity: StreamingBatchIdentity) =
    col(LineageColumn) === lit(identity.lineage.value) && col(BatchIdColumn) === lit(identity.batchId.value)

  private def nowMicros(value: Instant): Instant = value.truncatedTo(ChronoUnit.MICROS)
}

private object DeltaStreamingBatchJournal {
  private val LineageColumn = "lineage"
  private val BatchIdColumn = "batchId"
  private val ObservedAtColumn = "observedAt"
  private val PriorWatermarkColumn = "priorWatermark"
  private val InputFingerprintColumn = "inputFingerprint"
  private val SourceEndOffsetsColumn = "sourceEndOffsets"
  private val DeliveredOffsetsColumn = "deliveredOffsets"
  private val OutcomeColumn = "outcome"
  private val CandidateWatermarkColumn = "candidateWatermark"
  private val CompletedAtColumn = "completedAt"
  private val RevisionColumn = "revision"
  private val MarkerFingerprintColumn = "deletionMarkerFingerprint"
  private val PublicationRunIdColumn = "publicationRunId"
  private val PublicationRangeFingerprintColumn = "publicationRangeFingerprint"
  private val PublicationGenerationColumn = "publicationGeneration"
  private val PublicationRevisionColumn = "publicationRevision"
  private val PreparedName = "Prepared"
  private val IngestionCommittedName = "IngestionCommitted"
  private val QualityBlockedName = "QualityBlocked"
  private val ErasurePendingName = "ErasurePending"
  private val PublishedName = "Published"

  // Delta cannot enforce NOT NULL inside arrays. Required values are checked by the typed journal decoder.
  private val OffsetType = StructType(
    Vector(
      StructField(Columns.Topic, StringType, nullable = true),
      StructField(Columns.Partition, IntegerType, nullable = true),
      StructField("minimumDeliveredOffset", LongType, nullable = true),
      StructField("maximumDeliveredOffset", LongType, nullable = true),
      StructField("deliveredRecordCount", LongType, nullable = true)
    )
  )
  private val EndOffsetType = StructType(
    Vector(
      StructField(Columns.Topic, StringType, nullable = true),
      StructField(Columns.Partition, IntegerType, nullable = true),
      StructField("endOffset", LongType, nullable = true)
    )
  )
  private val ProgressShape: AnalyticsTableSchemas.Shape = Vector(
    LineageColumn -> StringType,
    BatchIdColumn -> LongType,
    ObservedAtColumn -> TimestampType,
    PriorWatermarkColumn -> TimestampType,
    InputFingerprintColumn -> StringType,
    SourceEndOffsetsColumn -> ArrayType(EndOffsetType, containsNull = true),
    DeliveredOffsetsColumn -> ArrayType(OffsetType, containsNull = true),
    OutcomeColumn -> StringType,
    CandidateWatermarkColumn -> TimestampType,
    CompletedAtColumn -> TimestampType
  )
  private val DecisionShape: AnalyticsTableSchemas.Shape = Vector(
    LineageColumn -> StringType,
    BatchIdColumn -> LongType,
    RevisionColumn -> LongType,
    MarkerFingerprintColumn -> StringType,
    CandidateWatermarkColumn -> TimestampType,
    PublicationRunIdColumn -> StringType,
    PublicationRangeFingerprintColumn -> StringType,
    PublicationGenerationColumn -> LongType,
    PublicationRevisionColumn -> LongType
  )
  private val ProgressSchema = AnalyticsTableSchemas.struct(ProgressShape)
  private val DecisionSchema = AnalyticsTableSchemas.struct(DecisionShape)
  private val IdentityKeys = Seq(LineageColumn, BatchIdColumn)

  private val malformedProgress = AnalyticsError.InvalidConfiguration("streaming batch journal record is malformed")
  private val preparationConflict =
    AnalyticsError.InvalidInput.one("streaming batch journal conflicts with persisted immutable input evidence")
  private val progressConflict =
    AnalyticsError.InvalidInput.one("streaming batch journal operation conflicts with persisted progress")
}
