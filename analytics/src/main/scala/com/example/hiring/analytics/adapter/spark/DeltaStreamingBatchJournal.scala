package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import com.example.hiring.analytics.service.streaming.*

import cats.data.NonEmptyChain
import cats.effect.Async
import cats.syntax.all.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.*

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Delta-backed journal for immutable input evidence, retry decisions, and publication progress. */
private[analytics] final class DeltaStreamingBatchJournal[F[_]: Async](
    spark: SparkSession,
    paths: AnalyticsLakehousePaths,
    execution: SparkExecution[F]
) extends StreamingBatchJournal[F] {
  import DeltaStreamingBatchJournal.*

  override def load(identity: StreamingBatchIdentity): F[Option[StreamingJournalState]] =
    execution.either {
      if (!deltaTableExists(paths.streamingProgress)) Right(None)
      else {
        ensureProgressSchema()
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
    execution.either {
      if (!deltaTableExists(paths.streamingProgress)) Right(None)
      else {
        ensureProgressSchema()
        latestPublishedWatermark(lineage)
      }
    }

  override def hasLineageState(lineage: StreamingLineage): F[Boolean] =
    execution.either {
      if (!deltaTableExists(paths.streamingProgress)) Right(false)
      else {
        ensureProgressSchema()
        val found = spark.read
          .format("delta")
          .load(paths.streamingProgress)
          .filter(col(LineageColumn) === lit(lineage.value))
          .limit(1)
          .take(1)
          .nonEmpty
        Right(found)
      }
    }

  private def latestPublishedWatermark(lineage: StreamingLineage): Either[AnalyticsError, Option[Instant]] = {
    val row = spark.read
      .format("delta")
      .load(paths.streamingProgress)
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
        case Some(row) if immutablePreparationMatches(row, preparation) => Right(())
        case Some(_)                                                    => Left(preparationConflict)
        case None                                                       =>
          val frame = spark.createDataFrame(Vector(preparationRow(preparation)).asJava, ProgressSchema)
          DeltaTable
            .forPath(spark, paths.streamingProgress)
            .as("target")
            .merge(frame.as("source"), identityCondition)
            .whenNotMatched()
            .insertAll()
            .execute()
          Either.cond(
            readProgress(preparation.identity).exists(immutablePreparationMatches(_, preparation)),
            (),
            preparationConflict
          )
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
                  DeltaTable
                    .forPath(spark, paths.streamingDecisions)
                    .as("target")
                    .merge(frame.as("source"), decisionCondition)
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
    if (!deltaTableExists(paths.streamingDecisions)) Right(None)
    else {
      ensureDecisionSchema()
      val rows = spark.read
        .format("delta")
        .load(paths.streamingDecisions)
        .filter(identityFilter(identity))
        .orderBy(col(RevisionColumn).desc)
        .limit(1)
        .collect()
      rows.headOption.traverse(decodeDecision)
    }

  private def readProgress(identity: StreamingBatchIdentity): Option[Row] =
    spark.read
      .format("delta")
      .load(paths.streamingProgress)
      .filter(identityFilter(identity))
      .limit(1)
      .collect()
      .headOption

  private def readDecision(
      identity: StreamingBatchIdentity,
      revision: Long
  ): Either[AnalyticsError, Option[StreamingDecisionRevision]] =
    if (!deltaTableExists(paths.streamingDecisions)) Right(None)
    else {
      ensureDecisionSchema()
      spark.read
        .format("delta")
        .load(paths.streamingDecisions)
        .filter(identityFilter(identity) && col(RevisionColumn) === lit(revision))
        .limit(1)
        .collect()
        .headOption
        .traverse(decodeDecision)
    }

  private def decodeProgress(row: Row): Either[AnalyticsError, StreamingJournalState] =
    Either
      .catchNonFatal(decodeProgressFields(row))
      .leftMap(_ => malformedProgress)
      .flatMap(identity)

  private def decodeProgressFields(row: Row): Either[AnalyticsError, StreamingJournalState] = {
    val decoded = for {
      lineage <- StreamingLineage.from(row.getAs[String](LineageColumn)).leftMap(_ => malformedProgress)
      batchId <- StreamingBatchId.from(row.getAs[Long](BatchIdColumn)).leftMap(_ => malformedProgress)
      observedAt <- readRequiredInstant(row, ObservedAtColumn).toRight(malformedProgress)
      priorWatermark <- readOptionalInstant(row, PriorWatermarkColumn).leftMap(_ => malformedProgress)
      inputFingerprint <- RangeFingerprint
        .from(row.getAs[String](InputFingerprintColumn))
        .leftMap(_ => malformedProgress)
      offsets <- row.getAs[Seq[Row]](DeliveredOffsetsColumn).toVector.traverse(decodeOffset)
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
        offsets
      ),
      state._1,
      None,
      state._2
    )
    decoded
  }

  private def decodeOffset(row: Row): Either[AnalyticsError, StreamingPartitionSummary] =
    Either
      .catchNonFatal(
        StreamingPartitionSummary
          .from(
            row.getAs[String]("topic"),
            row.getAs[Int]("partition"),
            row.getAs[Long]("minimumDeliveredOffset"),
            row.getAs[Long]("maximumDeliveredOffset"),
            row.getAs[Long]("deliveredRecordCount")
          )
          .toEither
      )
      .leftMap(_ => malformedProgress)
      .flatMap(_.leftMap(_ => malformedProgress))

  private def decodeDecision(row: Row): Either[AnalyticsError, StreamingDecisionRevision] =
    Either
      .catchNonFatal(decodeDecisionFields(row))
      .leftMap(_ => malformedProgress)
      .flatMap(identity)

  private def decodeDecisionFields(row: Row): Either[AnalyticsError, StreamingDecisionRevision] =
    for {
      lineage <- StreamingLineage.from(row.getAs[String](LineageColumn)).leftMap(_ => malformedProgress)
      batchId <- StreamingBatchId.from(row.getAs[Long](BatchIdColumn)).leftMap(_ => malformedProgress)
      revision <- Either.cond(row.getAs[Long](RevisionColumn) >= 0L, row.getAs[Long](RevisionColumn), malformedProgress)
      markerFingerprint <- Option(row.getAs[String](MarkerFingerprintColumn))
        .filter(_.matches("[0-9a-f]{64}"))
        .toRight(malformedProgress)
      candidate <- readOptionalInstant(row, CandidateWatermarkColumn).leftMap(_ => malformedProgress)
    } yield StreamingDecisionRevision(StreamingBatchIdentity(lineage, batchId), revision, markerFingerprint, candidate)

  private def validDecision(decision: StreamingDecisionRevision, progress: Row): Boolean =
    try {
      decision.identity.lineage.value == progress.getAs[String](LineageColumn) &&
      decision.identity.batchId.value == progress.getAs[Long](BatchIdColumn) &&
      decision.revision >= 0L && decision.deletionMarkerFingerprint.matches("[0-9a-f]{64}") &&
      readOptionalInstant(progress, PriorWatermarkColumn).exists(
        _.forall(prior => decision.candidateWatermark.forall(candidate => !candidate.isBefore(prior)))
      )
    } catch { case NonFatal(_) => false }

  private def decisionMatches(row: Row, decision: StreamingDecisionRevision): Boolean =
    decodeDecision(row).contains(decision)

  private def immutablePreparationMatches(row: Row, preparation: StreamingInputPreparation): Boolean =
    try {
      row.getAs[String](LineageColumn) == preparation.identity.lineage.value &&
      row.getAs[Long](BatchIdColumn) == preparation.identity.batchId.value &&
      readRequiredInstant(row, ObservedAtColumn).contains(nowMicros(preparation.observedAt)) &&
      readOptionalInstant(row, PriorWatermarkColumn).toOption.flatten == preparation.priorWatermark.map(nowMicros) &&
      row.getAs[String](InputFingerprintColumn) == preparation.inputFingerprint.value &&
      row.getAs[Seq[Row]](DeliveredOffsetsColumn).toVector.map(offsetValues) == sortedOffsets(preparation).map(
        summaryValues
      )
    } catch { case NonFatal(_) => false }

  private def offsetValues(row: Row): (String, Int, Long, Long, Long) =
    (
      row.getAs[String]("topic"),
      row.getAs[Int]("partition"),
      row.getAs[Long]("minimumDeliveredOffset"),
      row.getAs[Long]("maximumDeliveredOffset"),
      row.getAs[Long]("deliveredRecordCount")
    )

  private def summaryValues(summary: StreamingPartitionSummary): (String, Int, Long, Long, Long) =
    (
      AnalyticsTopic.unwrap(summary.topic),
      AnalyticsPartition.unwrap(summary.partition),
      AnalyticsOffset.unwrap(summary.minimumDeliveredOffset),
      AnalyticsOffset.unwrap(summary.maximumDeliveredOffset),
      summary.deliveredRecordCount
    )

  private def sortedOffsets(preparation: StreamingInputPreparation): Vector[StreamingPartitionSummary] =
    preparation.deliveredOffsets.sortBy(summary =>
      (AnalyticsTopic.unwrap(summary.topic), AnalyticsPartition.unwrap(summary.partition))
    )

  private def preparationRow(preparation: StreamingInputPreparation): Row =
    Row(
      preparation.identity.lineage.value,
      preparation.identity.batchId.value,
      Timestamp.from(nowMicros(preparation.observedAt)),
      preparation.priorWatermark.map(value => Timestamp.from(nowMicros(value))).orNull,
      preparation.inputFingerprint.value,
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
      decision.candidateWatermark.map(value => Timestamp.from(nowMicros(value))).orNull
    )

  private def updateProgress(
      identity: StreamingBatchIdentity,
      status: String,
      candidateWatermark: Option[Timestamp],
      completedAt: Option[Timestamp]
  ): Unit = {
    val sourceRow = Row(
      identity.lineage.value,
      identity.batchId.value,
      status,
      candidateWatermark.orNull,
      completedAt.orNull
    )
    val sourceSchema = StructType(
      Vector(
        StructField(LineageColumn, StringType, nullable = false),
        StructField(BatchIdColumn, LongType, nullable = false),
        StructField(OutcomeColumn, StringType, nullable = false),
        StructField(CandidateWatermarkColumn, TimestampType, nullable = true),
        StructField(CompletedAtColumn, TimestampType, nullable = true)
      )
    )
    val source = spark.createDataFrame(Vector(sourceRow).asJava, sourceSchema)
    DeltaTable
      .forPath(spark, paths.streamingProgress)
      .as("target")
      .merge(
        source.as("source"),
        s"target.$LineageColumn = source.$LineageColumn AND target.$BatchIdColumn = source.$BatchIdColumn AND " +
          s"target.$OutcomeColumn IN ('$PreparedName', '$IngestionCommittedName')"
      )
      .whenMatched()
      .updateExpr(
        Map(
          OutcomeColumn -> s"source.$OutcomeColumn",
          CandidateWatermarkColumn -> s"source.$CandidateWatermarkColumn",
          CompletedAtColumn -> s"source.$CompletedAtColumn"
        ).asJava
      )
      .execute()
  }

  private def ensureProgressTable(): Unit =
    AnalyticsTableSchemas.createOrValidate(spark, paths.streamingProgress, ProgressShape)

  private def ensureProgressSchema(): Unit =
    if (!AnalyticsTableSchemas.matches(spark.read.format("delta").load(paths.streamingProgress).schema, ProgressShape))
      throw malformedProgress

  private def ensureDecisionTable(): Unit =
    AnalyticsTableSchemas.createOrValidate(spark, paths.streamingDecisions, DecisionShape)

  private def ensureDecisionSchema(): Unit =
    if (!AnalyticsTableSchemas.matches(spark.read.format("delta").load(paths.streamingDecisions).schema, DecisionShape))
      throw malformedProgress

  private def deltaTableExists(path: String): Boolean = {
    val tablePath = new org.apache.hadoop.fs.Path(path)
    val fileSystem = tablePath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fileSystem.exists(tablePath)) false
    else if (DeltaTable.isDeltaTable(spark, path)) true
    else throw malformedProgress
  }

  private def readRequiredInstant(row: Row, name: String): Option[Instant] =
    Either
      .catchNonFatal(Option(row.getAs[Timestamp](name)).map(_.toInstant.truncatedTo(ChronoUnit.MICROS)))
      .toOption
      .flatten

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
  private val DeliveredOffsetsColumn = "deliveredOffsets"
  private val OutcomeColumn = "outcome"
  private val CandidateWatermarkColumn = "candidateWatermark"
  private val CompletedAtColumn = "completedAt"
  private val RevisionColumn = "revision"
  private val MarkerFingerprintColumn = "deletionMarkerFingerprint"
  private val PreparedName = "Prepared"
  private val IngestionCommittedName = "IngestionCommitted"
  private val QualityBlockedName = "QualityBlocked"
  private val ErasurePendingName = "ErasurePending"
  private val PublishedName = "Published"

  private val OffsetType = StructType(
    Vector(
      StructField("topic", StringType, nullable = false),
      StructField("partition", IntegerType, nullable = false),
      StructField("minimumDeliveredOffset", LongType, nullable = false),
      StructField("maximumDeliveredOffset", LongType, nullable = false),
      StructField("deliveredRecordCount", LongType, nullable = false)
    )
  )
  private val ProgressShape: AnalyticsTableSchemas.Shape = Vector(
    LineageColumn -> StringType,
    BatchIdColumn -> LongType,
    ObservedAtColumn -> TimestampType,
    PriorWatermarkColumn -> TimestampType,
    InputFingerprintColumn -> StringType,
    DeliveredOffsetsColumn -> ArrayType(OffsetType, containsNull = false),
    OutcomeColumn -> StringType,
    CandidateWatermarkColumn -> TimestampType,
    CompletedAtColumn -> TimestampType
  )
  private val DecisionShape: AnalyticsTableSchemas.Shape = Vector(
    LineageColumn -> StringType,
    BatchIdColumn -> LongType,
    RevisionColumn -> LongType,
    MarkerFingerprintColumn -> StringType,
    CandidateWatermarkColumn -> TimestampType
  )
  private val ProgressSchema = AnalyticsTableSchemas.struct(ProgressShape)
  private val DecisionSchema = AnalyticsTableSchemas.struct(DecisionShape)
  private val identityCondition =
    s"target.$LineageColumn = source.$LineageColumn AND target.$BatchIdColumn = source.$BatchIdColumn"
  private val decisionCondition =
    s"target.$LineageColumn = source.$LineageColumn AND target.$BatchIdColumn = source.$BatchIdColumn AND " +
      s"target.$RevisionColumn = source.$RevisionColumn"

  private val malformedProgress = AnalyticsError.InvalidConfiguration("streaming batch journal record is malformed")
  private val preparationConflict = AnalyticsError.InvalidInput(
    NonEmptyChain.one("streaming batch journal conflicts with persisted immutable input evidence")
  )
  private val progressConflict = AnalyticsError.InvalidInput(
    NonEmptyChain.one("streaming batch journal operation conflicts with persisted progress")
  )
}
