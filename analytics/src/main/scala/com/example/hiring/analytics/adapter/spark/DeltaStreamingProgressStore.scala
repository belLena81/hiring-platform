package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import com.example.hiring.analytics.service.streaming.StreamingProgressRepository

import cats.data.NonEmptyChain
import cats.effect.Async
import cats.syntax.all.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import scala.jdk.CollectionConverters.*

/** Durable preparation and terminal progress for one Spark streaming lineage. */
private[analytics] final class DeltaStreamingProgressStore[F[_]: Async](
    spark: SparkSession,
    paths: AnalyticsLakehousePaths,
    execution: SparkExecution[F]
) extends StreamingProgressRepository[F] {
  import DeltaStreamingProgressStore.*

  override def load(identity: StreamingBatchIdentity): F[Option[StreamingBatchProgress]] =
    execution.either {
      if (!DeltaTable.isDeltaTable(spark, paths.streamingProgress)) Right(None)
      else
        persistedRow(identityFilter(identity)) match {
          case None        => Right(None)
          case Some(value) => decode(value)
        }
    }

  override def loadLatest(lineage: StreamingLineage): F[Option[StreamingBatchProgress]] =
    execution.either {
      if (!DeltaTable.isDeltaTable(spark, paths.streamingProgress)) Right(None)
      else {
        val latest = spark.read
          .format("delta")
          .load(paths.streamingProgress)
          .filter(col("lineage") === lit(lineage.value) && col(OutcomeColumn) =!= lit(PreparedName))
          .orderBy(col("batchId").desc)
          .limit(1)
          .collect()
          .headOption
        latest.traverse(decode).map(_.flatten)
      }
    }

  /** Inserts a preparation once; retries with identical inputs are no-ops and conflicts fail closed. */
  def prepare(preparation: StreamingBatchPreparation): F[Unit] =
    execution.either {
      ensureTable()
      val key = identityFilter(preparation.identity)
      persistedRow(key) match {
        case Some(row) if preparationMatches(row, preparation) => Right(())
        case Some(_)                                           => Left(preparationConflict)
        case None                                              =>
          val row = preparationRow(preparation)
          val source = spark.createDataFrame(
            Vector(row).asJava,
            AnalyticsTableSchemas.struct(AnalyticsTableSchemas.streamingProgress)
          )
          DeltaTable
            .forPath(spark, paths.streamingProgress)
            .as("target")
            .merge(source.as("source"), "target.lineage = source.lineage AND target.batchId = source.batchId")
            .whenNotMatched()
            .insertAll()
            .execute()
          // A concurrent insert with a different preparation must not be accepted as success.
          Either.cond(persistedRow(key).exists(preparationMatches(_, preparation)), (), preparationConflict)
      }
    }

  /** Commits a terminal projection without ever changing the immutable preparation columns. */
  def complete(progress: StreamingBatchProgress): F[Unit] =
    Async[F].fromEither(
      StreamingBatchProgress.validate(progress).leftMap(_ => preparationConflict)
    ) *> (progress.outcome match {
      case terminal =>
        prepare(progress.preparation) *> execution.either {
          ensureTable()
          val key = identityFilter(progress.preparation.identity)
          persistedRow(key) match {
            case None                                                          => Left(preparationConflict)
            case Some(row) if !preparationMatches(row, progress.preparation)   => Left(preparationConflict)
            case Some(row) if terminalMatches(row, progress)                   => Right(())
            case Some(row) if row.getAs[String](OutcomeColumn) != PreparedName => Left(preparationConflict)
            case Some(_)                                                       =>
              val source = spark.createDataFrame(
                Vector(
                  Row(
                    progress.preparation.identity.lineage.value,
                    progress.preparation.identity.batchId.value,
                    terminal.toString,
                    progress.candidateWatermark.map(Timestamp.from).orNull,
                    progress.completedAt.map(Timestamp.from).orNull
                  )
                ).asJava,
                completionSchema
              )
              DeltaTable
                .forPath(spark, paths.streamingProgress)
                .as("target")
                .merge(
                  source.as("source"),
                  "target.lineage = source.lineage AND target.batchId = source.batchId AND target.outcome = 'Prepared'"
                )
                .whenMatched()
                .updateExpr(
                  Map(
                    "outcome" -> "source.outcome",
                    "candidateWatermark" -> "source.candidateWatermark",
                    "completedAt" -> "source.completedAt"
                  ).asJava
                )
                .execute()
              Either.cond(
                persistedRow(key)
                  .exists(row => preparationMatches(row, progress.preparation) && terminalMatches(row, progress)),
                (),
                preparationConflict
              )
          }
        }
    })

  private def decode(row: Row): Either[AnalyticsError, Option[StreamingBatchProgress]] = {
    val malformed = AnalyticsError.InvalidConfiguration("streaming progress record is malformed")
    Either
      .catchNonFatal {
        val lineage = row.getAs[String]("lineage")
        val batchId = row.getAs[Long]("batchId")
        val offsetRows = row.getAs[Seq[Row]]("deliveredOffsets").toVector
        (lineage, batchId, offsetRows)
      }
      .leftMap(_ => malformed)
      .flatMap { case (lineageValue, batchValue, offsetRows) =>
        for {
          lineage <- StreamingLineage.from(lineageValue).leftMap(_ => malformed)
          batchId <- StreamingBatchId.from(batchValue).leftMap(_ => malformed)
          observedAt <- readTimestamp(row, "observedAt").toRight(malformed)
          fingerprint <- RangeFingerprint.from(row.getAs[String]("inputFingerprint")).leftMap(_ => malformed)
          offsets <- offsetRows
            .traverse(offsetRow =>
              StreamingPartitionSummary
                .from(
                  offsetRow.getAs[String]("topic"),
                  offsetRow.getAs[Int]("partition"),
                  offsetRow.getAs[Long]("minimumDeliveredOffset"),
                  offsetRow.getAs[Long]("maximumDeliveredOffset"),
                  offsetRow.getAs[Long]("deliveredRecordCount")
                )
                .toEither
                .leftMap(_ => malformed)
            )
          priorWatermark <- readOptionalTimestamp(row, "priorWatermark").leftMap(_ => malformed)
          preparedCandidate <- readOptionalTimestamp(row, "preparedCandidateWatermark").leftMap(_ => malformed)
          outcome <- StreamingBatchOutcome
            .fromPersisted(row.getAs[String](OutcomeColumn))
            .leftMap(_ => malformed)
          completedAt <- readOptionalTimestamp(row, CompletedAtColumn).leftMap(_ => malformed)
          committedCandidate <- readOptionalTimestamp(row, CandidateWatermarkColumn).leftMap(_ => malformed)
          preparation = StreamingBatchPreparation(
            StreamingBatchIdentity(lineage, batchId),
            observedAt,
            priorWatermark,
            preparedCandidate,
            fingerprint,
            offsets
          )
          progress = StreamingBatchProgress(preparation, outcome, committedCandidate, completedAt)
          _ <-
            if (outcome == StreamingBatchOutcome.Prepared) {
              Either.cond(completedAt.isEmpty && committedCandidate.isEmpty, (), malformed)
            } else StreamingBatchProgress.validate(progress).leftMap(_ => malformed).void
        } yield Some(progress)
      }
  }

  private def readTimestamp(row: Row, name: String): Option[java.time.Instant] =
    Option(row.getAs[Timestamp](name)).map(_.toInstant.truncatedTo(ChronoUnit.MICROS))

  private def readOptionalTimestamp(row: Row, name: String): Either[Unit, Option[java.time.Instant]] =
    Either
      .catchNonFatal(Option(row.getAs[Timestamp](name)).map(_.toInstant.truncatedTo(ChronoUnit.MICROS)))
      .leftMap(_ => ())

  private def ensureTable(): Unit =
    AnalyticsTableSchemas.createOrValidate(spark, paths.streamingProgress, AnalyticsTableSchemas.streamingProgress)

  private def persistedRow(filter: org.apache.spark.sql.Column): Option[Row] =
    spark.read.format("delta").load(paths.streamingProgress).filter(filter).limit(1).collect().headOption

  private def identityFilter(identity: StreamingBatchIdentity): org.apache.spark.sql.Column =
    col("lineage") === lit(identity.lineage.value) && col("batchId") === lit(identity.batchId.value)

  private def preparationMatches(row: Row, preparation: StreamingBatchPreparation): Boolean = {
    val expected = preparationRow(preparation)
    PreparationColumns.zipWithIndex.forall { case (name, index) =>
      val actualValue = row.get(row.fieldIndex(name))
      val expectedValue = expected.get(index)
      if (TimestampColumns.contains(name)) sameTimestamp(actualValue, expectedValue)
      else actualValue == expectedValue
    }
  }

  private def terminalMatches(row: Row, progress: StreamingBatchProgress): Boolean =
    row.getAs[String](OutcomeColumn) == progress.outcome.toString &&
      timestamp(row, CandidateWatermarkColumn) == progress.candidateWatermark.map(_.truncatedTo(ChronoUnit.MICROS))

  private def timestamp(row: Row, name: String): Option[java.time.Instant] =
    Option(row.getAs[Timestamp](name)).map(_.toInstant.truncatedTo(ChronoUnit.MICROS))

  private def sameTimestamp(left: Any, right: Any): Boolean =
    Option(left.asInstanceOf[Timestamp]).map(_.toInstant.truncatedTo(ChronoUnit.MICROS)) ==
      Option(right.asInstanceOf[Timestamp]).map(_.toInstant.truncatedTo(ChronoUnit.MICROS))

  private def preparationRow(preparation: StreamingBatchPreparation): Row = {
    val offsets = preparation.deliveredOffsets
      .sortBy(summary => (AnalyticsTopic.unwrap(summary.topic), AnalyticsPartition.unwrap(summary.partition)))
      .map { summary =>
        Row(
          AnalyticsTopic.unwrap(summary.topic),
          AnalyticsPartition.unwrap(summary.partition),
          AnalyticsOffset.unwrap(summary.minimumDeliveredOffset),
          AnalyticsOffset.unwrap(summary.maximumDeliveredOffset),
          summary.deliveredRecordCount
        )
      }
    Row(
      preparation.identity.lineage.value,
      preparation.identity.batchId.value,
      Timestamp.from(preparation.observedAt),
      preparation.priorWatermark.map(Timestamp.from).orNull,
      preparation.candidateWatermark.map(Timestamp.from).orNull,
      preparation.inputFingerprint.value,
      offsets,
      PreparedName,
      null,
      null
    )
  }
}

private object DeltaStreamingProgressStore {
  private val OutcomeColumn = "outcome"
  private val CandidateWatermarkColumn = "candidateWatermark"
  private val CompletedAtColumn = "completedAt"
  private val PreparedName = StreamingBatchOutcome.Prepared.toString
  private val PreparationColumns = Vector(
    "lineage",
    "batchId",
    "observedAt",
    "priorWatermark",
    "preparedCandidateWatermark",
    "inputFingerprint",
    "deliveredOffsets"
  )
  private val TimestampColumns = Set("observedAt", "priorWatermark", "preparedCandidateWatermark")
  private val completionSchema = org.apache.spark.sql.types.StructType(
    Vector(
      org.apache.spark.sql.types.StructField("lineage", org.apache.spark.sql.types.StringType, nullable = false),
      org.apache.spark.sql.types.StructField("batchId", org.apache.spark.sql.types.LongType, nullable = false),
      org.apache.spark.sql.types.StructField("outcome", org.apache.spark.sql.types.StringType, nullable = false),
      org.apache.spark.sql.types
        .StructField("candidateWatermark", org.apache.spark.sql.types.TimestampType, nullable = true),
      org.apache.spark.sql.types.StructField("completedAt", org.apache.spark.sql.types.TimestampType, nullable = true)
    )
  )

  private val preparationConflict = AnalyticsError.InvalidInput(
    NonEmptyChain.one("streaming batch preparation conflicts with persisted progress")
  )
}
