package com.example.hiring.analytics.adapter.spark

import cats.effect.Async
import cats.syntax.all.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.*
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.*

import java.sql.Timestamp
import java.time.{Clock, Instant}
import scala.jdk.CollectionConverters.*

/** Explicit coordinate selection and normalized Silver merge; the caller owns the shared lakehouse mutex. */
private[analytics] final class SparkAnalyticsLateFactReplayStages[F[_]: Async](
    spark: SparkSession,
    paths: AnalyticsLakehousePaths,
    execution: SparkExecution[F],
    reader: DeltaBatchReader[F],
    writer: DeltaWriter[F],
    maintenance: AnalyticsBatchMaintenance[F],
    clock: cats.effect.Clock[F],
    mergeClock: Clock = Clock.systemUTC()
) extends AnalyticsLateFactReplayStages[F] {
  private val F = Async[F]
  private val now = clock.realTimeInstant
  private val silverShape = AnalyticsTableSchemas.silver ++ AnalyticsTableSchemas.expiry

  override def validateHmacConfiguration: F[Unit] = maintenance.validateHmacConfigurationLocked

  override def validateSelectedFacts(
      request: AnalyticsLateFactReplayRequest,
      activeTokens: Vector[SubjectToken],
      observedAt: Instant
  ): F[Unit] =
    selectedFacts(request, activeTokens, observedAt).void

  override def applyActiveDeletions(activeTokens: Vector[SubjectToken]): F[Unit] =
    if (activeTokens.isEmpty) F.unit else maintenance.applyActiveDeletions(activeTokens)

  override def mergeSelectedFacts(
      request: AnalyticsLateFactReplayRequest,
      activeTokens: Vector[SubjectToken],
      observedAt: Instant
  ): F[Unit] =
    for {
      selected <- selectedFacts(request, activeTokens, observedAt)
      silver <- execution {
        // Multiple Kafka coordinates can name the same event. Prefer its earliest original expiry.
        val ordering = Window
          .partitionBy(Columns.EventId)
          .orderBy(
            col(Columns.ExpiresAt),
            col(Columns.IngestedAt),
            col(Columns.Topic),
            col(Columns.Partition),
            col(Columns.Offset)
          )
        selected
          .withColumn("replayRow", row_number().over(ordering))
          .filter(col("replayRow") === lit(1))
          .select(silverShape.map { case (name, _) => col(name) }*)
      }
      _ <- writer.mergeWhenFresh(silver, paths.silver, "target.eventId = source.eventId", () => mergeClock.instant())
    } yield ()

  override def rebuildGoldAndExtractReport(asOf: Instant): F[AnalyticsReportOutput] =
    for {
      _ <- maintenance.expireStored(asOf)
      silver <- reader.readOrEmpty(spark, paths.silver, AnalyticsTableSchemas.struct(silverShape))
      _ <- execution.either(
        Either.cond(
          AnalyticsTableSchemas.matches(silver.schema, silverShape),
          (),
          AnalyticsError.InvalidLateFactSchema("stored Silver schema is incompatible with late replay")
        )
      )
      _ <- AnalyticsGoldStage.rebuild(paths, silver, execution)
      report <- AnalyticsGoldStage.extract(spark, paths, asOf, execution)
    } yield report

  private def selectedFacts(
      request: AnalyticsLateFactReplayRequest,
      activeTokens: Vector[SubjectToken],
      observedAt: Instant
  ): F[DataFrame] =
    for {
      actualNow <- now
      at = if (actualNow.isAfter(observedAt)) actualNow else observedAt
      _ <- F.raiseUnless(
        request.coordinates.nonEmpty &&
          request.coordinates.size <= AnalyticsLateFactReplayRequest.MaximumCoordinates
      )(AnalyticsError.LateFactReplayRejected)
      retained <- reader.readOrEmpty(
        spark,
        paths.lateFacts,
        AnalyticsTableSchemas.struct(AnalyticsTableSchemas.lateFacts)
      )
      _ <- execution.either(
        Either.cond(
          AnalyticsTableSchemas.matches(retained.schema, AnalyticsTableSchemas.lateFacts),
          (),
          AnalyticsError.InvalidLateFactSchema("persisted dataset does not match its declared schema")
        )
      )
      selected <- execution.either {
        val coordinateSchema = StructType(
          Seq(
            StructField(Columns.Topic, StringType, false),
            StructField(Columns.Partition, IntegerType, false),
            StructField(Columns.Offset, LongType, false)
          )
        )
        val coordinates = request.coordinates.map(coordinate =>
          Row(
            AnalyticsTopic.unwrap(coordinate.topic),
            AnalyticsPartition.unwrap(coordinate.partition),
            AnalyticsOffset.unwrap(coordinate.offset)
          )
        )
        val selector = spark.createDataFrame(coordinates.asJava, coordinateSchema)
        val rows = retained
          .join(broadcast(selector), Seq(Columns.Topic, Columns.Partition, Columns.Offset), "inner")
          .select(AnalyticsTableSchemas.lateFacts.map { case (name, _) => col(name) }*)
          .limit(request.coordinates.size + 1)
          .collect()
          .toVector
        val required = Vector(
          Columns.EventId,
          Columns.EventFingerprint,
          Columns.EventType,
          Columns.OccurredAt,
          Columns.AggregateType,
          Columns.AggregateId,
          Columns.SubjectToken,
          Columns.SubjectTokens,
          Columns.IngestedAt,
          Columns.ExpiresAt,
          Columns.AdmissionReason
        )
        val essentialValuesValid = rows.forall(row =>
          required.forall(name => !row.isNullAt(row.fieldIndex(name))) &&
            row.getAs[Timestamp](Columns.ExpiresAt).toInstant.isAfter(at) &&
            row.getAs[Timestamp](Columns.ExpiresAt).after(row.getAs[Timestamp](Columns.IngestedAt)) &&
            row.getAs[String](Columns.EventFingerprint).matches("[a-f0-9]{64}") &&
            row.getAs[String](Columns.AdmissionReason) == "CLOSED_DAY" &&
            row.getAs[scala.collection.Seq[String]](Columns.SubjectTokens).nonEmpty &&
            row
              .getAs[scala.collection.Seq[String]](Columns.SubjectTokens)
              .forall(token => Option(token).exists(_.nonEmpty)) &&
            row
              .getAs[scala.collection.Seq[String]](Columns.SubjectTokens)
              .contains(row.getAs[String](Columns.SubjectToken))
        )
        val actualCoordinates = rows
          .map(row =>
            (row.getAs[String](Columns.Topic), row.getAs[Int](Columns.Partition), row.getAs[Long](Columns.Offset))
          )
          .toSet
        val expectedCoordinates = request.coordinates
          .map(coordinate =>
            (
              AnalyticsTopic.unwrap(coordinate.topic),
              AnalyticsPartition.unwrap(coordinate.partition),
              AnalyticsOffset.unwrap(coordinate.offset)
            )
          )
          .toSet
        Either.cond(
          rows.size == request.coordinates.size && actualCoordinates == expectedCoordinates && essentialValuesValid,
          spark.createDataFrame(rows.asJava, retained.schema),
          AnalyticsError.LateFactReplayRejected
        )
      }
      _ <- execution.either {
        val markerSchema = StructType(Seq(StructField(Columns.SubjectToken, StringType, false)))
        val markers = spark.createDataFrame(activeTokens.map(token => Row(token.value)).asJava, markerSchema)
        AnalyticsSubjectPrivacy
          .excludeActiveDeletionMarkers(selected, markers)
          .flatMap(safe =>
            Either.cond(safe.count() == request.coordinates.size.toLong, (), AnalyticsError.LateFactReplayRejected)
          )
      }
      storedSilver <- reader.readOrEmpty(spark, paths.silver, AnalyticsTableSchemas.struct(silverShape))
      _ <- execution.either {
        if (!AnalyticsTableSchemas.matches(storedSilver.schema, silverShape))
          Left(AnalyticsError.InvalidLateFactSchema("stored Silver schema is incompatible with late replay"))
        else {
          val incoming = selected.select(Columns.EventId, Columns.EventFingerprint).distinct()
          val retainedFingerprints = retained
            .filter(col(Columns.ExpiresAt) > lit(Timestamp.from(at)))
            .select(Columns.EventId, Columns.EventFingerprint)
          val stored = storedSilver.select(Columns.EventId, Columns.EventFingerprint).unionByName(retainedFingerprints)
          val conflicts = incoming
            .as("incoming")
            .join(stored.as("stored"), col("incoming.eventId") === col("stored.eventId"), "inner")
            .filter(
              col("incoming.eventFingerprint") =!= col("stored.eventFingerprint") || col(
                "stored.eventFingerprint"
              ).isNull
            )
            .limit(1)
            .count()
          Either.cond(conflicts == 0L, (), AnalyticsError.LateFactReplayRejected)
        }
      }
    } yield selected
}
