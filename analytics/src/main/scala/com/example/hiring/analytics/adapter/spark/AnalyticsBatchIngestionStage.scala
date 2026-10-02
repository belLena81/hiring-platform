package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.config.AnalyticsPositiveInt.*

import com.example.hiring.analytics.domain.AnalyticsRunManifest
import com.example.hiring.analytics.domain.AnalyticsTopic
import com.example.hiring.analytics.domain.SubjectPseudonymizer
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.config.AnalyticsRetentionSettings
import com.example.hiring.analytics.service.batch.{
  AnalyticsLakehousePaths,
  AnalyticsManifestStatus,
  AnalyticsRunManifestStore
}

import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all.*
import io.github.iltotore.iron.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.*

/** Reads a bounded operational range and commits its replayable Bronze representation. */
private[analytics] final class AnalyticsBatchIngestionStage[F[_]: Async](
    paths: AnalyticsLakehousePaths,
    pseudonymizer: SubjectPseudonymizer,
    execution: SparkExecution[F],
    manifestStore: AnalyticsRunManifestStore[F],
    deltaWriter: DeltaWriter[F],
    retention: AnalyticsRetentionSettings,
    private[analytics] val nowOverride: Option[F[java.time.Instant]] = None
) {
  private val blocking = execution
  private val now = nowOverride.getOrElse(Async[F].realTimeInstant)

  def ingest(
      spark: SparkSession,
      source: BoundedOperationalEventSource[F],
      manifest: AnalyticsRunManifest,
      markerTokens: DataFrame,
      configureTables: F[Unit]
  ): Resource[F, AnalyticsBronzeInput] =
    Resource
      .make(
        for {
          raw <- source.read(spark, manifest)
          rawSchema <- blocking(raw.schema)
          _ <- KafkaRecordColumns.validate(rawSchema)
          parsed <- blocking(OperationalEventTransforms.parseKafkaRecords(raw).persist())
        } yield parsed
      )(parsed => blocking(parsed.unpersist(blocking = true)).void)
      .evalMap { parsed =>
        for {
          _ <- source.verifyOffsets(parsed, manifest)
          _ <- validateRunIdentity(spark, manifest)
          bronze <- persistParsed(
            spark,
            parsed,
            markerTokens,
            configureTables,
            now,
            startedAt => manifestStore.persist(manifest, AnalyticsManifestStatus.Started, startedAt)
          )
        } yield bronze
      }

  /** Persists the shared Bronze representation of an already parsed Kafka frame.
    *
    * Streaming callers supply their stable observation time and no batch manifest callback. This method deliberately
    * consumes the provided frame as-is: it neither reads a source nor derives Kafka offset spans.
    */
  private[analytics] def persistParsed(
      spark: SparkSession,
      parsed: DataFrame,
      markerTokens: DataFrame,
      configureTables: F[Unit],
      startedAt: F[java.time.Instant],
      beforeMerge: java.time.Instant => F[Unit]
  ): F[AnalyticsBronzeInput] =
    for {
      _ <- configureTables
      valid <- blocking(OperationalEventTransforms.validEvents(parsed))
      pseudonymized <- blocking(AnalyticsSubjectPrivacy.withSubjectToken(valid, pseudonymizer))
      safeToPersist <- blocking.either(
        AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(pseudonymized, markerTokens)
      )
      observedAt <- startedAt
      incoming <- blocking(
        deltaWriter.withExpiry(
          OperationalEventTransforms.bronze(safeToPersist),
          observedAt,
          retention.bronzeDays.value
        )
      )
      counts <- blocking {
        val row = parsed
          .agg(
            count(lit(1)).as("recordCount"),
            count(when(OperationalEventTransforms.isValidEvent, lit(1))).as("validCount"),
            count(when(!OperationalEventTransforms.isValidEvent, lit(1))).as("malformedCount")
          )
          .head()
        (row.getLong(0), row.getLong(1), row.getLong(2))
      }
      _ <- beforeMerge(observedAt)
      _ <- deltaWriter.merge(
        incoming,
        paths.bronze,
        "target.topic = source.topic AND target.partition = source.partition AND target.offset = source.offset"
      )
    } yield AnalyticsBronzeInput(parsed, observedAt, counts._1, counts._2, counts._3)

  private def validateRunIdentity(spark: SparkSession, manifest: AnalyticsRunManifest): F[Unit] = blocking.either {
    if (DeltaTable.isDeltaTable(spark, SparkPhysicalLocation.resolve(paths.manifests))) {
      val existing = spark.read
        .format("delta")
        .load(SparkPhysicalLocation.resolve(paths.manifests))
        .filter(col("runId") === lit(manifest.runId.value))
        .select("topic", "partition", "startOffset", "endOffsetExclusive")
        .distinct()
        .collect()
        .toVector
        .map(row => (row.getString(0), row.getInt(1), row.getLong(2), row.getLong(3)))
        .toSet
      val expected = manifest.offsetRanges
        .map(range =>
          (AnalyticsTopic.unwrap(range.topic), range.partition, range.startOffset, range.endOffsetExclusive)
        )
        .toSet
      Either.cond(existing.isEmpty || existing == expected, (), AnalyticsError.RunIdRangeConflict(manifest.runId.value))
    } else Right(())
  }
}
