package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.config.AnalyticsPositiveInt.*

import com.example.hiring.analytics.domain.AnalyticsRunManifest
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsManifestStatus

import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all.*
import io.github.iltotore.iron.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.*

import java.time.Instant

/** Reads a bounded operational range and commits its replayable Bronze representation. */
private[spark] final class AnalyticsBatchIngestionStage[F[_]: Async](ports: IngestionStagePorts[F]) {
  import ports.*
  private val blocking = execution

  def ingest(
      spark: SparkSession,
      source: BoundedOperationalEventSource[F],
      manifest: AnalyticsRunManifest,
      markerTokens: DataFrame
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
          valid <- blocking(OperationalEventTransforms.validEvents(parsed))
          pseudonymized <- blocking(AnalyticsSubjectPrivacy.withSubjectToken(valid, pseudonymizer))
          safeToPersist <- blocking.either(
            AnalyticsSubjectPrivacy.excludeActiveDeletionMarkers(pseudonymized, markerTokens)
          )
          startedAt <- clock.realTime.map(duration => Instant.ofEpochMilli(duration.toMillis))
          incoming <- blocking(
            deltaWriter.withExpiry(
              OperationalEventTransforms.bronze(safeToPersist),
              startedAt,
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
          _ <- manifestStore.persist(manifest, AnalyticsManifestStatus.Started, startedAt)
          _ <- deltaWriter.merge(
            incoming,
            paths.bronze,
            "target.topic = source.topic AND target.partition = source.partition AND target.offset = source.offset"
          )
        } yield AnalyticsBronzeInput(parsed, startedAt, counts._1, counts._2, counts._3)
      }

  private def validateRunIdentity(spark: SparkSession, manifest: AnalyticsRunManifest): F[Unit] = blocking.either {
    if (DeltaTable.isDeltaTable(spark, paths.manifests)) {
      val existing = spark.read
        .format("delta")
        .load(paths.manifests)
        .filter(col("runId") === lit(manifest.runId.value))
        .select("topic", "partition", "startOffset", "endOffsetExclusive")
        .distinct()
        .collect()
        .toVector
        .map(row => (row.getString(0), row.getInt(1), row.getLong(2), row.getLong(3)))
        .toSet
      val expected = manifest.offsetRanges
        .map(range => (range.topic, range.partition, range.startOffset, range.endOffsetExclusive))
        .toSet
      Either.cond(existing.isEmpty || existing == expected, (), AnalyticsError.RunIdRangeConflict(manifest.runId.value))
    } else Right(())
  }
}
