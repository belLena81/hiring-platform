package com.example.hiring.analytics.adapter.spark
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import com.example.hiring.analytics.*

import cats.effect.Async
import cats.syntax.all.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.time.Instant
import java.sql.Timestamp
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** Reads a bounded operational range and commits its replayable Bronze representation. */
private[spark] final class AnalyticsBatchIngestionStage[F[_]: Async](ports: IngestionStagePorts[F]) {
  import ports.*
  private val blocking = execution

  def ingest(
      spark: SparkSession,
      source: BoundedOperationalEventSource[F],
      manifest: AnalyticsRunManifest,
      markerTokens: DataFrame
  ): F[AnalyticsBronzeInput] =
    for {
      raw <- source.read(spark, manifest)
      rawSchema <- blocking(raw.schema)
      _ <- KafkaRecordColumns.validate(rawSchema)
      _ <- source.verifyOffsets(raw, manifest)
      _ <- validateRunIdentity(spark, manifest)
      parsed <- blocking(OperationalEventTransforms.parseKafkaRecords(raw))
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
          AnalyticsRetention.BronzeDays
        )
      )
      recordCount <- blocking(raw.count())
      _ <- manifestStore.persist(spark, manifest, "STARTED", startedAt.toString)
      _ <- deltaWriter.merge(
        incoming,
        paths.bronze,
        "target.topic = source.topic AND target.partition = source.partition AND target.offset = source.offset"
      )
    } yield AnalyticsBronzeInput(parsed, startedAt, recordCount)

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
