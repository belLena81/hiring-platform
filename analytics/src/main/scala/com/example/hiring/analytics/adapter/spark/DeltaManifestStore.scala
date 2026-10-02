package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AnalyticsRunManifest
import com.example.hiring.analytics.domain.AnalyticsTopic
import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths
import com.example.hiring.analytics.service.batch.{AnalyticsManifestStatus, AnalyticsRunManifestStore}

import cats.effect.Async
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SparkSession}
import scala.jdk.CollectionConverters.*

/** Persists run state in Delta independently of the batch workflow so other compositions can share it. */
private[analytics] final class DeltaManifestStore[F[_]: Async](
    spark: SparkSession,
    paths: AnalyticsLakehousePaths,
    sparkExecution: SparkExecution[F]
) extends AnalyticsRunManifestStore[F] {
  override def persist(
      manifest: AnalyticsRunManifest,
      status: AnalyticsManifestStatus,
      updatedAt: java.time.Instant
  ): F[Unit] =
    LakehouseErrors.adapt(sparkExecution {
      val rows = manifest.offsetRanges.map(range =>
        Row(
          manifest.runId.value,
          AnalyticsTopic.unwrap(range.topic),
          range.partition,
          range.startOffset,
          range.endOffsetExclusive,
          status.persistedName,
          updatedAt.toString
        )
      )
      AnalyticsTableSchemas.createOrValidate(spark, paths.manifests, AnalyticsTableSchemas.manifests)
      val frame = spark.createDataFrame(rows.asJava, AnalyticsTableSchemas.struct(AnalyticsTableSchemas.manifests))
      val condition =
        "target.runId = source.runId AND target.topic = source.topic AND target.partition = source.partition"
      DeltaTable
        .forPath(spark, SparkPhysicalLocation.resolve(paths.manifests))
        .as("target")
        .merge(frame.as("source"), condition)
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
      ()
    })
}
