package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AnalyticsRunManifest
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.{AnalyticsLakehousePaths, ManifestStore}
import cats.effect.Async
import cats.syntax.all.*
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Persists run state in Delta independently of the batch workflow so other compositions can share it. */
private[analytics] final class DeltaManifestStore[F[_]: Async](paths: AnalyticsLakehousePaths)
    extends ManifestStore[F] {
  override def persist(
      spark: SparkSession,
      manifest: AnalyticsRunManifest,
      status: String,
      updatedAt: String
  ): F[Unit] =
    Async[F]
      .blocking {
        val rows = manifest.offsetRanges.map(range =>
          Row(
            manifest.runId.value,
            range.topic,
            range.partition,
            range.startOffset,
            range.endOffsetExclusive,
            status,
            updatedAt
          )
        )
        val schema = StructType(
          Seq(
            StructField("runId", StringType, nullable = false),
            StructField("topic", StringType, nullable = false),
            StructField("partition", IntegerType, nullable = false),
            StructField("startOffset", LongType, nullable = false),
            StructField("endOffsetExclusive", LongType, nullable = false),
            StructField("status", StringType, nullable = false),
            StructField("updatedAt", StringType, nullable = false)
          )
        )
        val frame = spark.createDataFrame(rows.asJava, schema)
        val condition =
          "target.runId = source.runId AND target.topic = source.topic AND target.partition = source.partition"
        if (DeltaTable.isDeltaTable(spark, paths.manifests))
          DeltaTable
            .forPath(spark, paths.manifests)
            .as("target")
            .merge(frame.as("source"), condition)
            .whenMatched()
            .updateAll()
            .whenNotMatched()
            .insertAll()
            .execute()
        else frame.write.format("delta").mode("errorifexists").save(paths.manifests)
        ()
      }
      .handleErrorWith {
        case error: AnalyticsError => Async[F].raiseError(error)
        case NonFatal(error)       => Async[F].raiseError(AnalyticsError.LakehouseFailure(error))
        case error                 => Async[F].raiseError(error)
      }
}
