package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

import cats.effect.Async
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, concat, lit, sha2, struct, to_json, when}
import org.apache.spark.sql.types.StructType

import java.sql.Timestamp
import java.time.Instant

/** Owns idempotent Delta writes used by the bounded batch pipeline. */
private[analytics] final class DeltaBatchWriter[F[_]: Async](
    paths: AnalyticsLakehousePaths,
    execution: SparkExecution[F]
) extends DeltaWriter[F] {
  override def merge(source: DataFrame, path: String, condition: String): F[Unit] = execution {
    val shape =
      if (path == paths.bronze) AnalyticsTableSchemas.bronze
      else if (path == paths.quarantine) AnalyticsTableSchemas.quarantine
      else AnalyticsTableSchemas.silver ++ AnalyticsTableSchemas.expiry
    AnalyticsTableSchemas.createOrValidate(
      source.sparkSession,
      path,
      shape,
      raw = path == paths.bronze || path == paths.quarantine
    )
    if (!AnalyticsTableSchemas.matches(source.schema, shape))
      throw com.example.hiring.analytics.errors.AnalyticsError.LakehouseFailure(
        new IllegalStateException(s"incoming Delta schema differs from the expected analytics schema at $path")
      )
    DeltaTable
      .forPath(source.sparkSession, path)
      .as("target")
      .merge(source.as("source"), condition)
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  override def withExpiry(frame: DataFrame, now: Instant, days: Int): DataFrame =
    frame
      .withColumn(Columns.IngestedAt, lit(Timestamp.from(now)))
      .withColumn(Columns.ExpiresAt, lit(Timestamp.from(now.plus(java.time.Duration.ofDays(days.toLong)))))
}

/** Reads an existing Delta table or creates an empty frame with the caller's expected schema. */
private[analytics] final class DeltaBatchReader[F[_]: Async](execution: SparkExecution[F]) extends DeltaReader[F] {
  override def readOrEmpty(spark: SparkSession, path: String, schema: StructType): F[DataFrame] = execution {
    if (DeltaTable.isDeltaTable(spark, path)) spark.read.format("delta").load(path)
    else spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }
}

/** Builds stable quarantine keys for malformed and conflicting operational events. */
private[analytics] object QuarantineIdentifier extends QuarantineId {
  override def apply(): Column =
    when(
      col("rawValue").isNull,
      concat(
        lit("tombstone:"),
        to_json(struct(col("topic").as("topic"), col("partition").as("partition"), col("offset").as("offset")))
      )
    ).otherwise(sha2(col("rawValue"), 256))
}
