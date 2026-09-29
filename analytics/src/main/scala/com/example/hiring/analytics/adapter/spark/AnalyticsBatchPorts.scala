package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AnalyticsRunManifest

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Supplies one explicit bounded Kafka range to the analytics batch. */
trait BoundedOperationalEventSource[F[_]] {
  def read(spark: SparkSession, manifest: AnalyticsRunManifest): F[DataFrame]
  def verifyOffsets(frame: DataFrame, manifest: AnalyticsRunManifest): F[Unit]
}
