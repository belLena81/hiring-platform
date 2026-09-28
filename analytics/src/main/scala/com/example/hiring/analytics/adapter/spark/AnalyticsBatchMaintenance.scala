package com.example.hiring.analytics.adapter.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Instant

/** Shared Delta maintenance operations needed while coordinating a batch publication. */
private[analytics] trait AnalyticsBatchMaintenance[F[_]] {
  def validateHmacConfigurationLocked(spark: SparkSession): F[Unit]
  def configureRawTables(spark: SparkSession): F[Unit]
  def applyActiveDeletions(spark: SparkSession, markerTokens: DataFrame): F[Unit]
  def expireStored(spark: SparkSession, at: Instant): F[Unit]
}
