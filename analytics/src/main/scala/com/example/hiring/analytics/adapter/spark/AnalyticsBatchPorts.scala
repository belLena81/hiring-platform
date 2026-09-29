package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.AnalyticsRunManifest

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Supplies one explicit bounded Kafka range to the analytics batch. */
trait BoundedOperationalEventSource[F[_]] {
  def read(spark: SparkSession, manifest: AnalyticsRunManifest): F[DataFrame]
  def verifyOffsets(frame: DataFrame, manifest: AnalyticsRunManifest): F[Unit]
}

/** Supplies only active HMAC subject tokens, independently of MongoDB. */
trait ActiveDeletionMarkerSource[F[_]] {
  def activeSubjectTokens(spark: SparkSession): F[DataFrame]
}

trait ManifestStore[F[_]] {
  def persist(spark: SparkSession, manifest: AnalyticsRunManifest, status: String, updatedAt: String): F[Unit]
}
