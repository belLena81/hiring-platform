package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorization

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import java.time.Instant

/** Capabilities shared by Spark-backed stages; each stage receives only the subset it needs. */
private[analytics] trait SparkExecution[F[_]] {

  /** Evaluates Spark work on its owned execution context with a cancelable Spark job group. */
  def apply[A](work: => A): F[A]
  def either[A](work: => Either[AnalyticsError, A]): F[A]
}

private[analytics] trait DeltaWriter[F[_]] {
  def merge(source: DataFrame, path: String, condition: String): F[Unit]
  def mergeWhenFresh(source: DataFrame, path: String, condition: String, at: () => Instant): F[Unit]
  def withExpiry(frame: DataFrame, now: Instant, days: Int): DataFrame
}

private[analytics] trait DeltaReader[F[_]] {
  def readOrEmpty(spark: SparkSession, path: String, schema: StructType): F[DataFrame]
}

private[analytics] trait QuarantineId {
  def apply(): Column
}

private[analytics] trait KeyRetirementLookup[F[_]] {
  def list(lakehouseRoot: String): F[Vector[HmacKeyRetirementAuthorization]]
}

private[analytics] final case class AnalyticsBronzeInput(
    frame: DataFrame,
    startedAt: Instant,
    records: Long,
    validRecords: Long,
    malformedRecords: Long
)

private[analytics] final case class AnalyticsPreparedEvents(
    incomingSilver: DataFrame,
    conflicts: DataFrame,
    validRecords: Long,
    suppressedRecords: Long,
    quarantinedRecords: Long,
    conflictingEventIds: Long
)
