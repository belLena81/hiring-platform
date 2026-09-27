package com.example.hiring.analytics.batch

import com.example.hiring.analytics.{
  AnalyticsError,
  AnalyticsRunManifest,
  HmacKeyRetirementAuthorization,
  SubjectPseudonymizer
}

import cats.effect.IO
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

import java.time.Instant

private[batch] trait AnalyticsBatchBlocking {
  def apply[A](work: => A): IO[A]
  def either[A](work: => Either[AnalyticsError, A]): IO[A]
}

private[batch] final case class AnalyticsBatchStageRuntime(
    paths: AnalyticsLakehousePaths,
    pseudonymizer: SubjectPseudonymizer,
    blocking: AnalyticsBatchBlocking,
    currentTime: () => IO[Instant],
    persistManifest: (SparkSession, AnalyticsRunManifest, String, String) => IO[Unit],
    merge: (DataFrame, String, String) => IO[Unit],
    readOrEmpty: (SparkSession, String, StructType) => IO[DataFrame],
    withExpiry: (DataFrame, Instant, Int) => DataFrame,
    quarantineId: () => Column,
    configureRawTablePrivacy: SparkSession => IO[Unit],
    maximumErasureEvidenceFiles: Int,
    retirementAuthorizations: String => IO[Vector[HmacKeyRetirementAuthorization]]
)

private[batch] final case class AnalyticsBronzeInput(frame: DataFrame, startedAt: Instant, records: Long)

private[batch] final case class AnalyticsPreparedEvents(
    incomingSilver: DataFrame,
    conflicts: DataFrame,
    validRecords: Long,
    suppressedRecords: Long,
    quarantinedRecords: Long,
    conflictingEventIds: Long
)
