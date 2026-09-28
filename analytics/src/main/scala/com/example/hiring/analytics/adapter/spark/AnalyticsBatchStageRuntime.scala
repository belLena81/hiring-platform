package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.keyretirement.HmacKeyRetirementAuthorization
import com.example.hiring.analytics.service.batch.{AnalyticsLakehousePaths, ManifestStore}
import cats.effect.Async
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import java.time.Instant

/** The bounded maintenance surface consumed by erasure orchestration. */
private[analytics] trait AnalyticsErasureLakehouse[F[_]] {
  def validateHmacConfiguration(spark: SparkSession): F[Unit]
  def reclaimRetainedFiles(spark: SparkSession): F[Long]
  def verifyMarkedSubjectsAbsent(spark: SparkSession, markerTokens: DataFrame): F[Unit]
  def countMarkedRows(spark: SparkSession, markerTokens: DataFrame): F[Long]
  def captureMarkedFiles(spark: SparkSession, markerTokens: DataFrame): F[Vector[String]]
  def checkpointPurgedRawLogs(spark: SparkSession): F[Vector[String]]
  def verifyFilesAbsent(spark: SparkSession, files: Vector[String]): F[Unit]
  def checkpointRawTableLogs(spark: SparkSession): F[Unit]
  def purgeMarkedSubjectRows(spark: SparkSession, path: String, markerTokens: DataFrame): F[Unit]
  def applyDeletionMarkers(spark: SparkSession, markerTokens: DataFrame): F[Unit]
  def rebuildGoldAndExtractReport(spark: SparkSession, asOf: Instant): F[AnalyticsReportOutput]
}

/** Capabilities shared by Spark-backed stages; each stage receives only the subset it needs. */
private[analytics] trait SparkExecution[F[_]] {
  def apply[A](work: => A): F[A]
  def either[A](work: => Either[AnalyticsError, A]): F[A]
}

private[spark] trait DeltaWriter[F[_]] {
  def merge(source: DataFrame, path: String, condition: String): F[Unit]
  def withExpiry(frame: DataFrame, now: Instant, days: Int): DataFrame
}

private[spark] trait DeltaReader[F[_]] {
  def readOrEmpty(spark: SparkSession, path: String, schema: StructType): F[DataFrame]
}

private[spark] trait QuarantineId {
  def apply(): Column
}

private[analytics] trait KeyRetirementLookup[F[_]] {
  def list(lakehouseRoot: String): F[Vector[HmacKeyRetirementAuthorization]]
}

private[spark] final case class IngestionStagePorts[F[_]](
    paths: AnalyticsLakehousePaths,
    pseudonymizer: SubjectPseudonymizer,
    execution: SparkExecution[F],
    manifestStore: ManifestStore[F],
    deltaWriter: DeltaWriter[F],
    clock: cats.effect.Clock[F]
)

private[spark] final case class SilverStagePorts[F[_]](
    paths: AnalyticsLakehousePaths,
    pseudonymizer: SubjectPseudonymizer,
    execution: SparkExecution[F],
    deltaWriter: DeltaWriter[F],
    deltaReader: DeltaReader[F],
    quarantineId: QuarantineId
)

private[analytics] final case class KeyContinuityStagePorts[F[_]](
    paths: AnalyticsLakehousePaths,
    pseudonymizer: SubjectPseudonymizer,
    execution: SparkExecution[F],
    clock: cats.effect.Clock[F],
    retirementAuthorizations: KeyRetirementLookup[F]
)

private[spark] final case class ErasureStagePorts[F[_]](
    paths: AnalyticsLakehousePaths,
    execution: SparkExecution[F],
    configureRawTablePrivacy: SparkSession => F[Unit],
    maximumEvidenceFiles: Int
)

private[spark] final case class AnalyticsBronzeInput(frame: DataFrame, startedAt: Instant, records: Long)

private[spark] final case class AnalyticsPreparedEvents(
    incomingSilver: DataFrame,
    conflicts: DataFrame,
    validRecords: Long,
    suppressedRecords: Long,
    quarantinedRecords: Long,
    conflictingEventIds: Long
)
