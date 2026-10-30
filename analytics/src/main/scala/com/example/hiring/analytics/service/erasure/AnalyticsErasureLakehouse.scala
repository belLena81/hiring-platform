package com.example.hiring.analytics.service.erasure

import com.example.hiring.analytics.domain.{AnalyticsReportOutput, SubjectToken}

import java.time.Instant

/** Delta capabilities used by erasure orchestration; Spark sessions and frames stay in the adapter. */
trait AnalyticsErasureLakehouse[F[_]] {
  def validateHmacConfiguration: F[Unit]
  def reclaimRetainedFiles: F[Long]

  /** When the latest table compaction ran. `OPTIMIZE` replaces data files but leaves the replaced ones, with their
    * subject tokens, until VACUUM, so the data-file erasure horizon cannot start before it.
    */
  def lastCompactionAt: F[Option[Instant]]
  def verifyMarkedSubjectsAbsent(markerTokens: Vector[SubjectToken]): F[Unit]
  def countMarkedRows(markerTokens: Vector[SubjectToken]): F[Long]
  def captureMarkedFiles(markerTokens: Vector[SubjectToken]): F[Vector[String]]
  def checkpointPurgedRawLogs: F[Vector[String]]
  def verifyFilesAbsent(files: Vector[String]): F[Unit]
  def applyDeletionMarkers(markerTokens: Vector[SubjectToken]): F[Unit]
  def rebuildGoldAndExtractReport(asOf: Instant): F[AnalyticsReportOutput]
}
