package com.example.hiring.analytics.service.batch

import com.example.hiring.analytics.domain.{AnalyticsReportOutput, AnalyticsRunManifest, SubjectToken}

import java.time.Instant

enum AnalyticsManifestStatus(val persistedName: String) {
  case Started extends AnalyticsManifestStatus("STARTED")
  case QualityBlocked extends AnalyticsManifestStatus("QUALITY_BLOCKED")
  case ErasurePending extends AnalyticsManifestStatus("ERASURE_PENDING")
  case Published extends AnalyticsManifestStatus("PUBLISHED")
}

trait ActiveDeletionMarkerSource[F[_]] {
  def activeSubjectTokens: F[Vector[SubjectToken]]
}

trait AnalyticsRunManifestStore[F[_]] {
  def persist(manifest: AnalyticsRunManifest, status: AnalyticsManifestStatus, updatedAt: Instant): F[Unit]
}

final case class AnalyticsBatchResult(
    bronzeRecords: Long,
    validRecords: Long,
    suppressedRecords: Long,
    quarantinedRecords: Long,
    conflictingEventIds: Long
)

/** Lakehouse operations used by batch orchestration. Spark frames and sessions remain adapter details. */
trait AnalyticsBatchLakehouse[F[_]] {
  def validateHmacConfiguration: F[Unit]
  def prepare(manifest: AnalyticsRunManifest, activeTokens: Vector[SubjectToken]): F[AnalyticsBatchResult]
  def rebuildGoldAndExtractReport(asOf: Instant): F[AnalyticsReportOutput]
}
