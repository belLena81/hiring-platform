package com.example.hiring.analytics.service.batch

import com.example.hiring.analytics.domain.{AnalyticsReportOutput, RangeFingerprint, RunId}
import com.example.hiring.analytics.service.erasure.ErasureClaim

import java.time.Instant

/** Durable publication boundary shared with the operational report reader. */
trait AnalyticsReportPublisher[F[_]] {
  def reserve(runId: RunId, rangeFingerprint: RangeFingerprint, now: Instant): F[AnalyticsReportReservation]
  def publish(reservation: AnalyticsReportReservation, report: AnalyticsReportOutput, expiresAt: Instant): F[Unit]
  def publishErasure(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: Instant,
      claim: ErasureClaim,
      completedAt: Instant
  ): F[Unit]
}

final case class AnalyticsReportReservation(
    runId: RunId,
    rangeFingerprint: RangeFingerprint,
    generation: Long,
    revision: Long
)
