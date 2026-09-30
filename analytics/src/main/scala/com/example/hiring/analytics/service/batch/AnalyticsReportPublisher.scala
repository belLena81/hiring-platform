package com.example.hiring.analytics.service.batch

import com.example.hiring.analytics.domain.{AnalyticsReportOutput, RangeFingerprint, RunId}
import com.example.hiring.analytics.service.erasure.ErasureClaim

import cats.Applicative
import java.time.Instant

/** Durable publication boundary shared with the operational report reader. */
trait AnalyticsReportPublisher[F[_]] {
  def reserve(runId: RunId, rangeFingerprint: RangeFingerprint, now: Instant): F[AnalyticsReportReservation]
  def publish(reservation: AnalyticsReportReservation, report: AnalyticsReportOutput, expiresAt: Instant): F[Unit]
  def publicationReceipt(reservation: AnalyticsReportReservation)(using
      applicative: Applicative[F]
  ): F[AnalyticsReportPublicationReceipt] = applicative.pure(AnalyticsReportPublicationReceipt.Absent)
  def publishErasure(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: Instant,
      claim: ErasureClaim,
      completedAt: Instant
  ): F[Unit]
}

enum AnalyticsReportPublicationReceipt {
  case Absent
  case CurrentGeneration
  case Superseded
}

final case class AnalyticsReportReservation(
    runId: RunId,
    rangeFingerprint: RangeFingerprint,
    generation: Long,
    revision: Long
)
