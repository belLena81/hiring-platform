package com.example.hiring.analytics.service.batch
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import cats.effect.IO
import com.example.hiring.analytics.errors.AnalyticsError

import java.time.Instant

/** Durable publication boundary shared with the operational report reader. */
trait AnalyticsReportPublisher {
  def reserve(runId: String, rangeFingerprint: String, now: java.time.Instant): IO[AnalyticsReportReservation]
  def publish(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: java.time.Instant
  ): IO[Unit]
  def publishErasure(
      reservation: AnalyticsReportReservation,
      report: AnalyticsReportOutput,
      expiresAt: java.time.Instant,
      claim: ErasureClaim,
      completedAt: Instant
  ): IO[Unit] =
    IO.raiseError(AnalyticsError.InvalidConfiguration("guarded erasure publication is not configured"))
}

final case class AnalyticsReportReservation(runId: String, rangeFingerprint: String, generation: Long, revision: Long)

object AnalyticsReportPublisher {
  val unavailable: AnalyticsReportPublisher = new AnalyticsReportPublisher {
    override def reserve(
        runId: String,
        rangeFingerprint: String,
        now: java.time.Instant
    ): IO[AnalyticsReportReservation] =
      IO.raiseError(AnalyticsError.InvalidConfiguration("analytics report publisher is not configured"))

    override def publish(
        reservation: AnalyticsReportReservation,
        report: AnalyticsReportOutput,
        expiresAt: java.time.Instant
    ): IO[Unit] = IO.raiseError(AnalyticsError.InvalidConfiguration("analytics report publisher is not configured"))

    override def publishErasure(
        reservation: AnalyticsReportReservation,
        report: AnalyticsReportOutput,
        expiresAt: java.time.Instant,
        claim: ErasureClaim,
        completedAt: java.time.Instant
    ): IO[Unit] = IO.raiseError(AnalyticsError.InvalidConfiguration("analytics erasure publisher is not configured"))
  }
}
