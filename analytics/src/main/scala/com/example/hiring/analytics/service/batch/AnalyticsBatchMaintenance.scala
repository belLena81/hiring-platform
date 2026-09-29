package com.example.hiring.analytics.service.batch

import com.example.hiring.analytics.domain.SubjectToken

import java.time.Instant

trait AnalyticsBatchMaintenance[F[_]] {
  def validateHmacConfigurationLocked: F[Unit]
  def configureRawTables: F[Unit]
  def applyActiveDeletions(markerTokens: Vector[SubjectToken]): F[Unit]
  def expireStored(at: Instant): F[Unit]
}
