package com.example.hiring.analytics.service.batch

import com.example.hiring.analytics.domain.SubjectToken

import java.time.Instant

trait AnalyticsBatchMaintenance[F[_]] {
  def validateHmacConfigurationLocked: F[Unit]

  /** Creates or validates the subject-attributed tables. The first initialization of the application stage times
    * backfills them from Silver, leaving out every application that involves an active deletion marker.
    */
  def configureRawTables(markerTokens: Vector[SubjectToken]): F[Unit]
  def applyActiveDeletions(markerTokens: Vector[SubjectToken]): F[Unit]
  def expireStored(at: Instant): F[Unit]
}
