package com.example.hiring.analytics.config

import scala.concurrent.duration.*

final case class AnalyticsRetentionSettings(
    bronzeDays: Int,
    quarantineDays: Int,
    silverDays: Int,
    publishedSnapshotDays: Int,
    deletionMarkerDays: Int,
    deltaVacuumSafetyDays: Int,
    deltaLogRetentionDays: Int
) {
  def deltaVacuumSafetyCheckEnabled: Boolean =
    deltaVacuumSafetyDays >= AnalyticsRetentionSettings.DeltaMinimumSafeVacuumDays
}

object AnalyticsRetentionSettings {
  val DeltaMinimumSafeVacuumDays: Int = 7
}

final case class AnalyticsOperationalSettings(
    retention: AnalyticsRetentionSettings,
    reportReservationTtlDays: Int,
    mongoTransactionWindowSeconds: Int,
    maximumErasureEvidenceFiles: Int,
    mongoPublisherBufferSize: Int
) {
  def reportReservationTtl: FiniteDuration = reportReservationTtlDays.toLong.days
  def mongoTransactionWindow: FiniteDuration = mongoTransactionWindowSeconds.toLong.seconds
}

object AnalyticsOperationalSettings {
  val MaximumMongoPublisherBufferSize: Int = 65536
}
