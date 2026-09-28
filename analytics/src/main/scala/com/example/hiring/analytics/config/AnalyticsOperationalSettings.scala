package com.example.hiring.analytics.config

import scala.concurrent.duration.*
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.numeric.Positive

type AnalyticsPositiveInt = Int :| Positive

final case class AnalyticsRetentionSettings(
    bronzeDays: AnalyticsPositiveInt,
    quarantineDays: AnalyticsPositiveInt,
    silverDays: AnalyticsPositiveInt,
    publishedSnapshotDays: AnalyticsPositiveInt,
    deletionMarkerDays: AnalyticsPositiveInt,
    deltaVacuumSafetyDays: AnalyticsPositiveInt,
    deltaLogRetentionDays: AnalyticsPositiveInt
) {
  def deltaVacuumSafetyCheckEnabled: Boolean =
    deltaVacuumSafetyDays.value >= AnalyticsRetentionSettings.DeltaMinimumSafeVacuumDays
}

object AnalyticsRetentionSettings {
  val DeltaMinimumSafeVacuumDays: Int = 7
}

final case class AnalyticsOperationalSettings(
    retention: AnalyticsRetentionSettings,
    reportReservationTtl: FiniteDuration,
    mongoTransactionWindow: FiniteDuration,
    maximumErasureEvidenceFiles: Int,
    mongoPublisherBufferSize: Int
)

object AnalyticsOperationalSettings {
  val MaximumMongoPublisherBufferSize: Int = 65536
}
