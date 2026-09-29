package com.example.hiring.analytics.config

import AnalyticsPositiveInt.*

import scala.concurrent.duration.*
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.numeric.Positive

type AnalyticsPositiveInt = Int :| Positive

object AnalyticsPositiveInt {
  extension (value: AnalyticsPositiveInt) def value: Int = value.asInstanceOf[Int]
}

final case class AnalyticsRetentionSettings(
    bronzeDays: AnalyticsPositiveInt,
    quarantineDays: AnalyticsPositiveInt,
    silverDays: AnalyticsPositiveInt,
    publishedSnapshotDays: AnalyticsPositiveInt,
    deletionMarkerDays: AnalyticsPositiveInt,
    deltaVacuumSafety: FiniteDuration,
    deltaLogRetention: FiniteDuration
) {
  def deltaVacuumSafetyCheckEnabled: Boolean =
    deltaVacuumSafety >= AnalyticsRetentionSettings.DeltaMinimumSafeVacuum
}

object AnalyticsRetentionSettings {
  val DeltaMinimumSafeVacuum: FiniteDuration = 7.days
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
