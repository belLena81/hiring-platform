package com.example.hiring.analytics.config

import java.time.Instant
import java.time.temporal.ChronoUnit
import scala.concurrent.duration.*
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.numeric.{Interval, Positive}

type AnalyticsPositiveInt = Int :| Positive
type MaximumErasureEvidenceFiles = Int :| Interval.Closed[1, 2147483646]
type MongoPublisherBufferSize = Int :| Interval.Closed[1, 65536]

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

  /** Calendar-day arithmetic on `Instant` avoids the ~292-year `FiniteDuration` ceiling for large day counts. */
  def publishedSnapshotExpiry(from: Instant): Instant = from.plus(publishedSnapshotDays.toLong, ChronoUnit.DAYS)
  def deletionMarkerCutoff(observedAt: Instant): Instant =
    observedAt.minus(deletionMarkerDays.toLong, ChronoUnit.DAYS)
}

object AnalyticsRetentionSettings {
  val DeltaMinimumSafeVacuum: FiniteDuration = 7.days
}

final case class AnalyticsOperationalSettings(
    retention: AnalyticsRetentionSettings,
    reportReservationTtl: FiniteDuration,
    mongoTransactionWindow: FiniteDuration,
    maximumErasureEvidenceFiles: MaximumErasureEvidenceFiles,
    mongoPublisherBufferSize: MongoPublisherBufferSize,
    erasureWorkerTimings: AnalyticsErasureWorkerTimings
)

final case class AnalyticsErasureWorkerTimings(
    leaseDuration: FiniteDuration,
    deliveryTimeout: FiniteDuration,
    pollInterval: FiniteDuration
)

final case class AnalyticsErasureWorkerPolicy(
    retention: AnalyticsRetentionSettings,
    timings: AnalyticsErasureWorkerTimings
)
