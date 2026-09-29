package com.example.hiring.analytics.config

import AnalyticsPositiveInt.*

import scala.concurrent.duration.*
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.numeric.{Interval, Positive}

type AnalyticsPositiveInt = Int :| Positive
type MaximumErasureEvidenceFiles = Int :| Interval.Closed[1, 2147483646]
type MongoPublisherBufferSize = Int :| Interval.Closed[1, 65536]

object MaximumErasureEvidenceFiles {
  def unwrap(value: MaximumErasureEvidenceFiles): Int = value.asInstanceOf[Int]
}

object MongoPublisherBufferSize {
  def unwrap(value: MongoPublisherBufferSize): Int = value.asInstanceOf[Int]
}

object AnalyticsPositiveInt {
  extension (value: AnalyticsPositiveInt) def value: Int = value
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

object AnalyticsOperationalSettings {
  val MaximumMongoPublisherBufferSize: Int = 65536
}
