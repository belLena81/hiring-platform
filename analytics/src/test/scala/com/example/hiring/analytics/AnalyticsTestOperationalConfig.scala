package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.mongo.MongoPublisherStream
import com.example.hiring.analytics.config.{
  AnalyticsOperationalSettings,
  AnalyticsPositiveInt,
  AnalyticsRetentionSettings
}
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.numeric.Positive
import scala.concurrent.duration.*

private[analytics] object AnalyticsTestOperationalConfig {
  private def positive(value: Int): AnalyticsPositiveInt =
    value.refineUnsafe[Positive]

  val operational: AnalyticsOperationalSettings = AnalyticsOperationalSettings(
    AnalyticsRetentionSettings(
      bronzeDays = positive(7),
      quarantineDays = positive(7),
      silverDays = positive(30),
      publishedSnapshotDays = positive(30),
      deletionMarkerDays = positive(31),
      deltaVacuumSafetyDays = positive(7),
      deltaLogRetentionDays = positive(30)
    ),
    reportReservationTtl = 90.days,
    mongoTransactionWindow = 120.seconds,
    maximumErasureEvidenceFiles = 100000,
    mongoPublisherBufferSize = 256
  )

  def streams: MongoPublisherStream = new MongoPublisherStream(operational)
}
