package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.mongo.MongoPublisherStream
import com.example.hiring.analytics.config.{AnalyticsOperationalSettings, AnalyticsRetentionSettings}

private[analytics] object AnalyticsTestOperationalConfig {
  val operational: AnalyticsOperationalSettings = AnalyticsOperationalSettings(
    AnalyticsRetentionSettings(
      bronzeDays = 7,
      quarantineDays = 7,
      silverDays = 30,
      publishedSnapshotDays = 30,
      deletionMarkerDays = 31,
      deltaVacuumSafetyDays = 7,
      deltaLogRetentionDays = 30
    ),
    reportReservationTtlDays = 90,
    mongoTransactionWindowSeconds = 120,
    maximumErasureEvidenceFiles = 100000,
    mongoPublisherBufferSize = 256
  )

  def streams: MongoPublisherStream = new MongoPublisherStream(operational)
}
