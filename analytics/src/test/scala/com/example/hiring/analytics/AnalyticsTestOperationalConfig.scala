package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.mongo.MongoPublisherStream
import com.example.hiring.analytics.config.{
  AnalyticsErasureWorkerTimings,
  AnalyticsOperationalSettings,
  AnalyticsPositiveInt,
  AnalyticsRetentionSettings,
  MaximumErasureEvidenceFiles,
  MongoPublisherBufferSize
}
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.numeric.Positive
import scala.concurrent.duration.*

private[analytics] object AnalyticsTestOperationalConfig {
  private def positive(value: Int): AnalyticsPositiveInt =
    value.refineUnsafe[Positive]

  private def maximumEvidence(value: Int): MaximumErasureEvidenceFiles =
    value.refineUnsafe[io.github.iltotore.iron.constraint.numeric.Interval.Closed[1, 2147483646]]

  private def publisherBuffer(value: Int): MongoPublisherBufferSize =
    value.refineUnsafe[io.github.iltotore.iron.constraint.numeric.Interval.Closed[1, 65536]]

  val operational: AnalyticsOperationalSettings = AnalyticsOperationalSettings(
    AnalyticsRetentionSettings(
      bronzeDays = positive(7),
      quarantineDays = positive(7),
      silverDays = positive(30),
      publishedSnapshotDays = positive(30),
      deletionMarkerDays = positive(31),
      deltaVacuumSafety = 7.days,
      deltaLogRetention = 30.days
    ),
    reportReservationTtl = 90.days,
    mongoTransactionWindow = 120.seconds,
    maximumErasureEvidenceFiles = maximumEvidence(100000),
    mongoPublisherBufferSize = publisherBuffer(256),
    erasureWorkerTimings = AnalyticsErasureWorkerTimings(90.seconds, 30.seconds, 5.seconds)
  )

  def streams: MongoPublisherStream = new MongoPublisherStream(operational)
}
