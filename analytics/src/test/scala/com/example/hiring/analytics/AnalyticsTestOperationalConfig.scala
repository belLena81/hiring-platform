package com.example.hiring.analytics

import com.example.hiring.analytics.adapter.mongo.MongoPublisherStream
import com.example.hiring.analytics.config.{
  AnalyticsErasureWorkerTimings,
  AnalyticsOperationalSettings,
  AnalyticsPositiveInt,
  AnalyticsRetentionSettings,
  MaximumErasureEvidenceFiles,
  MongoPublisherBufferSize,
  StageTimesRetentionDays
}
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.numeric.Positive
import scala.concurrent.duration.*

private[analytics] object AnalyticsTestOperationalConfig {
  private def positive(value: Int): AnalyticsPositiveInt =
    value.refineUnsafe[Positive]

  private def stageDays(value: Int): StageTimesRetentionDays =
    value.refineUnsafe[io.github.iltotore.iron.constraint.numeric.Interval.Closed[1, 3650]]

  private def maximumEvidence(value: Int): MaximumErasureEvidenceFiles =
    value.refineUnsafe[io.github.iltotore.iron.constraint.numeric.Interval.Closed[1, 2147483646]]

  private def publisherBuffer(value: Int): MongoPublisherBufferSize =
    value.refineUnsafe[io.github.iltotore.iron.constraint.numeric.Interval.Closed[1, 65536]]

  val operational: AnalyticsOperationalSettings = AnalyticsOperationalSettings(
    AnalyticsRetentionSettings(
      bronzeDays = positive(7),
      quarantineDays = positive(7),
      silverDays = positive(30),
      stageTimesTerminalDays = stageDays(30),
      stageTimesOpenDays = stageDays(180),
      publishedSnapshotDays = positive(30),
      deletionMarkerDays = positive(31),
      deltaVacuumSafety = 7.days,
      deltaLogRetention = 14.days,
      erasureCompletionBudget = 21.days,
      kafkaRetention = 7.days,
      kafkaSegmentRoll = 7.days
    ),
    reportReservationTtl = 90.days,
    mongoTransactionWindow = 120.seconds,
    maximumErasureEvidenceFiles = maximumEvidence(100000),
    mongoPublisherBufferSize = publisherBuffer(256),
    erasureWorkerTimings = AnalyticsErasureWorkerTimings(90.seconds, 30.seconds, 5.seconds, 5.minutes, 1.hour)
  )

  def streams: MongoPublisherStream = new MongoPublisherStream(operational)
}
