package com.example.hiring.analytics

/** The analytics retention policy is intentionally separate from Kafka retention. */
object AnalyticsRetention {
  val BronzeDays: Int = 7
  val QuarantineDays: Int = 7
  val SilverDays: Int = 30
  val GoldDays: Int = 30
  val PublishedSnapshotDays: Int = 30
  val DeletionMarkerDays: Int = 31
  val MinimumContributors: Long = 10L
}

final case class PartitionOffsetRange(
    topic: String,
    partition: Int,
    startOffset: Long,
    endOffsetExclusive: Long
) {
  require(topic.nonEmpty, "topic must be non-empty")
  require(partition >= 0, "partition must be non-negative")
  require(startOffset >= 0, "start offset must be non-negative")
  require(endOffsetExclusive >= startOffset, "end offset must not precede start offset")
}

final case class AnalyticsRunManifest(runId: String, offsetRanges: Vector[PartitionOffsetRange]) {
  require(runId.nonEmpty, "run id must be non-empty")
  require(
    offsetRanges.map(range => (range.topic, range.partition)).distinct.size == offsetRanges.size,
    "each topic partition may occur only once"
  )
}
