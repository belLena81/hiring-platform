package com.example.hiring.analytics

import cats.data.ValidatedNec
import cats.syntax.all.*
import com.example.hiring.analytics.domain.{AnalyticsRunManifest, AnalyticsTopic, PartitionOffsetRange, RunId}

/** Checked fixture construction; invalid fixture data fails at the test boundary. */
private[analytics] object TestPartitionOffsetRange {
  def from(
      topic: String,
      partition: Int,
      startOffset: Long,
      endOffsetExclusive: Long
  ): ValidatedNec[String, PartitionOffsetRange] =
    PartitionOffsetRange.fromValidatedTopic(
      AnalyticsTopic.from(topic).toValidatedNec,
      partition,
      startOffset,
      endOffsetExclusive
    )

  def unsafe(topic: String, partition: Int, startOffset: Long, endOffsetExclusive: Long): PartitionOffsetRange =
    from(topic, partition, startOffset, endOffsetExclusive).toEither.fold(
      errors => throw new IllegalArgumentException(errors.toNonEmptyList.toList.mkString("; ")),
      identity
    )

  def manifest(runId: RunId, ranges: Vector[PartitionOffsetRange]): ValidatedNec[String, AnalyticsRunManifest] =
    AnalyticsRunManifest.from(runId, ranges.validNec)

  def manifestOf(rawRunId: String, ranges: Vector[PartitionOffsetRange]): ValidatedNec[String, AnalyticsRunManifest] =
    RunId.from(rawRunId).toValidatedNec.andThen(manifest(_, ranges))
}
