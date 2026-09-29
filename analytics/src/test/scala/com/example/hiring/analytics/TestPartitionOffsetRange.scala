package com.example.hiring.analytics

import com.example.hiring.analytics.domain.PartitionOffsetRange

/** Checked fixture construction; invalid fixture data fails at the test boundary. */
private[analytics] object TestPartitionOffsetRange {
  def unsafe(topic: String, partition: Int, startOffset: Long, endOffsetExclusive: Long): PartitionOffsetRange =
    PartitionOffsetRange
      .from(topic, partition, startOffset, endOffsetExclusive)
      .toEither
      .fold(
        errors => throw new IllegalArgumentException(errors.toNonEmptyList.toList.mkString("; ")),
        identity
      )
}
