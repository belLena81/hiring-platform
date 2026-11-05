package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.IO
import fs2.kafka.CommittableOffset
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

/** A committable offset whose commit is an observable effect, for drills that exercise the acknowledgment step. */
private[graphQL] object CommitProbeOffsets {
  def offset(topic: String, partition: Int, position: Long)(onCommit: IO[Unit]): CommittableOffset[IO] =
    CommittableOffset[IO](
      new TopicPartition(topic, partition),
      new OffsetAndMetadata(position + 1),
      Some("commit-probe"),
      _ => onCommit
    )
}
