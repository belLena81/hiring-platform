package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.IO
import fs2.kafka.CommittableOffset
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition

/** Committable offsets whose commit is an observable function, for specs that never start a Kafka client. */
private[kafka] object KafkaFixtures {
  def offset(topic: String, partition: Int, position: Long, group: String)(
      commit: Map[TopicPartition, OffsetAndMetadata] => IO[Unit]
  ): CommittableOffset[IO] =
    CommittableOffset[IO](
      new TopicPartition(topic, partition),
      new OffsetAndMetadata(position + 1),
      Some(group),
      commit
    )
}
