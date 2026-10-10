package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.config.KafkaSaslSecurityProtocol
import com.example.graphQL.cats.domain.workflow.InterviewTopicPair
import com.example.graphQL.cats.service.port.InterviewRetentionBarrier
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.jdk.CollectionConverters.*

/** Physical log barriers, captured only after durable subject fences and broker-confirmed producer fencing. Every
  * operation owns its consumer, because a Kafka consumer must not be shared between threads.
  */
final class InterviewKafkaRetention private[kafka] (
    open: Resource[IO, KafkaConsumer[String, Array[Byte]]],
    topics: InterviewTopicPair
) {
  def capture: IO[Vector[InterviewRetentionBarrier]] = open.use(consumer =>
    IO.blocking {
      Vector(topics.commands, topics.results).flatMap { topic =>
        val partitions =
          consumer.partitionsFor(topic).asScala.map(info => new TopicPartition(topic, info.partition())).toVector
        val offsets = consumer.endOffsets(partitions.asJava)
        partitions.map(partition => InterviewRetentionBarrier(topic, partition.partition(), offsets.get(partition)))
      }
    }
  )

  def passed(barriers: Vector[InterviewRetentionBarrier]): IO[Boolean] = open.use(consumer =>
    IO.blocking {
      val partitions = barriers.map(value => new TopicPartition(value.topic, value.partition))
      val beginnings = consumer.beginningOffsets(partitions.asJava)
      barriers.forall(value => beginnings.get(new TopicPartition(value.topic, value.partition)) >= value.endOffset)
    }
  )
}

object InterviewKafkaRetention {
  def apply(
      bootstrapServers: String,
      username: String,
      password: String,
      protocol: KafkaSaslSecurityProtocol,
      topics: InterviewTopicPair = InterviewTopicPair.Default
  ): InterviewKafkaRetention = {
    val properties = KafkaClientSettings.clientProperties(bootstrapServers, Some(username), Some(password), protocol)
    properties.setProperty("key.deserializer", classOf[StringDeserializer].getName)
    properties.setProperty("value.deserializer", classOf[ByteArrayDeserializer].getName)
    properties.setProperty("enable.auto.commit", "false")
    new InterviewKafkaRetention(
      Resource.make(IO.blocking(new KafkaConsumer[String, Array[Byte]](properties)))(consumer =>
        IO.blocking(consumer.close())
      ),
      topics
    )
  }
}
