package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.config.KafkaSaslSecurityProtocol
import com.example.graphQL.cats.service.port.InterviewRetentionBarrier
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import java.util.Properties
import scala.jdk.CollectionConverters.*

/** Physical log barriers, captured only after durable subject fences and broker-confirmed producer fencing. */
object InterviewKafkaRetention {
  def resource(
      bootstrapServers: String,
      username: String,
      password: String,
      protocol: KafkaSaslSecurityProtocol
  ): Resource[IO, InterviewKafkaRetention] = {
    val properties = new Properties()
    properties.put("bootstrap.servers", bootstrapServers)
    properties.put("key.deserializer", classOf[StringDeserializer].getName)
    properties.put("value.deserializer", classOf[ByteArrayDeserializer].getName)
    properties.put("enable.auto.commit", "false")
    properties.put("request.timeout.ms", "10000")
    properties.put("default.api.timeout.ms", "10000")
    OperationalEventKafkaRuntime
      .saslProperties(Some(username), Some(password), protocol)
      .foreach { case (key, value) => properties.put(key, value) }
    Resource
      .make(IO.blocking(new KafkaConsumer[String, Array[Byte]](properties)))(consumer => IO.blocking(consumer.close()))
      .map(new InterviewKafkaRetention(_))
  }
}
final class InterviewKafkaRetention private[kafka] (consumer: KafkaConsumer[String, Array[Byte]]) {
  def capture: IO[Vector[InterviewRetentionBarrier]] = IO.blocking {
    Vector(InterviewMessageCodec.CommandsTopic, InterviewMessageCodec.ResultsTopic).flatMap { topic =>
      val partitions =
        consumer.partitionsFor(topic).asScala.map(info => new TopicPartition(topic, info.partition())).toVector
      val offsets = consumer.endOffsets(partitions.asJava)
      partitions.map(partition => InterviewRetentionBarrier(topic, partition.partition(), offsets.get(partition)))
    }
  }
  def passed(barriers: Vector[InterviewRetentionBarrier]): IO[Boolean] = IO.blocking {
    val partitions = barriers.map(value => new TopicPartition(value.topic, value.partition))
    val beginnings = consumer.beginningOffsets(partitions.asJava)
    barriers.forall(value => beginnings.get(new TopicPartition(value.topic, value.partition)) >= value.endOffset)
  }
}
