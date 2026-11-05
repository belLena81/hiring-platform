package com.example.graphQL.cats.acceptance

import cats.effect.IO
import com.example.hiring.testing.{KafkaTestNamespace, LocalTestServices}
import io.circe.Json
import io.circe.parser.parse
import java.time.Duration
import java.util.{Collections, Properties, UUID}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import scala.jdk.CollectionConverters.*

/** Reads the published operational events from the isolated test topic with the analytics reader's credentials. */
object OperationalEventTopic {
  private val reader = "analytics_reader"
  private val readTimeout = Duration.ofMillis(500)

  /** Every committed event on the topic from its start, as the parsed envelope. */
  def readAll(namespace: KafkaTestNamespace.Namespace): IO[Vector[Json]] = IO.blocking {
    val manifest = namespace.manifest
    val properties = new Properties()
    properties.put("bootstrap.servers", manifest.kafkaBootstrap)
    properties.put("group.id", s"hiring-story-acceptance-${UUID.randomUUID()}")
    properties.put("key.deserializer", classOf[StringDeserializer].getName)
    properties.put("value.deserializer", classOf[ByteArrayDeserializer].getName)
    LocalTestServices.saslSettings(reader, manifest.readerPassword).foreach { case (key, value) =>
      properties.put(key, value)
    }
    properties.put("isolation.level", "read_committed")
    properties.put("enable.auto.commit", "false")
    val consumer = new KafkaConsumer[String, Array[Byte]](properties)
    val partition = new TopicPartition(namespace.events, 0)
    try {
      consumer.assign(Collections.singleton(partition))
      consumer.seekToBeginning(Collections.singleton(partition))
      val end = consumer.endOffsets(Collections.singleton(partition)).get(partition).longValue()
      def drain(collected: Vector[Json]): Vector[Json] =
        if (consumer.position(partition) >= end) collected
        else {
          val records = consumer.poll(readTimeout).records(partition).asScala.toVector
          drain(
            collected ++ records.map(record =>
              parse(new String(record.value(), java.nio.charset.StandardCharsets.UTF_8)).getOrElse(Json.Null)
            )
          )
        }
      drain(Vector.empty)
    } finally consumer.close()
  }
}
