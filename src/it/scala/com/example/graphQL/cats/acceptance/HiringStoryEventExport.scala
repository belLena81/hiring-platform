package com.example.graphQL.cats.acceptance

import cats.effect.IO
import com.example.hiring.testing.{KafkaTestNamespace, LocalTestServices}
import io.circe.Json
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.time.Duration
import java.util.{Collections, Properties, UUID}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import scala.jdk.CollectionConverters.*

/** Hands the story's committed operational events (key and exact value, in topic order) to the analytics step.
  *
  * The analytics build is a separate sbt build with its own classpath, so the two steps meet in a file under the
  * ignored `.local/data/` tree. Nothing but the events the application published is written: no credentials, no
  * expected values.
  */
object HiringStoryEventExport {
  val Directory = "hiring-story-analytics"
  val FileName = "story-events.jsonl"

  def target(workspace: String): Path = Path.of(workspace).resolve(".local/data").resolve(Directory).resolve(FileName)

  /** Every committed record of partition 0 from its start, as one JSON line `{"key":..,"value":..}` each. */
  def write(namespace: KafkaTestNamespace.Namespace, fileName: String = FileName): IO[(Path, Int)] = IO.blocking {
    val manifest = namespace.manifest
    val properties = new Properties()
    properties.put("bootstrap.servers", manifest.kafkaBootstrap)
    properties.put("group.id", s"hiring-story-export-${UUID.randomUUID()}")
    properties.put("key.deserializer", classOf[StringDeserializer].getName)
    properties.put("value.deserializer", classOf[ByteArrayDeserializer].getName)
    properties.put("security.protocol", "SASL_PLAINTEXT")
    properties.put("sasl.mechanism", "PLAIN")
    properties.put("sasl.jaas.config", LocalTestServices.jaasConfig("analytics_reader", manifest.readerPassword))
    properties.put("isolation.level", "read_committed")
    properties.put("enable.auto.commit", "false")
    val consumer = new KafkaConsumer[String, Array[Byte]](properties)
    val partition = new TopicPartition(namespace.events, 0)
    try {
      consumer.assign(Collections.singleton(partition))
      consumer.seekToBeginning(Collections.singleton(partition))
      val end = consumer.endOffsets(Collections.singleton(partition)).get(partition).longValue()
      def drain(collected: Vector[String]): Vector[String] =
        if (consumer.position(partition) >= end) collected
        else {
          val records = consumer.poll(Duration.ofMillis(500)).records(partition).asScala.toVector
          drain(
            collected ++ records.map(record =>
              Json
                .obj(
                  "key" -> Json.fromString(record.key()),
                  "value" -> Json.fromString(new String(record.value(), StandardCharsets.UTF_8))
                )
                .noSpaces
            )
          )
        }
      val lines = drain(Vector.empty)
      val file = target(manifest.workspace).resolveSibling(fileName)
      Files.createDirectories(file.getParent)
      val _ = Files.writeString(
        file,
        lines.mkString("", "\n", "\n"),
        StandardOpenOption.CREATE,
        StandardOpenOption.TRUNCATE_EXISTING,
        StandardOpenOption.WRITE
      )
      (file, lines.size)
    } finally consumer.close()
  }
}
