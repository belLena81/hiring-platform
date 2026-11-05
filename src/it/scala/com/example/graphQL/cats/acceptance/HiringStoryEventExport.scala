package com.example.graphQL.cats.acceptance

import cats.effect.IO
import com.example.hiring.testing.{KafkaTestNamespace, LocalTestServices}
import io.circe.Json
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardOpenOption}
import java.nio.file.attribute.PosixFilePermissions
import java.time.Duration
import java.util.{Collections, Properties, UUID}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import scala.jdk.CollectionConverters.*

/** Hands the story's committed operational events (key and exact value, in topic order) to the analytics step.
  *
  * The analytics build is a separate sbt build with its own classpath, so the two steps meet in a file under the
  * ignored `.local/data/` tree. The files hold the published events, which carry pseudonymous subject UUIDs (no names,
  * credentials or expected values); they are disposable and are deleted and rewritten by the next export. Starting an
  * export deletes the downstream report file of the same dataset, so an old report cannot satisfy a new run; the later
  * steps also compare the SHA-256 of the events file that the report was built from.
  */
object HiringStoryEventExport {
  val Directory = "hiring-story-analytics"
  val FileName = "story-events.jsonl"

  def target(workspace: String): Path = Path.of(workspace).resolve(".local/data").resolve(Directory).resolve(FileName)

  /** Every committed record of partition 0 from its start, as one JSON line `{"key":..,"value":..}` each. */
  def write(
      namespace: KafkaTestNamespace.Namespace,
      fileName: String = FileName,
      downstreamFileName: String = "report-documents.jsonl"
  ): IO[(Path, Int)] = IO.blocking {
    val manifest = namespace.manifest
    val properties = new Properties()
    properties.put("bootstrap.servers", manifest.kafkaBootstrap)
    properties.put("group.id", s"hiring-story-export-${UUID.randomUUID()}")
    properties.put("key.deserializer", classOf[StringDeserializer].getName)
    properties.put("value.deserializer", classOf[ByteArrayDeserializer].getName)
    LocalTestServices.saslSettings("analytics_reader", manifest.readerPassword).foreach { case (key, value) =>
      properties.put(key, value)
    }
    properties.put("isolation.level", "read_committed")
    properties.put("enable.auto.commit", "false")
    val directory = target(manifest.workspace).getParent
    val file = directory.resolve(fileName)
    Files.createDirectories(
      directory,
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"))
    )
    // A stale hand-off must not satisfy a new run: remove this dataset's events and downstream report first.
    Files.deleteIfExists(directory.resolve(downstreamFileName))
    Files.deleteIfExists(file)
    val consumer = new KafkaConsumer[String, Array[Byte]](properties)
    val partition = new TopicPartition(namespace.events, 0)
    try {
      consumer.assign(Collections.singleton(partition))
      consumer.seekToBeginning(Collections.singleton(partition))
      val end = consumer.endOffsets(Collections.singleton(partition)).get(partition).longValue()
      val deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos
      def drain(collected: Vector[String]): Vector[String] =
        if (consumer.position(partition) >= end) collected
        else if (System.nanoTime() > deadline)
          throw new IllegalStateException("The export did not reach the topic end in 60 s")
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
      Files.createFile(file, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
      val _ = Files.writeString(file, lines.mkString("", "\n", "\n"), StandardOpenOption.WRITE)
      (file, lines.size)
    } finally consumer.close()
  }
}

/** The explicit opt-in of the analytics chain (spec D7): the marker file
  * `.local/data/hiring-story-analytics/chain-started`, created by whoever starts the chain (the wrapper does not pass
  * custom environment variables or system properties through, and the agent harness forbids setting them). The export,
  * the analytics step and the Admin read run only while the marker exists. Otherwise they are skipped, which is
  * reported as unverified and never as a pass, and nothing in the hand-off directory is touched, so an unordered full
  * run gives the same result in any suite order. In a started chain a missing, stale or mismatched hand-off FAILS.
  */
object AnalyticsChain {
  val MarkerFile = "chain-started"
  private def marker: java.nio.file.Path =
    Path
      .of(sys.env.getOrElse("HIRING_TEST_WORKSPACE", System.getProperty("user.dir")))
      .resolve(".local/data")
      .resolve(HiringStoryEventExport.Directory)
      .resolve(MarkerFile)
  def started: Boolean = Files.isRegularFile(marker)
  def require(): Unit =
    munit.Assertions.assume(started, s"SKIPPED: analytics chain not started (create $MarkerFile, spec D7)")
}
