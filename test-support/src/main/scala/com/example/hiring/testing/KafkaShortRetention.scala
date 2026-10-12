package com.example.hiring.testing

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import org.apache.kafka.clients.admin.{Admin, AlterConfigOp, ConfigEntry, OffsetSpec}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.serialization.StringSerializer
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Accelerated real physical retention on exact temporary topics, without changing shared broker policy. */
object KafkaShortRetention {
  private val Timeout = java.util.concurrent.TimeUnit.SECONDS

  /** One broker-privileged admin and producer pair for the isolated test services. */
  final class Probe private[KafkaShortRetention] (admin: Admin, producer: KafkaProducer[String, String]) {
    private def partition(topic: String) = new TopicPartition(topic, 0)

    /** Applies the shortened retention and segment settings to this exact topic only. */
    def shorten(topic: String): IO[Unit] = IO.blocking {
      val target = new ConfigResource(ConfigResource.Type.TOPIC, topic)
      val operations = Vector("retention.ms" -> "1500", "segment.ms" -> "1000", "file.delete.delay.ms" -> "1000")
        .map { case (key, value) => new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET) }
      val _ = admin.incrementalAlterConfigs(Map(target -> operations.asJava).asJava).all().get(10L, Timeout)
    }

    def send(topic: String, value: String): IO[Unit] = IO.blocking {
      val _ = producer.send(new ProducerRecord(topic, "retention-probe", value)).get(10L, Timeout)
    }

    def offset(topic: String, spec: OffsetSpec): IO[Long] = IO.blocking {
      admin
        .listOffsets(Map(partition(topic) -> spec).asJava)
        .partitionResult(partition(topic))
        .get(10L, Timeout)
        .offset()
    }

    def earliest(topic: String): IO[Long] = offset(topic, OffsetSpec.earliest())
    def latest(topic: String): IO[Long] = offset(topic, OffsetSpec.latest())

    /** Polls the earliest offset, rolling segments with probe records, until it reaches the barrier or attempts end. */
    def awaitEarliestAtLeast(topic: String, barrier: Long, attempts: Int = 30): IO[Long] =
      earliest(topic).flatMap { first =>
        if (first >= barrier) IO.pure(first)
        else if (attempts > 0)
          IO.sleep(1.second) *> send(topic, "segment-roll") *> awaitEarliestAtLeast(topic, barrier, attempts - 1)
        else IO.raiseError(new AssertionError("Physical Kafka retention barrier did not pass"))
      }

    /** Shortens a topic, writes records on both sides of a segment boundary and waits for the old ones to expire. */
    def expireCurrentRecords(topic: String): IO[(Long, Long)] =
      for {
        _ <- shorten(topic)
        _ <- send(topic, "before") *> IO.sleep(2.seconds) *> send(topic, "after")
        barrier <- latest(topic)
        _ <- IO.sleep(2.seconds) *> send(topic, "roll")
        beginning <- awaitEarliestAtLeast(topic, barrier)
      } yield (barrier, beginning)
  }

  def probe(manifest: LocalTestServices.Manifest): Resource[IO, Probe] = {
    val settings = LocalTestServices.adminProperties(manifest, "broker", manifest.brokerPassword)
    val _ = settings.put("key.serializer", classOf[StringSerializer].getName)
    val _ = settings.put("value.serializer", classOf[StringSerializer].getName)
    (
      Resource.make(IO.blocking(Admin.create(settings)))(admin => IO.blocking(admin.close())),
      Resource.make(IO.blocking(new KafkaProducer[String, String](settings)))(producer => IO.blocking(producer.close()))
    ).mapN(new Probe(_, _))
  }
}
