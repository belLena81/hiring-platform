package com.example.hiring.analytics

import cats.effect.{IO, Resource}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import java.util.Properties
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** High-water marks captured only after the deleted subject's producer fences and retryable outbox rows have drained.
  * A barrier passes when Kafka's earliest retained offset reaches each captured exclusive offset.
  */
final case class KafkaRetentionBarrier(topic: String, partitions: Vector[KafkaRetentionBarrier.Partition])

/** Kafka retention operations used by the resumable erasure lifecycle. */
trait KafkaRetention {
  def capture(connection: KafkaConnection, topic: String): IO[KafkaRetentionBarrier]
  def retentionPassed(connection: KafkaConnection, barrier: KafkaRetentionBarrier): IO[Boolean]
}

object KafkaRetentionBarrier {
  final case class Partition(number: Int, endOffsetExclusive: Long)

  def validate(barrier: KafkaRetentionBarrier): Either[AnalyticsError, KafkaRetentionBarrier] = {
    val invalid = barrier.topic == null || barrier.topic.trim.isEmpty || barrier.partitions.isEmpty ||
      barrier.partitions.exists(partition => partition.number < 0 || partition.endOffsetExclusive < 0L) ||
      barrier.partitions.map(_.number).distinct.size != barrier.partitions.size
    if (invalid) Left(AnalyticsError.InvalidConfiguration("Kafka erasure retention barrier is malformed"))
    else Right(barrier.copy(partitions = barrier.partitions.sortBy(_.number)))
  }

  def hasExpired(barrier: KafkaRetentionBarrier, earliestOffsets: Map[Int, Long]): Either[AnalyticsError, Boolean] =
    validate(barrier).flatMap { valid =>
      if (valid.partitions.exists(partition => !earliestOffsets.contains(partition.number)))
        Left(AnalyticsError.InvalidConfiguration("Kafka erasure retention barrier references a missing partition"))
      else Right(valid.partitions.forall(partition => earliestOffsets(partition.number) >= partition.endOffsetExclusive))
    }

  /** Captures the topic's exclusive end offsets. Callers must first prove that no subject lease or retryable outbox
    * event can still publish, then persist this result before waiting for retention.
    */
  def capture(connection: KafkaConnection, topic: String): IO[KafkaRetentionBarrier] =
    consumer(connection).use { client =>
      IO.blocking {
        val partitions = Option(client.partitionsFor(topic)).toVector.flatMap(_.asScala)
          .map(partition => new TopicPartition(topic, partition.partition()))
          .sortBy(_.partition())
        if (partitions.isEmpty)
          throw AnalyticsError.InvalidConfiguration("Kafka erasure barrier topic has no partitions")
        val ends = client.endOffsets(partitions.asJava)
        val barrier = KafkaRetentionBarrier(
          topic,
          partitions.map(partition => Partition(partition.partition(), ends.get(partition).longValue())).toVector
        )
        validate(barrier).fold(throw _, identity)
      }.adaptError {
        case error: AnalyticsError => error
        case NonFatal(cause)        => AnalyticsError.SourceReadFailure(cause)
      }
    }

  /** Checks actual broker earliest offsets rather than inferring expiry from wall-clock age. */
  def retentionPassed(connection: KafkaConnection, barrier: KafkaRetentionBarrier): IO[Boolean] =
    IO.fromEither(validate(barrier)).flatMap { valid =>
      consumer(connection).use { client =>
        IO.blocking {
          val partitions = valid.partitions.map(partition => new TopicPartition(valid.topic, partition.number))
          val beginnings = client.beginningOffsets(partitions.asJava)
          val earliest = valid.partitions.flatMap { partition =>
            Option(beginnings.get(new TopicPartition(valid.topic, partition.number)))
              .map(offset => partition.number -> offset.longValue())
          }.toMap
          hasExpired(valid, earliest).fold(throw _, identity)
        }.adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)        => AnalyticsError.SourceReadFailure(cause)
        }
      }
    }

  val liveRetention: KafkaRetention = new KafkaRetention {
    override def capture(connection: KafkaConnection, topic: String): IO[KafkaRetentionBarrier] =
      KafkaRetentionBarrier.capture(connection, topic)

    override def retentionPassed(connection: KafkaConnection, barrier: KafkaRetentionBarrier): IO[Boolean] =
      KafkaRetentionBarrier.retentionPassed(connection, barrier)
  }

  private def consumer(connection: KafkaConnection): Resource[IO, KafkaConsumer[Array[Byte], Array[Byte]]] =
    Resource.fromAutoCloseable(IO.blocking {
      val properties = new Properties()
      properties.setProperty("bootstrap.servers", connection.bootstrapServers)
      properties.setProperty("group.id", "hiring-analytics-erasure")
      properties.setProperty("key.deserializer", classOf[ByteArrayDeserializer].getName)
      properties.setProperty("value.deserializer", classOf[ByteArrayDeserializer].getName)
      properties.setProperty("enable.auto.commit", "false")
      properties.setProperty("default.api.timeout.ms", "10000")
      KafkaConnection.clientProperties(connection).foreach { case (key, value) => properties.setProperty(key, value) }
      new KafkaConsumer[Array[Byte], Array[Byte]](properties)
    })
}
