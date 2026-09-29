package com.example.hiring.analytics.adapter.kafka

import com.example.hiring.analytics.config.KafkaConnection
import com.example.hiring.analytics.adapter.spark.SparkBlockingExecution
import com.example.hiring.analytics.domain.{AnalyticsPartition, AnalyticsTopic}
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.erasure.{KafkaRetention, KafkaRetentionBarrier}

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import java.util.Properties
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

private[analytics] final class KafkaRetentionAdapter[F[_]: Async](
    connection: KafkaConnection,
    topic: AnalyticsTopic,
    driverExecution: SparkBlockingExecution[F]
) extends KafkaRetention[F] {
  override def capture(): F[KafkaRetentionBarrier] =
    KafkaRetentionAdapter.capture[F](connection, topic, driverExecution)

  override def retentionPassed(barrier: KafkaRetentionBarrier): F[Boolean] =
    KafkaRetentionAdapter.retentionPassed[F](connection, barrier, driverExecution)
}

private[analytics] object KafkaRetentionAdapter {

  /** Captures the topic's exclusive end offsets. Callers must first prove that no subject lease or retryable outbox
    * event can still publish, then persist this result before waiting for retention.
    */
  def capture[F[_]: Async](
      connection: KafkaConnection,
      topic: AnalyticsTopic,
      driverExecution: SparkBlockingExecution[F]
  ): F[KafkaRetentionBarrier] =
    consumer[F](connection, driverExecution).use { client =>
      driverExecution
        .blocking {
          val topicName = AnalyticsTopic.unwrap(topic)
          val partitions = Option(client.partitionsFor(topicName)).toVector
            .flatMap(_.asScala)
            .map(partition => new TopicPartition(topicName, partition.partition()))
            .sortBy(_.partition())
          for {
            _ <- Either.cond(
              partitions.nonEmpty,
              (),
              AnalyticsError.InvalidConfiguration("Kafka erasure barrier topic has no partitions")
            )
            ends = client.endOffsets(partitions.asJava)
            offsets <- partitions.traverse { partition =>
              Option(ends.get(partition))
                .map(_.longValue())
                .toRight(AnalyticsError.InvalidConfiguration("Kafka erasure barrier end offset is unavailable"))
            }
            barrier <- KafkaRetentionBarrier.from(
              topicName,
              partitions.zip(offsets).map { case (partition, offset) => partition.partition() -> offset }
            )
          } yield barrier
        }
        .flatMap(Async[F].fromEither)
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.SourceReadFailure(cause)
        }
    }

  /** Checks actual broker earliest offsets rather than inferring expiry from wall-clock age. */
  def retentionPassed[F[_]: Async](
      connection: KafkaConnection,
      barrier: KafkaRetentionBarrier,
      driverExecution: SparkBlockingExecution[F]
  ): F[Boolean] =
    Async[F].fromEither(KafkaRetentionBarrier.validate(barrier)).flatMap { valid =>
      consumer[F](connection, driverExecution).use { client =>
        driverExecution
          .blocking {
            val topicName = AnalyticsTopic.unwrap(valid.topic)
            val partitions = valid.partitions
              .map(partition => new TopicPartition(topicName, AnalyticsPartition.unwrap(partition.number)))
            val beginnings = client.beginningOffsets(partitions.asJava)
            val earliest = valid.partitions.flatMap { partition =>
              Option(beginnings.get(new TopicPartition(topicName, AnalyticsPartition.unwrap(partition.number))))
                .map(offset => AnalyticsPartition.unwrap(partition.number) -> offset.longValue())
            }.toMap
            KafkaRetentionBarrier.hasExpired(valid, earliest)
          }
          .flatMap(Async[F].fromEither)
          .adaptError {
            case error: AnalyticsError => error
            case NonFatal(cause)       => AnalyticsError.SourceReadFailure(cause)
          }
      }
    }

  def liveRetention[F[_]: Async](
      connection: KafkaConnection,
      topic: AnalyticsTopic,
      driverExecution: SparkBlockingExecution[F]
  ): KafkaRetention[F] =
    new KafkaRetentionAdapter[F](connection, topic, driverExecution)

  private def consumer[F[_]: Async](
      connection: KafkaConnection,
      driverExecution: SparkBlockingExecution[F]
  ): Resource[F, KafkaConsumer[Array[Byte], Array[Byte]]] =
    for {
      clientProperties <- Resource.eval(Async[F].fromEither(KafkaClientProperties.clientProperties(connection)))
      client <- Resource.make(driverExecution.blocking {
        val properties = new Properties()
        properties.setProperty("bootstrap.servers", connection.bootstrapServers)
        properties.setProperty("group.id", "hiring-analytics-erasure")
        properties.setProperty("key.deserializer", classOf[ByteArrayDeserializer].getName)
        properties.setProperty("value.deserializer", classOf[ByteArrayDeserializer].getName)
        properties.setProperty("enable.auto.commit", "false")
        properties.setProperty("default.api.timeout.ms", "10000")
        clientProperties.foreach { case (key, value) => properties.setProperty(key, value) }
        new KafkaConsumer[Array[Byte], Array[Byte]](properties)
      })(client => driverExecution.blocking(client.close()).void)
    } yield client
}
