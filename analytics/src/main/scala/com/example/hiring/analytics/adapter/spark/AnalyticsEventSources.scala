package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.adapter.kafka.KafkaClientProperties
import com.example.hiring.analytics.config.KafkaConnection
import com.example.hiring.analytics.domain.{AnalyticsRunManifest, PartitionOffsetRange}
import com.example.hiring.analytics.domain.AnalyticsTopic
import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import io.circe.Json
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit}

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Reads exactly the offsets named by a manifest; it never starts a streaming query. */
private[analytics] final class KafkaOffsetRangeSource[F[_]: Async](
    connection: KafkaConnection,
    driverExecution: SparkBlockingExecution[F],
    sparkExecution: SparkExecution[F]
) extends BoundedOperationalEventSource[F] {
  override def verifyOffsets(frame: DataFrame, manifest: AnalyticsRunManifest): F[Unit] =
    AnalyticsOffsetRanges.verifyCommittedKafkaRange(frame, manifest, sparkExecution)

  override def read(spark: SparkSession, manifest: AnalyticsRunManifest): F[DataFrame] =
    for {
      _ <- KafkaConnection.preflight[F](connection)
      _ <- AnalyticsOffsetRanges.requireNonEmpty(manifest)
      _ <- KafkaOffsetRangeSource.verifyAvailable(connection, manifest, driverExecution)
      options <- Async[F].fromEither(KafkaClientProperties.sparkOptions(connection))
      frame <- sparkExecution {
        spark.read
          .format("kafka")
          .option("kafka.bootstrap.servers", connection.bootstrapServers)
          .options(options)
          .option("assign", KafkaOffsetRangeSource.assignJson(manifest.offsetRanges))
          .option("startingOffsets", KafkaOffsetRangeSource.offsetJson(manifest.offsetRanges, _.startOffset))
          .option("endingOffsets", KafkaOffsetRangeSource.offsetJson(manifest.offsetRanges, _.endOffsetExclusive))
          .option("failOnDataLoss", "true")
          .load()
      }.adaptError {
        case error: AnalyticsError => error
        case NonFatal(cause)       => AnalyticsError.SourceReadFailure(cause)
      }
    } yield frame
}

object KafkaOffsetRangeSource {

  /** Stable broker identity used to bind streaming activation and checkpoints across endpoint changes. */
  private[analytics] def sourceIdentity[F[_]: Async](
      connection: KafkaConnection,
      topic: AnalyticsTopic,
      driverExecution: SparkBlockingExecution[F]
  ): F[(String, String)] =
    for {
      _ <- KafkaConnection.preflight[F](connection)
      clientProperties <- Async[F].fromEither(KafkaClientProperties.clientProperties(connection))
      identity <- driverExecution
        .blocking {
          val settings = new Properties()
          settings.setProperty("bootstrap.servers", connection.bootstrapServers)
          settings.setProperty("default.api.timeout.ms", "10000")
          settings.setProperty("request.timeout.ms", "10000")
          clientProperties.foreach { case (key, value) => settings.setProperty(key, value) }
          val client = AdminClient.create(settings)
          try {
            val timeoutSeconds = 10L
            val clusterId = Option(client.describeCluster().clusterId().get(timeoutSeconds, TimeUnit.SECONDS))
            val topicName = AnalyticsTopic.unwrap(topic)
            val description = client
              .describeTopics(java.util.Collections.singleton(topicName))
              .allTopicNames()
              .get(timeoutSeconds, TimeUnit.SECONDS)
              .get(topicName)
            for {
              cluster <- clusterId.toRight(new IllegalStateException("Kafka cluster ID is unavailable"))
              topicId <- Option(description)
                .flatMap(value => Option(value.topicId()))
                .map(_.toString)
                .filter(_.nonEmpty)
                .toRight(new IllegalStateException("Kafka topic ID is unavailable"))
            } yield cluster -> topicId
          } finally client.close(java.time.Duration.ofSeconds(10L))
        }
        .flatMap(Async[F].fromEither)
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.SourceReadFailure(cause)
        }
    } yield identity

  private def consumer[F[_]: Async](
      connection: KafkaConnection,
      driverExecution: SparkBlockingExecution[F]
  ): Resource[F, KafkaConsumer[Array[Byte], Array[Byte]]] =
    for {
      _ <- Resource.eval(KafkaConnection.preflight[F](connection))
      clientProperties <- Resource.eval(Async[F].fromEither(KafkaClientProperties.clientProperties(connection)))
      client <- Resource.make(driverExecution.blocking {
        val settings = new Properties()
        settings.setProperty("bootstrap.servers", connection.bootstrapServers)
        settings.setProperty("key.deserializer", classOf[ByteArrayDeserializer].getName)
        settings.setProperty("value.deserializer", classOf[ByteArrayDeserializer].getName)
        settings.setProperty("enable.auto.commit", "false")
        settings.setProperty("isolation.level", "read_committed")
        settings.setProperty("default.api.timeout.ms", "10000")
        clientProperties.foreach { case (key, value) => settings.setProperty(key, value) }
        new KafkaConsumer[Array[Byte], Array[Byte]](settings)
      })(client => driverExecution.blocking(client.close()).void)
    } yield client

  private[analytics] def availablePartitions[F[_]: Async](
      connection: KafkaConnection,
      topic: AnalyticsTopic,
      driverExecution: SparkBlockingExecution[F]
  ): F[Set[Int]] =
    consumer[F](connection, driverExecution).use { client =>
      driverExecution
        .blocking(
          Option(client.partitionsFor(AnalyticsTopic.unwrap(topic))).toVector
            .flatMap(_.asScala)
            .map(_.partition())
            .toSet
        )
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.SourceReadFailure(cause)
        }
    }

  private[analytics] def verifyAvailable[F[_]: Async](
      connection: KafkaConnection,
      manifest: AnalyticsRunManifest,
      driverExecution: SparkBlockingExecution[F]
  ): F[Unit] =
    consumer[F](connection, driverExecution).use { client =>
      driverExecution
        .blocking {
          val topic = AnalyticsTopic.unwrap(manifest.offsetRanges.head.topic)
          val partitions = Option(client.partitionsFor(topic)).toVector.flatMap(_.asScala).map(_.partition()).toSet
          val missing = manifest.offsetRanges.find(range => !partitions.contains(range.partition))
          missing match {
            case Some(range) =>
              Left(
                AnalyticsError.MissingOffsetRange(
                  AnalyticsTopic.unwrap(range.topic),
                  range.partition,
                  range.endOffsetExclusive - range.startOffset,
                  0L
                )
              )
            case None =>
              val requested = manifest.offsetRanges.map(range =>
                new TopicPartition(AnalyticsTopic.unwrap(range.topic), range.partition)
              )
              val earliest = client.beginningOffsets(requested.asJava)
              val latest = client.endOffsets(requested.asJava)
              manifest.offsetRanges.foldLeft[Either[AnalyticsError, Unit]](Right(())) { (result, range) =>
                val partition = new TopicPartition(AnalyticsTopic.unwrap(range.topic), range.partition)
                result.flatMap { _ =>
                  (Option(earliest.get(partition)), Option(latest.get(partition))) match {
                    case (Some(first), Some(last)) =>
                      AnalyticsOffsetRanges.available(range, first.longValue(), last.longValue())
                    case _ =>
                      Left(
                        AnalyticsError.MissingOffsetRange(
                          AnalyticsTopic.unwrap(range.topic),
                          range.partition,
                          range.endOffsetExclusive - range.startOffset,
                          0L
                        )
                      )
                  }
                }
              }
          }
        }
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.SourceReadFailure(cause)
        }
        .flatMap(Async[F].fromEither)
    }

  private[analytics] def offsetJson(
      ranges: Vector[PartitionOffsetRange],
      select: PartitionOffsetRange => Long
  ): String = {
    val byTopic = ranges
      .groupBy(_.topic)
      .toSeq
      .sortBy(entry => AnalyticsTopic.unwrap(entry._1))
      .map { case (topic, topicRanges) =>
        AnalyticsTopic.unwrap(topic) -> Json.fromFields(
          topicRanges.sortBy(_.partition).map(range => range.partition.toString -> Json.fromLong(select(range)))
        )
      }
    Json.fromFields(byTopic).noSpaces
  }

  private[analytics] def assignJson(ranges: Vector[PartitionOffsetRange]): String = {
    val byTopic = ranges
      .groupBy(_.topic)
      .toSeq
      .sortBy(entry => AnalyticsTopic.unwrap(entry._1))
      .map { case (topic, topicRanges) =>
        AnalyticsTopic.unwrap(topic) -> Json.arr(topicRanges.map(_.partition).distinct.sorted.map(Json.fromInt)*)
      }
    Json.fromFields(byTopic).noSpaces
  }
}

/** Test and backfill adapter. Its frame must have Kafka's topic, partition, offset, timestamp and value columns. */
private[analytics] final case class DataFrameBatchSource[F[_]: Async](
    records: DataFrame,
    sparkExecution: SparkExecution[F]
) extends BoundedOperationalEventSource[F] {
  override def verifyOffsets(frame: DataFrame, manifest: AnalyticsRunManifest): F[Unit] =
    AnalyticsOffsetRanges.verify(frame, manifest, sparkExecution)

  override def read(spark: SparkSession, manifest: AnalyticsRunManifest): F[DataFrame] =
    AnalyticsOffsetRanges.requireNonEmpty(manifest) *>
      sparkExecution(records.schema)
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.SourceReadFailure(cause)
        }
        .flatMap(KafkaRecordColumns.validate) *>
      sparkExecution {
        val inManifest = manifest.offsetRanges.foldLeft(lit(false): Column) { (condition, range) =>
          condition || (
            col("topic") === lit(AnalyticsTopic.unwrap(range.topic)) &&
              col("partition") === lit(range.partition) &&
              col("offset") >= lit(range.startOffset) &&
              col("offset") < lit(range.endOffsetExclusive)
          )
        }
        records.filter(inManifest)
      }
        .adaptError {
          case error: AnalyticsError => error
          case NonFatal(cause)       => AnalyticsError.SourceReadFailure(cause)
        }
}

private[analytics] object DataFrameBatchSource {
  def apply[F[_]: Async](records: DataFrame): DataFrameBatchSource[F] =
    new DataFrameBatchSource[F](
      records,
      SparkBlockingExecution.forTests[F](scala.concurrent.ExecutionContext.parasitic)
    )
}
