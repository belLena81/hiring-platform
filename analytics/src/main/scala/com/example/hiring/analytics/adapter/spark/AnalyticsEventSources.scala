package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.adapter.kafka.KafkaClientProperties
import com.example.hiring.analytics.config.KafkaConnection
import com.example.hiring.analytics.domain.{AnalyticsRunManifest, PartitionOffsetRange}
import com.example.hiring.analytics.domain.AnalyticsTopic
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.errors.AnalyticsErrorTranslation.translating

import cats.effect.{Async, Resource}
import cats.effect.syntax.temporal.*
import cats.syntax.all.*
import io.circe.Json
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.KafkaFuture
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.Duration
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.control.NoStackTrace

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
      }.translating(AnalyticsError.SourceReadFailure(_))
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
      adminProperties <- Async[F].fromEither(KafkaClientProperties.adminProperties(connection))
      identity <- Resource
        .make(driverExecution.blocking(Admin.create(KafkaClientProperties.asJava(adminProperties))))(admin =>
          driverExecution.blocking(admin.close(Duration.ofMillis(KafkaClientProperties.ClientTimeoutMillis.toLong)))
        )
        .use { admin =>
          val topicName = AnalyticsTopic.unwrap(topic)
          for {
            clusterId <- awaited(driverExecution.blocking(admin.describeCluster().clusterId()))
            descriptions <- awaited(
              driverExecution.blocking(admin.describeTopics(java.util.Collections.singleton(topicName)).allTopicNames())
            )
            cluster <- unavailable("cluster ID", Option(clusterId))
            topicId <- unavailable(
              "topic ID",
              Option(descriptions.get(topicName))
                .flatMap(value => Option(value.topicId()))
                .map(_.toString)
                .filter(_.nonEmpty)
            )
          } yield cluster -> topicId
        }
        .translating(AnalyticsError.SourceReadFailure(_))
    } yield identity

  /** The broker round trip completes off the single-thread driver executor, is cancellable and bounded. */
  private def awaited[F[_]: Async, A](submitted: F[KafkaFuture[A]]): F[A] =
    submitted.flatMap(future =>
      Async[F]
        .fromCompletableFuture(Async[F].delay(future.toCompletionStage.toCompletableFuture))
        .timeout(KafkaClientProperties.ClientTimeoutMillis.millis)
    )

  private def unavailable[F[_]: Async, A](what: String, value: Option[A]): F[A] =
    Async[F].fromOption(value, AnalyticsError.SourceReadFailure(SourceIdentityUnavailable(what)))

  private final case class SourceIdentityUnavailable(what: String)
      extends RuntimeException(s"Kafka $what is unavailable")
      with NoStackTrace

  private def consumer[F[_]: Async](
      connection: KafkaConnection,
      driverExecution: SparkBlockingExecution[F]
  ): Resource[F, KafkaConsumer[Array[Byte], Array[Byte]]] =
    for {
      _ <- Resource.eval(KafkaConnection.preflight[F](connection))
      consumerProperties <- Resource.eval(
        Async[F].fromEither(KafkaClientProperties.readCommittedConsumerProperties(connection))
      )
      client <- Resource.make(
        driverExecution.blocking(
          new KafkaConsumer[Array[Byte], Array[Byte]](KafkaClientProperties.asJava(consumerProperties))
        )
      )(client => driverExecution.blocking(client.close()).void)
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
        .translating(AnalyticsError.SourceReadFailure(_))
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
          def missing(range: PartitionOffsetRange) =
            AnalyticsError.MissingOffsetRange(
              AnalyticsTopic.unwrap(range.topic),
              range.partition,
              range.endOffsetExclusive - range.startOffset,
              0L
            )
          manifest.offsetRanges.find(range => !partitions.contains(range.partition)).map(missing).toLeft(()).flatMap {
            _ =>
              val requested = manifest.offsetRanges
                .map(range => new TopicPartition(AnalyticsTopic.unwrap(range.topic), range.partition))
              val earliest = client.beginningOffsets(requested.asJava)
              val latest = client.endOffsets(requested.asJava)
              manifest.offsetRanges.traverse_ { range =>
                val partition = new TopicPartition(AnalyticsTopic.unwrap(range.topic), range.partition)
                (Option(earliest.get(partition)), Option(latest.get(partition))) match {
                  case (Some(first), Some(last)) =>
                    AnalyticsOffsetRanges.available(range, first.longValue(), last.longValue())
                  case _ => Left(missing(range))
                }
              }
          }
        }
        .translating(AnalyticsError.SourceReadFailure(_))
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
