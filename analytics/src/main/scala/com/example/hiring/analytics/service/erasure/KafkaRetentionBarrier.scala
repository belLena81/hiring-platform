package com.example.hiring.analytics.service.erasure

import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.domain.{AnalyticsOffset, AnalyticsPartition, AnalyticsTopic}

import cats.syntax.all.*

trait KafkaRetention[F[_]] {
  def capture(): F[KafkaRetentionBarrier]
  def retentionPassed(barrier: KafkaRetentionBarrier): F[Boolean]
}

final case class KafkaRetentionBarrier(topic: AnalyticsTopic, partitions: Vector[KafkaRetentionBarrier.Partition])

object KafkaRetentionBarrier {
  final case class Partition(number: AnalyticsPartition, endOffsetExclusive: AnalyticsOffset)

  object Partition {
    def from(number: Int, endOffsetExclusive: Long): Either[AnalyticsError, Partition] =
      for {
        partition <- AnalyticsPartition
          .from(number)
          .leftMap(_ => AnalyticsError.InvalidConfiguration("Kafka erasure retention barrier is malformed"))
        offset <- AnalyticsOffset
          .from(endOffsetExclusive)
          .leftMap(_ => AnalyticsError.InvalidConfiguration("Kafka erasure retention barrier is malformed"))
      } yield Partition(partition, offset)
  }

  def from(topic: String, partitions: Vector[(Int, Long)]): Either[AnalyticsError, KafkaRetentionBarrier] =
    for {
      refinedTopic <- AnalyticsTopic
        .from(topic)
        .leftMap(_ => AnalyticsError.InvalidConfiguration("Kafka erasure retention barrier is malformed"))
      refinedPartitions <- partitions.traverse { case (number, offset) => Partition.from(number, offset) }
      barrier <- validate(KafkaRetentionBarrier(refinedTopic, refinedPartitions))
    } yield barrier

  def validate(barrier: KafkaRetentionBarrier): Either[AnalyticsError, KafkaRetentionBarrier] = {
    val invalid = barrier.partitions.isEmpty ||
      barrier.partitions.map(_.number).distinct.size != barrier.partitions.size
    if (invalid) Left(AnalyticsError.InvalidConfiguration("Kafka erasure retention barrier is malformed"))
    else Right(barrier.copy(partitions = barrier.partitions.sortBy(_.number)))
  }

  def hasExpired(
      barrier: KafkaRetentionBarrier,
      earliestOffsets: Map[Int, Long]
  ): Either[AnalyticsError, Boolean] =
    validate(barrier).flatMap { valid =>
      if (valid.partitions.exists(partition => !earliestOffsets.contains(AnalyticsPartition.unwrap(partition.number))))
        Left(AnalyticsError.InvalidConfiguration("Kafka erasure retention barrier references a missing partition"))
      else
        Right(
          valid.partitions.forall(partition =>
            earliestOffsets(AnalyticsPartition.unwrap(partition.number)) >= AnalyticsOffset.unwrap(
              partition.endOffsetExclusive
            )
          )
        )
    }
}
