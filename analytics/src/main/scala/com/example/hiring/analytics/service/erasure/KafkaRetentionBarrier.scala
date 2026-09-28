package com.example.hiring.analytics.service.erasure

import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.config.KafkaConnection

trait KafkaRetention[F[_]] {
  def capture(connection: KafkaConnection, topic: String): F[KafkaRetentionBarrier]
  def retentionPassed(connection: KafkaConnection, barrier: KafkaRetentionBarrier): F[Boolean]
}

final case class KafkaRetentionBarrier(topic: String, partitions: Vector[KafkaRetentionBarrier.Partition])

object KafkaRetentionBarrier {
  final case class Partition(number: Int, endOffsetExclusive: Long)

  def validate(barrier: KafkaRetentionBarrier): Either[AnalyticsError, KafkaRetentionBarrier] = {
    val invalid = barrier.topic.trim.isEmpty || barrier.partitions.isEmpty ||
      barrier.partitions.exists(partition => partition.number < 0 || partition.endOffsetExclusive < 0L) ||
      barrier.partitions.map(_.number).distinct.size != barrier.partitions.size
    if (invalid) Left(AnalyticsError.InvalidConfiguration("Kafka erasure retention barrier is malformed"))
    else Right(barrier.copy(partitions = barrier.partitions.sortBy(_.number)))
  }

  def hasExpired(barrier: KafkaRetentionBarrier, earliestOffsets: Map[Int, Long]): Either[AnalyticsError, Boolean] =
    validate(barrier).flatMap { valid =>
      if (valid.partitions.exists(partition => !earliestOffsets.contains(partition.number)))
        Left(AnalyticsError.InvalidConfiguration("Kafka erasure retention barrier references a missing partition"))
      else
        Right(valid.partitions.forall(partition => earliestOffsets(partition.number) >= partition.endOffsetExclusive))
    }
}
