package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.infrastructure.telemetry.{KafkaClientMetrics, KafkaRebalanceKind}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.port.RepositoryF
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.shared.CauseChain
import fs2.Stream
import fs2.kafka.{CommittableConsumerRecord, CommittableOffset, ConsumerSettings, KafkaConsumer}
import org.apache.kafka.clients.consumer.CommitFailedException
import org.apache.kafka.common.errors.{
  InvalidProducerEpochException,
  ProducerFencedException,
  RebalanceInProgressException
}
import retry.{HandlerDecision, RetryPolicies, retryingOnErrors}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*

/** Stream, offset and producer-generation behaviour shared by the operational-event and interview Kafka runtimes. */
private[kafka] object KafkaRuntimeSupport {

  /** The outer finalizer reports release only after the acquired producer has actually closed. */
  def observedGeneration[A](
      transactionalId: String,
      generationId: java.util.UUID,
      diagnostics: Diagnostics[IO]
  )(producer: Resource[IO, A]): Resource[IO, A] = {
    val fields = Map(LogField.TransactionalId -> transactionalId, LogField.GenerationId -> generationId.toString)
    Resource
      .makeFull[IO, (A, IO[Unit])](poll =>
        poll(producer.allocated).flatTap(_ => diagnostics.emit(LogEvent.ProducerGenerationStarted, fields = fields))
      ) { case (_, release) =>
        release *> diagnostics.emit(LogEvent.ProducerGenerationClosed, fields = fields)
      }
      .map(_._1)
  }

  /** The offset to commit once the record is durable. A failure raises first, so the offset is never emitted. */
  def processRecord(
      topic: String,
      partition: Int,
      offset: Long
  )(process: RepositoryF[IO, Unit])(committable: CommittableOffset[IO]): IO[CommittableOffset[IO]] =
    process.foldF(
      // Stop this stream before reading a later offset. Continuing here could
      // commit N+1 and make the undurable record at N unrecoverable for this group.
      error => IO.raiseError(RecordNotDurable(topic, partition, offset, error)),
      _ => IO.pure(committable)
    )

  /** A commit lost because the group rebalanced under it: the partition moved on, redelivery is absorbed by the durable
    * receipt, and the next batch commits normally.
    */
  def isRebalanceCommitFailure(error: Throwable): Boolean =
    CauseChain(error).exists(value =>
      value.isInstanceOf[CommitFailedException] || value.isInstanceOf[RebalanceInProgressException]
    )

  /** The consumer stream shared by every committed consumer: subscribes, reports assignment movement and client lag,
    * and commits per partition in batches. `process` returns the offset that is safe to commit.
    *
    * Lag comes from the client's own `records-lag` metrics on a timer, never from `endOffsets` or `committed`, which
    * would take the consumer lock the poll loop needs. Reading the local metrics avoids a broker round trip;
    * `consumer.metrics` still goes through fs2-kafka's serialized access to the consumer.
    */
  def consume(
      settings: ConsumerSettings[IO, String, Array[Byte]],
      topic: String,
      group: String,
      concurrency: Int,
      batching: CommitBatching,
      metrics: KafkaClientMetrics,
      diagnostics: Diagnostics[IO],
      lagSampleEvery: FiniteDuration
  )(process: CommittableConsumerRecord[IO, String, Array[Byte]] => IO[CommittableOffset[IO]]): Stream[IO, Unit] =
    KafkaConsumer.stream(settings).evalTap(_.subscribeTo(topic)).flatMap { consumer =>
      val partitions = consumer.partitionsMapStream.flatMap { assigned =>
        Stream.eval(metrics.rebalanced(group, KafkaRebalanceKind.Assigned, assigned.size)) >>
          Stream.emits(assigned.toVector).map { (partition, records) =>
            // Also runs on restart or shutdown, not only on a rebalance; see the metric description.
            records.onFinalize(
              metrics.rebalanced(group, KafkaRebalanceKind.Revoked, 1) *>
                metrics.dropLag(group, partition.topic, Set(partition.partition))
            )
          }
      }
      val sampleLag = Stream
        .awakeEvery[IO](lagSampleEvery)
        .evalMap(_ =>
          consumer.metrics
            .map(view =>
              KafkaClientMetrics.ClientLag.fromClientMetrics(
                view.toList.map((name, metric) => (name.name, name.group, name.tags.asScala.toMap, metric.metricValue))
              )
            )
            .flatMap(byTopic => metrics.recordLag(group, topic, byTopic.getOrElse(topic, Map.empty)))
            .handleError(_ => ())
        )
      KafkaPartitionProcessing(
        partitions,
        concurrency,
        batching,
        error =>
          diagnostics.emit(LogEvent.ConsumerCommitDropped, fields = LogFields.failure(error)) *>
            metrics.commitDropped(group)
      )(process).concurrently(sampleLag)
    }

  def resilientStream(
      diagnostics: Diagnostics[IO],
      stream: Stream[IO, Unit],
      baseDelay: FiniteDuration,
      stopRetrying: Throwable => Boolean = _ => false,
      maxDelay: FiniteDuration
  ): Stream[IO, Unit] =
    Stream.eval(
      retryingOnErrors(stream.compile.drain)(
        policy = restartPolicy(baseDelay, maxDelay),
        errorHandler = (error, _) =>
          diagnostics
            .emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error))
            .as(if (stopRetrying(error)) HandlerDecision.Stop else HandlerDecision.Continue)
      )
    )

  def restartPolicy(baseDelay: FiniteDuration, maxDelay: FiniteDuration): retry.RetryPolicy[IO, Any] =
    RetryPolicies.capDelay(maxDelay, RetryPolicies.fullJitter[IO](baseDelay))

  private final case class RecordNotDurable(topic: String, partition: Int, offset: Long, cause: RepositoryError)
      extends RuntimeException(s"Kafka record could not be durably processed: $topic-$partition@$offset ($cause)")

  def isProducerFenced(error: Throwable): Boolean =
    CauseChain(error)
      .exists(value => value.isInstanceOf[ProducerFencedException] || value.isInstanceOf[InvalidProducerEpochException])
}
