package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.port.RepositoryF
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.shared.CauseChain
import fs2.Stream
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException}
import retry.{HandlerDecision, RetryPolicies, retryingOnErrors}

import scala.concurrent.duration.FiniteDuration

/** Stream, offset and producer-generation behaviour shared by the operational-event and interview Kafka runtimes. */
private[kafka] object KafkaRuntimeSupport {

  /** The outer finalizer reports release only after the acquired producer has actually closed. */
  def observedGeneration[A](
      transactionalId: String,
      diagnostics: Diagnostics[IO]
  )(producer: Resource[IO, A]): Resource[IO, A] =
    Resource
      .makeFull[IO, (A, IO[Unit])](poll =>
        poll(producer.allocated).flatTap(_ =>
          diagnostics
            .emit(LogEvent.ProducerGenerationStarted, fields = Map(LogField.TransactionalId -> transactionalId))
        )
      ) { case (_, release) =>
        release *> diagnostics.emit(
          LogEvent.ProducerGenerationClosed,
          fields = Map(LogField.TransactionalId -> transactionalId)
        )
      }
      .map(_._1)

  def processRecord(
      topic: String,
      partition: Int,
      offset: Long
  )(process: RepositoryF[IO, Unit])(commit: IO[Unit]): IO[Unit] =
    process.value.flatMap {
      case Right(()) => commit
      // Stop this stream before reading a later offset. Continuing here could
      // commit N+1 and make the undurable record at N unrecoverable for this group.
      case Left(error) => IO.raiseError(RecordNotDurable(topic, partition, offset, error))
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
