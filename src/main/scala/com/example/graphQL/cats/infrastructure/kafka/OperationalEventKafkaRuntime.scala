package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{Clock, IO, Resource}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.config.KafkaConfig
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy
import org.apache.kafka.common.errors.{
  AuthorizationException,
  BrokerNotAvailableException,
  InvalidTopicException,
  RecordTooLargeException,
  RetriableException,
  SerializationException
}
import com.example.graphQL.cats.service.resilience.CircuitBreaker
import com.example.graphQL.cats.service.port.{
  ClaimedOperationalEvent,
  ConsumerReceiptRepository,
  EventQuarantineRecord,
  EventQuarantineRepository,
  OperationalEventFailureCategory,
  OperationalEventOutboxRepository,
  RepositoryF
}
import com.example.graphQL.cats.service.{BackgroundWorker, Diagnostics, LogEvent, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.service.events.OperationalEventJson
import com.example.graphQL.cats.shared.CauseChain
import fs2.Stream
import fs2.kafka.*
import fs2.kafka.producer.MkProducer

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

object OperationalEventKafkaRuntime {
  private val PublisherWorker = "operational-event-publisher"
  private val ConsumerWorker = "operational-event-consumer"
  private val PublicationWaveSize = 4
  private val OperationalRequestBytes = 1048576

  def resource(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository[IO],
      receipts: ConsumerReceiptRepository[IO],
      quarantine: EventQuarantineRepository[IO],
      diagnostics: Diagnostics[IO],
      clock: Clock[IO] = Clock[IO],
      uuidGen: UUIDGen[IO] = UUIDGen[IO],
      breaker: CircuitBreaker[IO] = CircuitBreaker.disabled[IO]
  ): Resource[IO, Unit] =
    if (!config.enabled) Resource.unit
    else {
      val publisher = publisherResource(config, outbox, diagnostics, clock, uuidGen, breaker)
      val consumer =
        if (config.consumer.enabled) consumerResource(config, receipts, quarantine, diagnostics, clock)
        else Resource.unit
      publisher *> consumer
    }

  private def publisherResource(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository[IO],
      diagnostics: Diagnostics[IO],
      clock: Clock[IO],
      uuidGen: UUIDGen[IO],
      breaker: CircuitBreaker[IO]
  ): Resource[IO, Unit] = {
    val settings = KafkaClientSettings.producer(
      config.bootstrapServers,
      config.publisher.saslUsername,
      config.publisher.saslPassword,
      config.saslSecurityProtocol,
      OperationalRequestBytes
    )

    def generation: Stream[IO, Unit] =
      Stream.eval(uuidGen.randomUUID).flatMap { id =>
        val transactionalId = KafkaProducerGenerationRetirement.PublisherPrefix + id.toString
        Stream
          .resource(
            KafkaRuntimeSupport.observedGeneration(transactionalId, diagnostics)(
              GuardedTransactionalProducer
                .resource(TransactionalProducerSettings(transactionalId, settings), MkProducer.mkProducerForSync[IO])
            )
          )
          .flatMap { producer =>
            KafkaRuntimeSupport.resilientStream(
              diagnostics,
              Stream
                .awakeEvery[IO](config.publisher.pollIntervalMillis.millis)
                .evalMap(_ => publishBatch(config, outbox, diagnostics, transactionalId, producer, clock, breaker)),
              config.publisher.retryDelaySeconds.seconds,
              stopRetrying = KafkaRuntimeSupport.isProducerFenced,
              maxDelay = config.restartMaxDelaySeconds.seconds
            )
          }
          .handleErrorWith {
            case _: ProducerGenerationFenced => Stream.eval(diagnostics.emit(LogEvent.RuntimeFailed))
            case error                       =>
              Stream.eval(diagnostics.emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error))) ++
                Stream.sleep_[IO](config.publisher.retryDelaySeconds.seconds)
          }
      }

    BackgroundWorker.resource(PublisherWorker, diagnostics)(Stream.suspend(generation).repeat.compile.drain)
  }

  private def publishBatch(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository[IO],
      diagnostics: Diagnostics[IO],
      transactionalId: String,
      producer: TransactionalKafkaProducer.WithoutOffsets[IO, String, Array[Byte]],
      clock: Clock[IO],
      breaker: CircuitBreaker[IO]
  ): IO[Unit] =
    publishWaves(config.publisher.batchSize) { limit =>
      claimWithin(breaker, limit) { allowed =>
        clock.realTimeInstant.flatMap { now =>
          requireOutboxSuccess(
            outbox.claim(
              config.publisher.workerId,
              transactionalId,
              now,
              now.plusSeconds(config.publisher.leaseSeconds.toLong),
              allowed
            )
          )
        }
      }
    } { claim =>
      val record = ProducerRecord(config.topic, claim.partitionKey, claim.envelopeBytes)
      publishClaim(config, outbox, diagnostics, claim, clock)(
        guardedSend(breaker, producer.produceWithoutOffsets(ProducerRecords.one(record)).void)
      )
    }

  /** While the breaker is open nothing is claimed, so outbox rows stay untouched until a probe is allowed; probing
    * claims a single event.
    */
  private[kafka] def claimWithin(breaker: CircuitBreaker[IO], limit: Int)(
      claim: Int => IO[List[ClaimedOperationalEvent]]
  ): IO[List[ClaimedOperationalEvent]] =
    breaker.admittedState.flatMap {
      case None                                       => IO.pure(Nil)
      case Some(CircuitBreakerPolicy.State.Closed(_)) => claim(limit)
      case Some(_)                                    => claim(1)
    }

  /** Transport and broker-availability failures only: timeouts, network and disconnect errors, leader changes and other
    * retriable broker errors. Per-record (too large, serialization, invalid topic), authorization and fencing errors
    * say nothing about the broker, and one poison event must not open the breaker for all publication.
    */
  private[kafka] def isBrokerUnavailable(error: Throwable): Boolean = {
    val chain = CauseChain(error).toList
    val perRecordOrAuthorization = chain.exists {
      case _: RecordTooLargeException | _: SerializationException | _: InvalidTopicException |
          _: AuthorizationException =>
        true
      case _ => false
    }
    val transport = chain.exists {
      case _: RetriableException | _: BrokerNotAvailableException | _: java.util.concurrent.TimeoutException |
          _: java.io.IOException =>
        true
      case _ => false
    }
    !KafkaRuntimeSupport.isProducerFenced(error) && !perRecordOrAuthorization && transport
  }

  /** Send failures count only when they show the broker is unavailable. */
  private[kafka] def guardedSend(breaker: CircuitBreaker[IO], send: IO[Unit]): IO[Unit] =
    breaker
      .protect(send)(CircuitBreaker.classifier[IO, Unit](isBrokerUnavailable))
      .flatMap(_.fold(_ => IO.raiseError(PublisherBreakerOpen), IO.pure))

  /** Claim only work that can start its lease heartbeat immediately. Kafka owns transaction serialization. */
  private[kafka] def publishWaves(batchSize: Int)(
      claim: Int => IO[List[ClaimedOperationalEvent]]
  )(publish: ClaimedOperationalEvent => IO[Unit]): IO[Unit] = {
    def loop(remaining: Int): IO[Unit] =
      if (remaining <= 0) IO.unit
      else {
        val limit = remaining.min(PublicationWaveSize)
        claim(limit).flatMap {
          case Nil                           => IO.unit
          case claims if claims.size > limit =>
            IO.raiseError(OutboxOperationFailed("claim exceeded requested publication wave"))
          case claims => publishClaims(claims)(publish) *> loop(remaining - claims.size)
        }
      }
    loop(batchSize)
  }

  private[kafka] def publishClaim(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository[IO],
      diagnostics: Diagnostics[IO],
      claim: ClaimedOperationalEvent,
      clock: Clock[IO] = Clock[IO]
  )(send: IO[Unit]): IO[Unit] = {
    def renew: IO[Unit] = clock.realTimeInstant.flatMap { now =>
      requireOutboxSuccess(
        outbox.renewLease(
          claim.event.eventId,
          claim.leaseToken,
          claim.subjectIds,
          now.plusSeconds(config.publisher.leaseSeconds.toLong)
        )
      )
    }
    val renewEvery = (config.publisher.leaseSeconds.seconds / 3).max(1.second)
    val heartbeat = Stream.awakeEvery[IO](renewEvery).evalMap(_ => renew).compile.drain
    renew *> IO
      .race(send.attempt, heartbeat)
      .flatMap {
        case Left(outcome) => IO.pure(outcome)
        case Right(_)      =>
          IO.raiseError[Either[Throwable, Unit]](
            OutboxOperationFailed("lease heartbeat stopped before Kafka send completed")
          )
      }
      .flatMap {
        case Right(_) =>
          clock.realTimeInstant.flatMap(done =>
            requireOutboxSuccess(
              outbox.markPublished(claim.event.eventId, claim.leaseToken, done, done.plusSeconds(7.days.toSeconds))
            )
          )
        case Left(error) if KafkaRuntimeSupport.isProducerFenced(error) =>
          IO.raiseError(ProducerGenerationFenced(error))
        case Left(PublisherBreakerOpen) =>
          // Never sent: not a delivery attempt that can exhaust the event, so it is only released.
          clock.realTimeInstant.flatMap(releasedAt =>
            requireOutboxSuccess(
              outbox.releaseForRetry(
                claim.event.eventId,
                claim.leaseToken,
                releasedAt,
                releasedAt.plusSeconds(config.publisher.retryDelaySeconds.toLong)
              )
            )
          )
        case Left(error) =>
          diagnostics.emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error)) *> clock.realTimeInstant
            .flatMap { failedAt =>
              if (claim.attempts >= config.publisher.maxAttempts)
                requireOutboxSuccess(
                  outbox.markFailed(claim.event.eventId, claim.leaseToken, failedAt, sanitized(error))
                )
              else
                requireOutboxSuccess(
                  outbox.releaseForRetry(
                    claim.event.eventId,
                    claim.leaseToken,
                    failedAt,
                    failedAt.plusSeconds(config.publisher.retryDelaySeconds.toLong)
                  )
                )
            }
      }
  }

  private def requireOutboxSuccess[A](result: RepositoryF[IO, A]): IO[A] =
    result.leftMap(error => OutboxOperationFailed(error.toString)).rethrowT

  /** Operational facts may arrive out of order; Kafka preserves append order within each partition. */
  private[kafka] def publishClaims(
      claims: List[ClaimedOperationalEvent]
  )(publish: ClaimedOperationalEvent => IO[Unit]): IO[Unit] =
    Stream.emits(claims).covary[IO].parEvalMapUnordered(PublicationWaveSize)(publish).compile.drain

  private[kafka] def consumerResource(
      config: KafkaConfig,
      receipts: ConsumerReceiptRepository[IO],
      quarantine: EventQuarantineRepository[IO],
      diagnostics: Diagnostics[IO],
      clock: Clock[IO] = Clock[IO]
  ): Resource[IO, Unit] = {
    val settings = KafkaClientSettings.committedConsumer(
      config.bootstrapServers,
      config.consumer.saslUsername,
      config.consumer.saslPassword,
      config.saslSecurityProtocol,
      config.consumerGroup
    )

    BackgroundWorker.resource(ConsumerWorker, diagnostics)(
      KafkaRuntimeSupport
        .resilientStream(
          diagnostics,
          KafkaPartitionProcessing(
            KafkaConsumer.stream(settings).subscribeTo(config.topic).partitionedRecords,
            config.consumer.partitionConcurrency
          ) { message =>
            val record = message.record
            KafkaRuntimeSupport.processRecord(record.topic, record.partition, record.offset)(
              recordDurably(
                config,
                receipts,
                quarantine,
                record.topic,
                record.partition,
                record.offset,
                Option(record.value),
                clock
              )
            )(message.offset.commit)
          },
          1.second,
          maxDelay = config.restartMaxDelaySeconds.seconds
        )
        .compile
        .drain
    )
  }

  /** Makes the record durable (receipt, or quarantine when malformed); a repository failure stays typed. */
  private[kafka] def recordDurably(
      config: KafkaConfig,
      receipts: ConsumerReceiptRepository[IO],
      quarantine: EventQuarantineRepository[IO],
      topic: String,
      partition: Int,
      offset: Long,
      bytes: Option[Array[Byte]],
      clock: Clock[IO] = Clock[IO]
  ): RepositoryF[IO, Unit] =
    RepositoryF.lift(clock.realTimeInstant).flatMap { now =>
      bytes.toRight("MalformedEnvelope").flatMap(OperationalEventJson.decode) match {
        case Left(_) =>
          quarantine.save(
            EventQuarantineRecord(
              topic,
              partition,
              offset,
              OperationalEventFailureCategory.MalformedEnvelope,
              bytes.fold("null event envelope")(_ => "malformed event envelope"),
              bytes.getOrElse(Array.emptyByteArray),
              now,
              now.plusSeconds(config.consumer.quarantineTtlDays.days.toSeconds)
            )
          )
        case Right(event) =>
          receipts
            .record(
              config.consumerGroup,
              event,
              now,
              now.plusSeconds(config.consumer.receiptTtlDays.days.toSeconds)
            )
            .void
      }
    }

  /** Client exception messages can embed rejected configuration values, so only class names reach the outbox. */
  private[kafka] def sanitized(error: Throwable): String =
    CauseChain(error)
      .take(5)
      .map(_.getClass.getName)
      .mkString(" <- ")
      .take(512)

  private final case class OutboxOperationFailed(reason: String)
      extends RuntimeException(s"outbox operation failed: $reason")
      with NoStackTrace

  private case object PublisherBreakerOpen
      extends RuntimeException("publisher circuit breaker is open")
      with NoStackTrace

  private final case class ProducerGenerationFenced(cause: Throwable)
      extends RuntimeException("transactional producer generation was fenced", cause)

}
