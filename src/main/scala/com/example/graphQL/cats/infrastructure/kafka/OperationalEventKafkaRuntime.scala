package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{Clock, IO, Resource}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.config.{KafkaConfig, KafkaSaslSecurityProtocol}
import com.example.graphQL.cats.service.port.{
  ClaimedOperationalEvent,
  ConsumerReceiptRepository,
  EventQuarantineRecord,
  EventQuarantineRepository,
  OperationalEventFailureCategory,
  OperationalEventOutboxRepository,
  RepositoryIO
}
import com.example.graphQL.cats.service.{BackgroundWorker, Diagnostics, LogEvent, LogFields, LogField}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.service.events.OperationalEventJson
import fs2.Stream
import fs2.kafka.*
import fs2.kafka.producer.MkProducer
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException}
import org.apache.kafka.clients.consumer.ConsumerConfig
import retry.{HandlerDecision, RetryPolicies, retryingOnErrors}

import java.time.Instant
import scala.concurrent.duration.*

object OperationalEventKafkaRuntime {
  private val PublisherWorker = "operational-event-publisher"
  private val ConsumerWorker = "operational-event-consumer"
  private val PublicationWaveSize = 4
  private val OperationalRequestBytes = 1048576

  def resource(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository,
      receipts: ConsumerReceiptRepository,
      quarantine: EventQuarantineRepository,
      diagnostics: Diagnostics,
      clock: Clock[IO] = Clock[IO],
      uuidGen: UUIDGen[IO] = UUIDGen[IO]
  ): Resource[IO, Unit] =
    if (!config.enabled) Resource.unit
    else {
      val publisher = publisherResource(config, outbox, diagnostics, clock, uuidGen)
      val consumer =
        if (config.consumer.enabled) consumerResource(config, receipts, quarantine, diagnostics, clock)
        else Resource.unit
      publisher *> consumer
    }

  private def publisherResource(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository,
      diagnostics: Diagnostics,
      clock: Clock[IO],
      uuidGen: UUIDGen[IO]
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
        val transactionalId = "hiring-publisher-" + id.toString
        Stream
          .resource(
            observedGeneration(transactionalId, diagnostics)(
              GuardedTransactionalProducer
                .resource(TransactionalProducerSettings(transactionalId, settings), MkProducer.mkProducerForSync[IO])
            )
          )
          .flatMap { producer =>
            resilientStream(
              diagnostics,
              Stream
                .awakeEvery[IO](config.publisher.pollIntervalMillis.millis)
                .evalMap(_ => publishBatch(config, outbox, diagnostics, transactionalId, producer, clock)),
              config.publisher.retryDelaySeconds.seconds,
              stopRetrying = isProducerFenced,
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

  /** The outer finalizer reports release only after the acquired producer has actually closed. */
  private[kafka] def observedGeneration[A](
      transactionalId: String,
      diagnostics: Diagnostics
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

  private def publishBatch(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository,
      diagnostics: Diagnostics,
      transactionalId: String,
      producer: TransactionalKafkaProducer.WithoutOffsets[IO, String, Array[Byte]],
      clock: Clock[IO]
  ): IO[Unit] =
    publishWaves(config.publisher.batchSize) { limit =>
      clock.realTimeInstant.flatMap { now =>
        outbox
          .claim(
            config.publisher.workerId,
            transactionalId,
            now,
            now.plusSeconds(config.publisher.leaseSeconds.toLong),
            limit
          )
          .value
          .flatMap {
            case Right(claims) => IO.pure(claims)
            case Left(error)   => IO.raiseError(new IllegalStateException(s"outbox claim failed: $error"))
          }
      }
    } { claim =>
      val record = ProducerRecord(config.topic, claim.partitionKey, claim.envelopeBytes)
      publishClaim(config, outbox, diagnostics, claim, clock)(
        producer.produceWithoutOffsets(ProducerRecords.one(record)).void
      )
    }

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
            IO.raiseError(new IllegalStateException("outbox claim exceeded requested publication wave"))
          case claims => publishClaims(claims)(publish) *> loop(remaining - claims.size)
        }
      }
    loop(batchSize)
  }

  private[kafka] def publishClaim(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository,
      diagnostics: Diagnostics,
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
            new IllegalStateException("outbox lease heartbeat stopped before Kafka send completed")
          )
      }
      .flatMap {
        case Right(_) =>
          clock.realTimeInstant.flatMap(done =>
            requireOutboxSuccess(
              outbox.markPublished(claim.event.eventId, claim.leaseToken, done, done.plusSeconds(7.days.toSeconds))
            )
          )
        case Left(error) if isProducerFenced(error) => IO.raiseError(ProducerGenerationFenced(error))
        case Left(error)                            =>
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

  private def requireOutboxSuccess(result: RepositoryIO[Unit]): IO[Unit] =
    result.value.flatMap {
      case Right(())   => IO.unit
      case Left(error) => IO.raiseError(new IllegalStateException(s"outbox operation failed: $error"))
    }

  /** Operational facts may arrive out of order; Kafka preserves append order within each partition. */
  private[kafka] def publishClaims(
      claims: List[ClaimedOperationalEvent]
  )(publish: ClaimedOperationalEvent => IO[Unit]): IO[Unit] =
    Stream.emits(claims).covary[IO].parEvalMapUnordered(PublicationWaveSize)(publish).compile.drain

  private[kafka] def consumerResource(
      config: KafkaConfig,
      receipts: ConsumerReceiptRepository,
      quarantine: EventQuarantineRepository,
      diagnostics: Diagnostics,
      clock: Clock[IO] = Clock[IO]
  ): Resource[IO, Unit] = {
    val settings = KafkaClientSettings
      .consumer(
        config.bootstrapServers,
        config.consumer.saslUsername,
        config.consumer.saslPassword,
        config.saslSecurityProtocol
      )
      .withGroupId(config.consumerGroup)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withEnableAutoCommit(false)
      .withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")

    BackgroundWorker.resource(ConsumerWorker, diagnostics)(
      resilientStream(
        diagnostics,
        KafkaPartitionProcessing(
          KafkaConsumer.stream(settings).subscribeTo(config.topic).partitionedRecords,
          config.consumer.partitionConcurrency
        ) { message =>
          val record = message.record
          processRecordBeforeCommit(record.topic, record.partition, record.offset)(
            handleRecord(
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
      ).compile.drain
    )
  }

  /** Compatibility entry point for the interview runtime; the builder itself is [[KafkaClientSettings]]. */
  private[kafka] def saslProperties(
      username: Option[String],
      password: Option[String],
      protocol: KafkaSaslSecurityProtocol = KafkaSaslSecurityProtocol.Tls
  ): Map[String, String] = KafkaClientSettings.security(username, password, protocol)

  private[kafka] def handleRecord(
      config: KafkaConfig,
      receipts: ConsumerReceiptRepository,
      quarantine: EventQuarantineRepository,
      topic: String,
      partition: Int,
      offset: Long,
      bytes: Option[Array[Byte]],
      clock: Clock[IO] = Clock[IO]
  ): IO[Boolean] =
    clock.realTimeInstant.flatMap { now =>
      bytes.toRight("MalformedEnvelope").flatMap(OperationalEventJson.decode) match {
        case Left(_) =>
          quarantineRecord(
            config,
            quarantine,
            topic,
            partition,
            offset,
            OperationalEventFailureCategory.MalformedEnvelope,
            bytes.fold("null event envelope")(_ => "malformed event envelope"),
            bytes.getOrElse(Array.emptyByteArray),
            now
          ).value.map(_.isRight)
        case Right(event) =>
          receipts
            .record(
              config.consumerGroup,
              event,
              now,
              now.plusSeconds(config.consumer.receiptTtlDays.days.toSeconds)
            )
            .value
            .map(_.isRight)
      }
    }

  private[kafka] def processRecordBeforeCommit(
      topic: String,
      partition: Int,
      offset: Long
  )(process: IO[Boolean])(commit: IO[Unit]): IO[Unit] =
    process.flatMap {
      case true => commit
      // Stop this stream before reading a later offset. Continuing with false here could
      // commit N+1 and make the undurable record at N unrecoverable for this group.
      case false => IO.raiseError(UndurableRecord(topic, partition, offset))
    }

  private def quarantineRecord(
      config: KafkaConfig,
      quarantine: EventQuarantineRepository,
      topic: String,
      partition: Int,
      offset: Long,
      category: OperationalEventFailureCategory,
      reason: String,
      bytes: Array[Byte],
      now: Instant
  ): RepositoryIO[Unit] =
    quarantine.save(
      EventQuarantineRecord(
        topic,
        partition,
        offset,
        category,
        reason,
        Option(bytes).getOrElse(Array.emptyByteArray),
        now,
        now.plusSeconds(config.consumer.quarantineTtlDays.days.toSeconds)
      )
    )

  private[kafka] def resilientStream(
      diagnostics: Diagnostics,
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

  private[kafka] def restartPolicy(baseDelay: FiniteDuration, maxDelay: FiniteDuration): retry.RetryPolicy[IO, Any] =
    RetryPolicies.capDelay(maxDelay, RetryPolicies.fullJitter[IO](baseDelay))

  /** Client exception messages can embed rejected configuration values, so only class names reach the outbox. */
  private[kafka] def sanitized(error: Throwable): String =
    Iterator
      .iterate(Option(error))(_.flatMap(value => Option(value.getCause).filterNot(_ eq value)))
      .takeWhile(_.nonEmpty)
      .flatten
      .take(5)
      .map(_.getClass.getName)
      .mkString(" <- ")
      .take(512)

  private final case class UndurableRecord(topic: String, partition: Int, offset: Long)
      extends RuntimeException(s"Kafka record could not be durably processed: $topic-$partition@$offset")

  private[kafka] def isProducerFenced(error: Throwable): Boolean =
    Iterator
      .iterate(Option(error))(_.flatMap(value => Option(value.getCause)))
      .takeWhile(_.nonEmpty)
      .flatten
      .exists(value => value.isInstanceOf[ProducerFencedException] || value.isInstanceOf[InvalidProducerEpochException])

  private final case class ProducerGenerationFenced(cause: Throwable)
      extends RuntimeException("transactional producer generation was fenced", cause)

}
