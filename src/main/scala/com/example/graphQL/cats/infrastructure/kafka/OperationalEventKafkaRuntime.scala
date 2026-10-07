package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Resource}
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
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.service.events.OperationalEventJson
import fs2.Stream
import fs2.kafka.*
import fs2.kafka.producer.MkProducer
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import retry.{HandlerDecision, RetryPolicies, retryingOnErrors}

import java.time.Instant
import scala.concurrent.duration.*

object OperationalEventKafkaRuntime {
  private val PublicationWaveSize = 4
  private val OperationalRequestBytes = 1048576

  def resource(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository,
      receipts: ConsumerReceiptRepository,
      quarantine: EventQuarantineRepository,
      diagnostics: Diagnostics
  ): Resource[IO, Unit] =
    if (!config.enabled) Resource.unit
    else {
      val publisher = publisherResource(config, outbox, diagnostics)
      val consumer =
        if (config.consumer.enabled) consumerResource(config, receipts, quarantine, diagnostics)
        else Resource.unit
      publisher *> consumer
    }

  private def publisherResource(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository,
      diagnostics: Diagnostics
  ): Resource[IO, Unit] = {
    val baseSettings =
      ProducerSettings(
        keySerializer = Serializer[IO, String],
        valueSerializer = Serializer[IO, Array[Byte]]
      )
        .withBootstrapServers(config.bootstrapServers)
        .withProperty(ProducerConfig.ACKS_CONFIG, "all")
        .withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        .withProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, OperationalRequestBytes.toString)
        .withProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000")
        .withProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    val settings = saslProperties(
      config.publisher.saslUsername,
      config.publisher.saslPassword,
      config.saslSecurityProtocol
    )
      .foldLeft(baseSettings) { case (current, (key, value)) => current.withProperty(key, value) }

    def generation: Stream[IO, Unit] =
      Stream.eval(UUIDGen[IO].randomUUID).flatMap { id =>
        val transactionalId = "hiring-publisher-" + id.toString
        Stream
          .resource(
            GuardedTransactionalProducer
              .resource(TransactionalProducerSettings(transactionalId, settings), MkProducer.mkProducerForSync[IO])
          )
          .flatMap { producer =>
            resilientStream(
              diagnostics,
              Stream
                .awakeEvery[IO](config.publisher.pollIntervalMillis.millis)
                .evalMap(_ => publishBatch(config, outbox, diagnostics, transactionalId, producer)),
              config.publisher.retryDelaySeconds.seconds,
              stopRetrying = isProducerFenced
            )
          }
          .handleErrorWith {
            case _: ProducerGenerationFenced => Stream.eval(diagnostics.emit(LogEvent.RuntimeFailed))
            case error                       =>
              Stream.eval(diagnostics.emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error))) ++
                Stream.sleep_[IO](config.publisher.retryDelaySeconds.seconds)
          }
      }

    background(Stream.suspend(generation).repeat)
  }

  private def publishBatch(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository,
      diagnostics: Diagnostics,
      transactionalId: String,
      producer: TransactionalKafkaProducer.WithoutOffsets[IO, String, Array[Byte]]
  ): IO[Unit] =
    publishWaves(config.publisher.batchSize) { limit =>
      IO.realTimeInstant.flatMap { now =>
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
      publishClaim(config, outbox, diagnostics, claim)(producer.produceWithoutOffsets(ProducerRecords.one(record)).void)
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
      claim: ClaimedOperationalEvent
  )(send: IO[Unit]): IO[Unit] = {
    def renew: IO[Unit] = IO.realTimeInstant.flatMap { now =>
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
          IO.realTimeInstant.flatMap(done =>
            requireOutboxSuccess(
              outbox.markPublished(claim.event.eventId, claim.leaseToken, done, done.plusSeconds(7.days.toSeconds))
            )
          )
        case Left(error) if isProducerFenced(error) => IO.raiseError(ProducerGenerationFenced(error))
        case Left(error)                            =>
          diagnostics.emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error)) *> IO.realTimeInstant
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
      diagnostics: Diagnostics
  ): Resource[IO, Unit] = {
    val baseSettings =
      ConsumerSettings(
        keyDeserializer = Deserializer[IO, String],
        valueDeserializer = Deserializer[IO, Array[Byte]]
      )
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(config.consumerGroup)
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withEnableAutoCommit(false)
        .withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    val settings = saslProperties(
      config.consumer.saslUsername,
      config.consumer.saslPassword,
      config.saslSecurityProtocol
    )
      .foldLeft(baseSettings) { case (current, (key, value)) => current.withProperty(key, value) }

    background(
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
              Option(record.value)
            )
          )(message.offset.commit)
        },
        1.second
      )
    )
  }

  private[kafka] def saslProperties(
      username: Option[String],
      password: Option[String],
      protocol: KafkaSaslSecurityProtocol = KafkaSaslSecurityProtocol.Tls
  ): Map[String, String] =
    (username, password) match {
      case (Some(user), Some(secret)) =>
        Map(
          "security.protocol" -> protocol.kafkaValue,
          "sasl.mechanism" -> "PLAIN",
          "sasl.jaas.config" ->
            s"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${jaasEscape(user)}\" password=\"${jaasEscape(secret)}\";"
        )
      case _ => Map.empty
    }

  private def jaasEscape(value: String): String = value.replace("\\", "\\\\").replace("\"", "\\\"")

  private[kafka] def handleRecord(
      config: KafkaConfig,
      receipts: ConsumerReceiptRepository,
      quarantine: EventQuarantineRepository,
      topic: String,
      partition: Int,
      offset: Long,
      bytes: Option[Array[Byte]]
  ): IO[Boolean] =
    IO.realTimeInstant.flatMap { now =>
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
      stopRetrying: Throwable => Boolean = _ => false
  ): Stream[IO, Unit] =
    Stream.eval(
      retryingOnErrors(stream.compile.drain)(
        policy = RetryPolicies.fullJitter[IO](baseDelay),
        errorHandler = (error, _) =>
          diagnostics
            .emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error))
            .as(if (stopRetrying(error)) HandlerDecision.Stop else HandlerDecision.Continue)
      )
    )

  private def background(stream: Stream[IO, ?]): Resource[IO, Unit] =
    Resource.make(stream.compile.drain.start)(_.cancel).void

  private def sanitized(error: Throwable): String =
    Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getSimpleName)

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
