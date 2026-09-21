package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.config.KafkaConfig
import com.example.graphQL.cats.repository.protocol.{
  ClaimedOperationalEvent, ConsumerReceiptRepository, EventQuarantineRecord, EventQuarantineRepository,
  OperationalEventFailureCategory, OperationalEventOutboxRepository
}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogFields, RepositoryError}
import com.example.graphQL.cats.shared.events.{OperationalAggregateType, OperationalEventEnvelope, OperationalEventJson}
import fs2.Stream
import fs2.kafka.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import retry.{HandlerDecision, RetryPolicies, retryingOnErrors}

import java.time.Instant
import scala.concurrent.duration.*

object OperationalEventKafkaRuntime {
  def resource(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository[IO],
      receipts: ConsumerReceiptRepository[IO],
      quarantine: EventQuarantineRepository[IO],
      diagnostics: Diagnostics = Diagnostics.noop
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
      outbox: OperationalEventOutboxRepository[IO],
      diagnostics: Diagnostics
  ): Resource[IO, Unit] = {
    val settings =
      ProducerSettings(
        keySerializer = Serializer[IO, String],
        valueSerializer = Serializer[IO, Array[Byte]]
      )
        .withBootstrapServers(config.bootstrapServers)
        .withProperty(ProducerConfig.ACKS_CONFIG, "all")
        .withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    KafkaProducer.resource(settings).flatMap { producer =>
      background(
        resilientStream(
          diagnostics,
          Stream.awakeEvery[IO](config.publisher.pollIntervalMillis.millis)
            .evalMap(_ => publishBatch(config, outbox, producer)),
          config.publisher.retryDelaySeconds.seconds
        )
      )
    }
  }

  private def publishBatch(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository[IO],
      producer: KafkaProducer[IO, String, Array[Byte]]
  ): IO[Unit] =
    IO.realTimeInstant.flatMap { now =>
      val leaseUntil = now.plusSeconds(config.publisher.leaseSeconds.toLong)
      outbox.claim(config.publisher.workerId, now, leaseUntil, config.publisher.batchSize).flatMap {
        case Left(_) => IO.unit
        case Right(claims) =>
          publishClaims(claims) { claim =>
            val record = ProducerRecord(config.topic, claim.partitionKey, claim.envelopeBytes)
            producer.produce(ProducerRecords.one(record)).flatten.attempt.flatMap {
              case Right(_) =>
                IO.realTimeInstant.flatMap(done =>
                  outbox.markPublished(claim.event.eventId, claim.leaseToken, done, done.plusSeconds(7.days.toSeconds)).void)
              case Left(error) =>
                IO.realTimeInstant.flatMap { failedAt =>
                  if (claim.attempts >= config.publisher.maxAttempts)
                    outbox.markFailed(claim.event.eventId, claim.leaseToken, failedAt, sanitized(error)).void
                  else
                    outbox.releaseForRetry(claim.event.eventId, claim.leaseToken, failedAt,
                      failedAt.plusSeconds(config.publisher.retryDelaySeconds.toLong)).void
                }
            }
          }
      }
    }

  /** Preserve ordering for one Kafka key while overlapping independent keys. */
  private[kafka] def publishClaims(
      claims: List[ClaimedOperationalEvent]
  )(publish: ClaimedOperationalEvent => IO[Unit]): IO[Unit] =
    claims.groupBy(_.partitionKey).values.toList.parTraverse_(_.traverse_(publish))

  private def consumerResource(
      config: KafkaConfig,
      receipts: ConsumerReceiptRepository[IO],
      quarantine: EventQuarantineRepository[IO],
      diagnostics: Diagnostics
  ): Resource[IO, Unit] = {
    val settings =
      ConsumerSettings(
        keyDeserializer = Deserializer[IO, String],
        valueDeserializer = Deserializer[IO, Array[Byte]]
      )
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(config.consumerGroup)
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withEnableAutoCommit(false)
        .withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")

    background(
      resilientStream(
        diagnostics,
        KafkaConsumer.stream(settings)
          .subscribeTo(config.topic)
          .records
          .evalMap { message =>
            val record = message.record
            handleRecord(config, receipts, quarantine, record.topic, record.partition, record.offset, record.value)
              .flatMap(commit => if (commit) message.offset.commit else IO.unit)
          },
        1.second
      )
    )
  }

  private[kafka] def handleRecord(
      config: KafkaConfig,
      receipts: ConsumerReceiptRepository[IO],
      quarantine: EventQuarantineRepository[IO],
      topic: String,
      partition: Int,
      offset: Long,
      bytes: Array[Byte]
  ): IO[Boolean] =
    IO.realTimeInstant.flatMap { now =>
      OperationalEventJson.decode(bytes) match {
        case Left("UnsupportedVersion") =>
          quarantineRecord(config, quarantine, topic, partition, offset, OperationalEventFailureCategory.UnsupportedVersion,
            "unsupported schema version", bytes, now).as(false)
        case Left(_) =>
          quarantineRecord(config, quarantine, topic, partition, offset, OperationalEventFailureCategory.MalformedEnvelope,
            "malformed event envelope", bytes, now).as(false)
        case Right(event) if invalidOrdering(event) =>
          quarantineRecord(config, quarantine, topic, partition, offset, OperationalEventFailureCategory.InvalidOrdering,
            "invalid aggregate ordering metadata", bytes, now).as(false)
        case Right(event) =>
          receipts.exists(config.consumerGroup, event.eventId).flatMap {
            case Right(true) => IO.pure(true)
            case Right(false) =>
              validateReceiptOrder(config, receipts, event).flatMap {
                case Left(OperationalEventFailureCategory.InvalidOrdering) =>
                  quarantineRecord(config, quarantine, topic, partition, offset, OperationalEventFailureCategory.InvalidOrdering,
                    "invalid aggregate ordering metadata", bytes, now).as(false)
                case Left(_) => IO.pure(false)
                case Right(()) =>
                  receipts.record(config.consumerGroup, event, now, now.plusSeconds(config.consumer.receiptTtlDays.days.toSeconds)).map {
                    case Right(_) => true
                    case Left(RepositoryError.Conflict) => true
                    case Left(_) => false
                  }
              }
            case Left(_) => IO.pure(false)
          }
      }
    }

  private[kafka] def validateReceiptOrder(
      config: KafkaConfig,
      receipts: ConsumerReceiptRepository[IO],
      event: OperationalEventEnvelope
  ): IO[Either[OperationalEventFailureCategory, Unit]] =
    event.aggregateType match {
      case OperationalAggregateType.Search => IO.pure(Right(()))
      case OperationalAggregateType.Application | OperationalAggregateType.Job =>
        receipts.latestSequence(config.consumerGroup, event.aggregateType.toString, event.aggregateId).map {
          case Right(None) if firstObservedEventAllowed(event) => Right(())
          case Right(Some(previous)) if event.sequence == previous + 1L => Right(())
          case Right(_) => Left(OperationalEventFailureCategory.InvalidOrdering)
          case Left(_) => Left(OperationalEventFailureCategory.ConsumerFailure)
        }
    }

  private def firstObservedEventAllowed(event: OperationalEventEnvelope): Boolean =
    event.sequence >= 0L && event.eventType != com.example.graphQL.cats.shared.events.OperationalEventType.CANDIDATE_HIRED

  private def quarantineRecord(
      config: KafkaConfig,
      quarantine: EventQuarantineRepository[IO],
      topic: String,
      partition: Int,
      offset: Long,
      category: OperationalEventFailureCategory,
      reason: String,
      bytes: Array[Byte],
      now: Instant
  ): IO[Unit] =
    quarantine.save(EventQuarantineRecord(
      topic,
      partition,
      offset,
      category,
      reason,
      bytes,
      now,
      now.plusSeconds(config.consumer.quarantineTtlDays.days.toSeconds)
    )).void

  private def invalidOrdering(event: OperationalEventEnvelope): Boolean =
    event.aggregateType match {
      case OperationalAggregateType.Application | OperationalAggregateType.Job => event.aggregateVersion < 0L || event.sequence < 0L
      case OperationalAggregateType.Search => event.sequence < 1L
    }

  private[kafka] def resilientStream(
      diagnostics: Diagnostics,
      stream: Stream[IO, Unit],
      baseDelay: FiniteDuration
  ): Stream[IO, Unit] =
    Stream.eval(
      retryingOnErrors(stream.compile.drain)(
        policy = RetryPolicies.fullJitter[IO](baseDelay),
        errorHandler = (error, _) =>
          Diagnostics.emit(diagnostics, LogEvent.RuntimeFailed, fields = LogFields.failure(error)).as(HandlerDecision.Continue)
      )
    )

  private def background(stream: Stream[IO, ?]): Resource[IO, Unit] =
    Resource.make(stream.compile.drain.start)(_.cancel).void

  private def sanitized(error: Throwable): String =
    Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getSimpleName)
}
