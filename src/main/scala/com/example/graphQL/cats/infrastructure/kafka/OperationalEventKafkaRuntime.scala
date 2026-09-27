package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.config.{KafkaConfig, KafkaSaslSecurityProtocol}
import com.example.graphQL.cats.repository.protocol.{
  ClaimedOperationalEvent,
  ConsumerReceiptRepository,
  EventQuarantineRecord,
  EventQuarantineRepository,
  OperationalEventFailureCategory,
  OperationalEventOutboxRepository
}
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.shared.events.OperationalEventJson
import fs2.Stream
import fs2.kafka.*
import fs2.kafka.producer.MkProducer
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.ByteArraySerializer
import retry.{HandlerDecision, RetryPolicies, retryingOnErrors}

import java.nio.file.{Files, LinkOption, Path}
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.jdk.CollectionConverters.*
import scala.concurrent.duration.*

object OperationalEventKafkaRuntime {
  def resource(
      config: KafkaConfig,
      outbox: OperationalEventOutboxRepository,
      receipts: ConsumerReceiptRepository,
      quarantine: EventQuarantineRepository,
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
      outbox: OperationalEventOutboxRepository,
      diagnostics: Diagnostics
  ): Resource[IO, Unit] = {
    val producerFactory = PublisherCommitProof.fromEnvironment.fold(MkProducer.mkProducerForSync[IO])(_.producerFactory)
    given MkProducer[IO] = producerFactory
    val baseSettings =
      ProducerSettings(
        keySerializer = Serializer[IO, String],
        valueSerializer = Serializer[IO, Array[Byte]]
      )
        .withBootstrapServers(config.bootstrapServers)
        .withProperty(ProducerConfig.ACKS_CONFIG, "all")
        .withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        .withProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000")
        .withProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    val settings = saslProperties(
      config.publisher.saslUsername,
      config.publisher.saslPassword,
      config.saslSecurityProtocol
    )
      .foldLeft(baseSettings) { case (current, (key, value)) => current.withProperty(key, value) }

    def generation: Stream[IO, Unit] = {
      val transactionalId = "hiring-publisher-" + java.util.UUID.randomUUID().toString
      Stream
        .resource(TransactionalKafkaProducer.resource(TransactionalProducerSettings(transactionalId, settings)))
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
    IO.realTimeInstant.flatMap { now =>
      val leaseUntil = now.plusSeconds(config.publisher.leaseSeconds.toLong)
      outbox.claim(config.publisher.workerId, transactionalId, now, leaseUntil, config.publisher.batchSize).flatMap {
        case Left(error) =>
          IO.raiseError(new IllegalStateException(s"outbox claim failed: $error"))
        case Right(claims) =>
          publishClaims(claims) { claim =>
            val record = ProducerRecord(config.topic, claim.partitionKey, claim.envelopeBytes)
            val send = producer.produceWithoutOffsets(ProducerRecords.one(record)).void
            val renewEvery = (config.publisher.leaseSeconds.seconds / 3).max(1.second)
            def renewalStream: Stream[IO, Unit] =
              Stream
                .awakeEvery[IO](renewEvery)
                .evalMap { _ =>
                  IO.realTimeInstant.flatMap(now =>
                    requireOutboxSuccess(
                      outbox.renewLease(
                        claim.event.eventId,
                        claim.leaseToken,
                        claim.subjectIds,
                        now.plusSeconds(config.publisher.leaseSeconds.toLong)
                      )
                    )
                  )
                }
            val heartbeat = renewalStream.compile.drain
            IO.race(send.attempt, heartbeat)
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
                      outbox.markPublished(
                        claim.event.eventId,
                        claim.leaseToken,
                        done,
                        done.plusSeconds(7.days.toSeconds)
                      )
                    )
                  )
                case Left(error) =>
                  if (isProducerFenced(error)) IO.raiseError(ProducerGenerationFenced(error))
                  else
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
      }
    }

  private def requireOutboxSuccess(result: IO[Either[RepositoryError, Unit]]): IO[Unit] =
    result.flatMap {
      case Right(())   => IO.unit
      case Left(error) => IO.raiseError(new IllegalStateException(s"outbox operation failed: $error"))
    }

  /** Preserve ordering for one Kafka key while overlapping independent keys. */
  private[kafka] def publishClaims(
      claims: List[ClaimedOperationalEvent]
  )(publish: ClaimedOperationalEvent => IO[Unit]): IO[Unit] =
    claims.groupBy(_.partitionKey).values.toList.parTraverse_(_.traverse_(publish))

  private def consumerResource(
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
        KafkaConsumer
          .stream(settings)
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
      bytes: Array[Byte]
  ): IO[Boolean] =
    IO.realTimeInstant.flatMap { now =>
      OperationalEventJson.decode(bytes) match {
        case Left(_) =>
          quarantineRecord(
            config,
            quarantine,
            topic,
            partition,
            offset,
            OperationalEventFailureCategory.MalformedEnvelope,
            "malformed event envelope",
            bytes,
            now
          ).map(_.isRight)
        case Right(event) =>
          receipts.exists(config.consumerGroup, event.eventId).flatMap {
            case Right(true)  => IO.pure(true)
            case Right(false) =>
              receipts
                .record(
                  config.consumerGroup,
                  event,
                  now,
                  now.plusSeconds(config.consumer.receiptTtlDays.days.toSeconds)
                )
                .map {
                  case Right(_)                       => true
                  case Left(RepositoryError.Conflict) => true
                  case Left(_)                        => false
                }
            case Left(_) => IO.pure(false)
          }
      }
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
  ): IO[Either[RepositoryError, Unit]] =
    quarantine.save(
      EventQuarantineRecord(
        topic,
        partition,
        offset,
        category,
        reason,
        bytes,
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

  private[kafka] def isProducerFenced(error: Throwable): Boolean =
    Iterator
      .iterate(Option(error))(_.flatMap(value => Option(value.getCause)))
      .takeWhile(_.nonEmpty)
      .flatten
      .exists(value => value.isInstanceOf[ProducerFencedException] || value.isInstanceOf[InvalidProducerEpochException])

  private final case class ProducerGenerationFenced(cause: Throwable)
      extends RuntimeException("transactional producer generation was fenced", cause)

  /** Local Compose proof gate. The real Kafka send has completed when fs2-kafka invokes commitTransaction. */
  private final class PublisherCommitProof private (directory: Path) {
    private val armed = directory.resolve("armed")
    private val held = directory.resolve("held")
    private val release = directory.resolve("release")
    private val result = directory.resolve("result")
    private val maxWaitNanos = 120.seconds.toNanos
    private val heldOnce = new AtomicBoolean(false)

    val producerFactory: MkProducer[IO] = new MkProducer[IO] {
      override def apply[G[_]](settings: ProducerSettings[G, ?, ?]): IO[KafkaByteProducer] = IO.delay {
        new org.apache.kafka.clients.producer.KafkaProducer[Array[Byte], Array[Byte]](
          settings.properties.asJava,
          new ByteArraySerializer,
          new ByteArraySerializer
        ) {
          override def commitTransaction(): Unit = {
            val hold = Files.exists(armed) && heldOnce.compareAndSet(false, true)
            if (hold) {
              Files.writeString(held, "open-transaction-before-commit")
              val deadline = System.nanoTime() + maxWaitNanos
              while (!Files.exists(release) && System.nanoTime() < deadline)
                TimeUnit.MILLISECONDS.sleep(50)
              if (!Files.exists(release)) {
                Files.writeString(result, "timeout")
                throw new IllegalStateException("local publisher proof release timed out")
              }
            }
            try {
              super.commitTransaction()
              if (hold) {
                val _ = Files.writeString(result, "committed")
              }
            } catch {
              case error: Throwable =>
                if (hold) {
                  val _ = Files.writeString(result, if (isProducerFenced(error)) "fenced" else "other-failure")
                }
                throw error
            }
          }
        }
      }
    }
  }

  private object PublisherCommitProof {
    def fromEnvironment: Option[PublisherCommitProof] =
      sys.env.get("HIRING_ACCOUNT_DELETION_PUBLISHER_PROOF_DIR").map { raw =>
        val root = Path.of(".local/data/account-deletion-compose-proof").toAbsolutePath.normalize()
        val directory = Path.of(raw).toAbsolutePath.normalize()
        require(directory.startsWith(root) && directory.getFileName.toString == "publisher")
        require(Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS))
        require(directory.toRealPath() == directory)
        new PublisherCommitProof(directory)
      }
  }
}
