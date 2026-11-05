package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Resource}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.config.{
  InterviewRuntimeConfig,
  KafkaConfig,
  KafkaSaslMechanism,
  KafkaSaslSecurityProtocol
}
import com.example.graphQL.cats.domain.workflow.InterviewTopicPair
import com.example.graphQL.cats.service.{BackgroundWorker, Diagnostics, WorkerAlive}
import com.example.graphQL.cats.service.port.{
  InterviewMessage,
  InterviewProducerGenerationFenced,
  InterviewPublisherGeneration,
  ProducerGenerationId,
  ProducerWorkerId,
  InterviewPublisherRole,
  InterviewResult,
  InterviewStep,
  InterviewTransport,
  RepositoryF
}
import com.example.graphQL.cats.infrastructure.events.{InterviewMessageHeaders, OperationalEventHeaders}
import com.example.graphQL.cats.infrastructure.telemetry.KafkaClientMetrics
import com.example.graphQL.cats.shared.crypto.SourceHash
import fs2.kafka.*
import fs2.kafka.producer.MkProducer
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.*
import io.circe.parser.decode
import io.circe.syntax.*

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.HexFormat
import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

object InterviewMessageCodec {
  given Encoder[InterviewStep] = Encoder.encodeString.contramap(_.toString)
  given Decoder[InterviewStep] =
    Decoder.decodeString.emap(value => InterviewStep.values.find(_.toString == value).toRight("invalid interview step"))
  given Encoder[InterviewResult] = Encoder.encodeString.contramap(_.toString)
  given Decoder[InterviewResult] = Decoder.decodeString.emap(value =>
    InterviewResult.values.find(_.toString == value).toRight("invalid interview result")
  )
  given Encoder[InterviewMessage] = deriveEncoder
  given Decoder[InterviewMessage] = deriveDecoder

  /** The largest record either side of the transport will produce or accept. */
  val MaxRecordBytes: Int = 65536
  def bytes(message: InterviewMessage): Array[Byte] = message.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)

  /** The wire bytes of an outgoing message, refused when they exceed the record limit. */
  def encode(message: InterviewMessage): Either[String, Array[Byte]] =
    Some(bytes(message)).filter(_.length <= MaxRecordBytes).toRight("interview record exceeds size limit")

  /** An outgoing message the transport refuses to produce. */
  final case class RecordRejected(reason: String) extends RuntimeException(reason) with NoStackTrace

  def parse(bytes: Array[Byte]): Either[String, InterviewMessage] =
    if (Option(bytes).isEmpty) Left("invalid null interview message")
    else if (bytes.length > MaxRecordBytes) Left("interview record exceeds size limit")
    else
      Some(new String(bytes, StandardCharsets.UTF_8))
        .toRight("invalid interview message")
        .flatMap(text =>
          decode[InterviewMessage](text).left
            .map(_ => "invalid interview message")
            .flatMap(message =>
              Either.cond(!InterviewMessageHeaders.hasDuplicateKeys(text), message, "interview message repeats a key")
            )
        )
        .flatMap(message =>
          Either.cond(
            message.revision >= 0 && message.stepId.nonEmpty && message.stepId.length <= 200,
            message,
            "invalid interview metadata"
          )
        )
}

final case class InterviewKafkaConfig(
    bootstrapServers: String,
    username: String,
    password: String,
    protocol: KafkaSaslSecurityProtocol,
    worker: Boolean,
    partitionConcurrency: Int = InterviewKafkaConfig.Defaults.partitionConcurrency,
    topics: InterviewTopicPair = InterviewKafkaConfig.Defaults.topics,
    workerGroup: String = InterviewKafkaConfig.Defaults.workerGroup,
    orchestratorGroup: String = InterviewKafkaConfig.Defaults.orchestratorGroup,
    restartMaxDelaySeconds: Int = KafkaConfig.DefaultRestartMaxDelaySeconds,
    workerId: ProducerWorkerId = InterviewKafkaConfig.Defaults.workerId,
    mechanism: KafkaSaslMechanism = KafkaSaslMechanism.ScramSha512,
    maxPollRecords: Int = InterviewKafkaConfig.Defaults.maxPollRecords,
    maxPollIntervalSeconds: Int = InterviewKafkaConfig.Defaults.maxPollIntervalSeconds,
    commitBatchSize: Int = InterviewKafkaConfig.Defaults.commitBatchSize,
    commitBatchIntervalMillis: Int = InterviewKafkaConfig.Defaults.commitBatchIntervalMillis,
    claimSeconds: Int = InterviewKafkaConfig.Defaults.claimSeconds
) {
  override def toString: String = s"InterviewKafkaConfig($workerId, $worker, [REDACTED])"
}

object InterviewKafkaConfig {
  private[kafka] val Defaults: InterviewRuntimeConfig = InterviewRuntimeConfig()
}

object InterviewKafkaRuntime {

  private[kafka] def rejectionIdentity(
      topic: String,
      partition: Int,
      offset: Long,
      payload: Option[Array[Byte]],
      reason: String
  ): String = {
    val digest = HexFormat
      .of()
      .formatHex(
        MessageDigest.getInstance("SHA-256").digest(payload.getOrElse(Array.emptyByteArray))
      )
    "transport:" + SourceHash.sha256(s"$topic:$partition:$offset:$digest:$reason")
  }

  /** The decoded message when the body, the workflow key and the present contract headers agree; otherwise a closed
    * rejection reason.
    */
  private[kafka] def accept(
      key: String,
      headers: OperationalEventHeaders.RecordHeaders,
      value: Array[Byte]
  ): Either[String, InterviewMessage] =
    InterviewMessageCodec
      .parse(value)
      .flatMap(message => Either.cond(message.workflowId.toString == key, message, "workflow key mismatch"))
      .flatMap(message =>
        InterviewMessageHeaders.verifyInterview(headers, message.step.toString).left.map(headerReason).as(message)
      )

  /** A closed reason per header refusal, so a quarantined record never echoes header content. */
  private[kafka] def headerReason(refusal: OperationalEventHeaders.HeaderRefusal): String =
    refusal match {
      case OperationalEventHeaders.HeaderRefusal.UnsupportedVersion    => "unsupported interview schema version"
      case OperationalEventHeaders.HeaderRefusal.ContentTypeMismatch   => "interview content type mismatch"
      case OperationalEventHeaders.HeaderRefusal.InterviewStepMismatch => "interview step header mismatch"
      case OperationalEventHeaders.HeaderRefusal.MalformedHeaders      => "malformed interview headers"
      case _                                                           => "invalid interview headers"
    }

  /** The callback succeeds once the inbox, state, outgoing intent or bounded quarantine has committed durably. A
    * failure tears down the consumer before any later offset can be committed.
    */
  def resource(
      config: InterviewKafkaConfig,
      diagnostics: Diagnostics[IO]
  )(
      receive: Either[String, InterviewMessage] => RepositoryF[IO, Unit]
  ): Resource[IO, InterviewTransport[IO]] =
    consumerResource(config, diagnostics)(receive) *> publisherResource(config, diagnostics)

  /** Producer initialization completes before the immutable generation is exposed for Mongo authorization. */
  def publisherResource(
      config: InterviewKafkaConfig,
      diagnostics: Diagnostics[IO] = Diagnostics.noop,
      uuidGen: UUIDGen[IO] = UUIDGen[IO],
      metrics: KafkaClientMetrics = KafkaClientMetrics.noop
  ): Resource[IO, InterviewTransport[IO]] = {
    val producerSettings = KafkaClientSettings
      .producer(
        config.bootstrapServers,
        Some(config.username),
        Some(config.password),
        config.protocol,
        InterviewMessageCodec.MaxRecordBytes,
        config.mechanism,
        config.claimSeconds
      )
      .withCloseTimeout(5.seconds)
    val role = if (config.worker) InterviewPublisherRole.Worker else InterviewPublisherRole.Orchestrator
    val output = if (config.worker) config.topics.results else config.topics.commands
    for {
      id <- Resource.eval(uuidGen.randomUUID)
      generation = InterviewPublisherGeneration(role, config.workerId, ProducerGenerationId(id))
      producer <- KafkaRuntimeSupport.observedGeneration(
        generation.transactionalId,
        generation.generationId.value,
        diagnostics
      )(
        GuardedTransactionalProducer.resource(
          TransactionalProducerSettings(generation.transactionalId, producerSettings),
          MkProducer.mkProducerForSync[IO]
        )
      )
    } yield new InterviewTransport[IO] {
      def generationFor(_message: InterviewMessage): InterviewPublisherGeneration = generation
      def publish(message: InterviewMessage): IO[Unit] =
        IO.fromEither(InterviewMessageCodec.encode(message).leftMap(InterviewMessageCodec.RecordRejected(_))).flatMap {
          bytes =>
            metrics
              .timedSend(
                producer
                  .produceWithoutOffsets(
                    ProducerRecords.one(
                      ProducerRecord(output, message.workflowId.toString, bytes).withHeaders(
                        Headers.fromIterable(
                          InterviewMessageHeaders
                            .interview(message.step.toString)
                            .map((key, value) => Header(key, value))
                        )
                      )
                    )
                  )
                  .void
              )
              .adaptError {
                case error if KafkaRuntimeSupport.isProducerFenced(error) =>
                  InterviewProducerGenerationFenced(error)
              }
        }
    }
  }

  /** Consumers have independent lifetimes so replacing a fenced producer does not reset their offset frontier. */
  def consumerResource(
      config: InterviewKafkaConfig,
      diagnostics: Diagnostics[IO],
      alive: WorkerAlive[IO] = WorkerAlive.none[IO],
      metrics: KafkaClientMetrics = KafkaClientMetrics.noop
  )(
      receive: Either[String, InterviewMessage] => RepositoryF[IO, Unit]
  ): Resource[IO, Unit] = {
    val consumerSettings = KafkaClientSettings.committedConsumer(
      config.bootstrapServers,
      Some(config.username),
      Some(config.password),
      config.protocol,
      if (config.worker) config.workerGroup else config.orchestratorGroup,
      config.mechanism,
      config.maxPollRecords,
      config.maxPollIntervalSeconds
    )
    val input = if (config.worker) config.topics.commands else config.topics.results
    val group = if (config.worker) config.workerGroup else config.orchestratorGroup
    val consumer = KafkaRuntimeSupport.consume(
      consumerSettings,
      input,
      group,
      config.partitionConcurrency,
      CommitBatching(config.commitBatchSize, config.commitBatchIntervalMillis.millis),
      metrics,
      diagnostics,
      alive.every
    ) { message =>
      val parsed =
        accept(message.record.key, OperationalEventHeaders.fromKafka(message.record.headers), message.record.value)
      KafkaRuntimeSupport.processRecord(input, message.record.partition, message.record.offset)(
        receive(
          parsed.left.map(reason =>
            rejectionIdentity(
              input,
              message.record.partition,
              message.record.offset,
              Option(message.record.value),
              reason
            )
          )
        )
      )(message.offset)
    }
    BackgroundWorker.resource(InterviewConsumerWorker, diagnostics)(
      KafkaRuntimeSupport
        .resilientStream(
          diagnostics,
          consumer.concurrently(fs2.Stream.awakeEvery[IO](alive.every).evalMap(_ => alive.beat)),
          1.second,
          maxDelay = config.restartMaxDelaySeconds.seconds
        )
        .compile
        .drain
    )
  }

  private val InterviewConsumerWorker = "interview-consumer"
}
