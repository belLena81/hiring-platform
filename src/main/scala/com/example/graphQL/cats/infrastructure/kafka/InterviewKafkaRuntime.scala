package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Resource}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.config.{InterviewRuntimeConfig, KafkaConfig, KafkaSaslSecurityProtocol}
import com.example.graphQL.cats.service.{BackgroundWorker, Diagnostics}
import com.example.graphQL.cats.service.port.{
  InterviewMessage,
  InterviewStep,
  InterviewResult,
  InterviewTransport,
  InterviewPublisherRole,
  InterviewPublisherGeneration,
  InterviewProducerGenerationFenced,
  RepositoryIO
}
import fs2.kafka.*
import fs2.kafka.producer.MkProducer
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.*
import io.circe.parser.decode
import io.circe.syntax.*

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*

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

  def parse(bytes: Array[Byte]): Either[String, InterviewMessage] =
    if (Option(bytes).isEmpty) Left("invalid null interview message")
    else if (bytes.length > MaxRecordBytes) Left("interview record exceeds size limit")
    else
      decode[InterviewMessage](new String(bytes, StandardCharsets.UTF_8)).left
        .map(_ => "invalid interview message")
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
    topics: com.example.graphQL.cats.domain.workflow.InterviewTopicPair = InterviewKafkaConfig.Defaults.topics,
    workerGroup: String = InterviewKafkaConfig.Defaults.workerGroup,
    orchestratorGroup: String = InterviewKafkaConfig.Defaults.orchestratorGroup,
    restartMaxDelaySeconds: Int = KafkaConfig.DefaultRestartMaxDelaySeconds
)

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
    val digest = java.util.HexFormat
      .of()
      .formatHex(
        java.security.MessageDigest.getInstance("SHA-256").digest(payload.getOrElse(Array.emptyByteArray))
      )
    "transport:" + com.example.graphQL.cats.shared.crypto.SourceHash
      .sha256(s"$topic:$partition:$offset:$digest:$reason")
  }

  /** The callback succeeds once the inbox, state, outgoing intent or bounded quarantine has committed durably. A
    * failure tears down the consumer before any later offset can be committed.
    */
  def resource(
      config: InterviewKafkaConfig,
      diagnostics: Diagnostics
  )(
      receive: Either[String, InterviewMessage] => RepositoryIO[Unit]
  ): Resource[IO, InterviewTransport] =
    consumerResource(config, diagnostics)(receive) *> publisherResource(config, diagnostics)

  /** Producer initialization completes before the immutable generation is exposed for Mongo authorization. */
  def publisherResource(
      config: InterviewKafkaConfig,
      diagnostics: Diagnostics = Diagnostics.noop,
      uuidGen: UUIDGen[IO] = UUIDGen[IO]
  ): Resource[IO, InterviewTransport] = {
    val producerSettings = KafkaClientSettings
      .producer(
        config.bootstrapServers,
        Some(config.username),
        Some(config.password),
        config.protocol,
        InterviewMessageCodec.MaxRecordBytes
      )
      .withCloseTimeout(5.seconds)
    val role = if (config.worker) InterviewPublisherRole.Worker else InterviewPublisherRole.Orchestrator
    val output = if (config.worker) config.topics.results else config.topics.commands
    for {
      id <- Resource.eval(uuidGen.randomUUID)
      generation = InterviewPublisherGeneration(role, id)
      producer <- KafkaRuntimeSupport.observedGeneration(generation.transactionalId, diagnostics)(
        GuardedTransactionalProducer.resource(
          TransactionalProducerSettings(generation.transactionalId, producerSettings),
          MkProducer.mkProducerForSync[IO]
        )
      )
    } yield new InterviewTransport {
      def generationFor(_message: InterviewMessage): InterviewPublisherGeneration = generation
      def publish(message: InterviewMessage): IO[Unit] =
        IO.fromEither(InterviewMessageCodec.encode(message).leftMap(new IllegalArgumentException(_))).flatMap { bytes =>
          producer
            .produceWithoutOffsets(ProducerRecords.one(ProducerRecord(output, message.workflowId.toString, bytes)))
            .void
            .adaptError {
              case error if KafkaRuntimeSupport.isProducerFenced(error) =>
                InterviewProducerGenerationFenced(error)
            }
        }
    }
  }

  /** Consumers have independent lifetimes so replacing a fenced producer does not reset their offset frontier. */
  def consumerResource(config: InterviewKafkaConfig, diagnostics: Diagnostics)(
      receive: Either[String, InterviewMessage] => RepositoryIO[Unit]
  ): Resource[IO, Unit] = {
    val consumerSettings = KafkaClientSettings.committedConsumer(
      config.bootstrapServers,
      Some(config.username),
      Some(config.password),
      config.protocol,
      if (config.worker) config.workerGroup else config.orchestratorGroup
    )
    val input = if (config.worker) config.topics.commands else config.topics.results
    val consumer = KafkaPartitionProcessing(
      KafkaConsumer.stream(consumerSettings).subscribeTo(input).partitionedRecords,
      config.partitionConcurrency
    ) { message =>
      val parsed = InterviewMessageCodec
        .parse(message.record.value)
        .flatMap(value => Either.cond(value.workflowId.toString == message.record.key, value, "workflow key mismatch"))
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
      )(message.offset.commit)
    }
    BackgroundWorker.resource(InterviewConsumerWorker, diagnostics)(
      KafkaRuntimeSupport
        .resilientStream(diagnostics, consumer, 1.second, maxDelay = config.restartMaxDelaySeconds.seconds)
        .compile
        .drain
    )
  }

  private val InterviewConsumerWorker = "interview-consumer"
}
