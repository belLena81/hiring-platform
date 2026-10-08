package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.config.KafkaSaslSecurityProtocol
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.{
  InterviewMessage,
  InterviewStep,
  InterviewResult,
  InterviewTransport,
  InterviewPublisherRole,
  InterviewPublisherGeneration,
  InterviewProducerGenerationFenced
}
import fs2.kafka.*
import fs2.kafka.producer.MkProducer
import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.*
import io.circe.parser.decode
import io.circe.syntax.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*

object InterviewMessageCodec {
  val CommandsTopic = "hiring.interview-commands"
  val ResultsTopic = "hiring.interview-results"
  given Encoder[InterviewStep] = Encoder.encodeString.contramap(_.toString)
  given Decoder[InterviewStep] =
    Decoder.decodeString.emap(value => InterviewStep.values.find(_.toString == value).toRight("invalid interview step"))
  given Encoder[InterviewResult] = Encoder.encodeString.contramap(_.toString)
  given Decoder[InterviewResult] = Decoder.decodeString.emap(value =>
    InterviewResult.values.find(_.toString == value).toRight("invalid interview result")
  )
  given Encoder[InterviewMessage] = deriveEncoder
  given Decoder[InterviewMessage] = deriveDecoder
  def bytes(message: InterviewMessage): Array[Byte] = message.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)
  def parse(bytes: Array[Byte]): Either[String, InterviewMessage] =
    if (Option(bytes).isEmpty) Left("invalid null interview message")
    else if (bytes.length > 65536) Left("interview record exceeds size limit")
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
    partitionConcurrency: Int = 4,
    topics: com.example.graphQL.cats.domain.workflow.InterviewTopicPair =
      com.example.graphQL.cats.domain.workflow.InterviewTopicPair.Default,
    workerGroup: String = "hiring-interview-workers",
    orchestratorGroup: String = "hiring-interview-orchestrator",
    restartMaxDelaySeconds: Int = 30
)

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

  /** Callback true means inbox/state/outgoing intent or bounded quarantine has committed durably. A false response
    * tears down the consumer before any later offset can be committed.
    */
  def resource(
      config: InterviewKafkaConfig,
      diagnostics: Diagnostics
  )(
      receive: Either[String, InterviewMessage] => IO[Boolean]
  ): Resource[IO, InterviewTransport] =
    consumerResource(config, diagnostics)(receive) *> publisherResource(config, diagnostics)

  /** Producer initialization completes before the immutable generation is exposed for Mongo authorization. */
  def publisherResource(
      config: InterviewKafkaConfig,
      diagnostics: Diagnostics = Diagnostics.noop
  ): Resource[IO, InterviewTransport] = {
    val properties = OperationalEventKafkaRuntime.saslProperties(
      Some(config.username),
      Some(config.password),
      config.protocol
    )
    val producerSettings = properties.foldLeft(
      ProducerSettings(Serializer[IO, String], Serializer[IO, Array[Byte]])
        .withBootstrapServers(config.bootstrapServers)
        .withCloseTimeout(5.seconds)
        .withProperty(ProducerConfig.ACKS_CONFIG, "all")
        .withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        .withProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "65536")
        .withProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000")
        .withProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    ) { case (current, (key, value)) => current.withProperty(key, value) }
    val role = if (config.worker) InterviewPublisherRole.Worker else InterviewPublisherRole.Orchestrator
    val output = if (config.worker) config.topics.results else config.topics.commands
    for {
      id <- Resource.eval(IO.randomUUID)
      generation = InterviewPublisherGeneration(role, id)
      producer <- OperationalEventKafkaRuntime.observedGeneration(generation.transactionalId, diagnostics)(
        initializedProducer(
          TransactionalProducerSettings(generation.transactionalId, producerSettings),
          MkProducer.mkProducerForSync[IO]
        )
      )
    } yield new InterviewTransport {
      def generationFor(_message: InterviewMessage): InterviewPublisherGeneration = generation
      def publish(message: InterviewMessage): IO[Unit] = {
        val bytes = InterviewMessageCodec.bytes(message)
        if (bytes.length > 65536) IO.raiseError(new IllegalArgumentException("interview record exceeds size limit"))
        else
          producer
            .produceWithoutOffsets(ProducerRecords.one(ProducerRecord(output, message.workflowId.toString, bytes)))
            .void
            .handleErrorWith { error =>
              if (OperationalEventKafkaRuntime.isProducerFenced(error))
                IO.raiseError(InterviewProducerGenerationFenced(error))
              else IO.raiseError(error)
            }
      }
    }
  }

  private[kafka] def initializedProducer(
      settings: TransactionalProducerSettings[IO, String, Array[Byte]],
      factory: MkProducer[IO]
  ): Resource[IO, TransactionalKafkaProducer.WithoutOffsets[IO, String, Array[Byte]]] =
    GuardedTransactionalProducer.resource(settings, factory)

  /** Consumers have independent lifetimes so replacing a fenced producer does not reset their offset frontier. */
  def consumerResource(config: InterviewKafkaConfig, diagnostics: Diagnostics)(
      receive: Either[String, InterviewMessage] => IO[Boolean]
  ): Resource[IO, Unit] = {
    val properties =
      OperationalEventKafkaRuntime.saslProperties(Some(config.username), Some(config.password), config.protocol)
    val consumerSettings = properties.foldLeft(
      ConsumerSettings(Deserializer[IO, String], Deserializer[IO, Array[Byte]])
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(if (config.worker) config.workerGroup else config.orchestratorGroup)
        .withEnableAutoCommit(false)
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    ) { case (current, (key, value)) => current.withProperty(key, value) }
    val input = if (config.worker) config.topics.commands else config.topics.results
    val consumer = KafkaPartitionProcessing(
      KafkaConsumer.stream(consumerSettings).subscribeTo(input).partitionedRecords,
      config.partitionConcurrency
    ) { message =>
      val parsed = InterviewMessageCodec
        .parse(message.record.value)
        .flatMap(value => Either.cond(value.workflowId.toString == message.record.key, value, "workflow key mismatch"))
      OperationalEventKafkaRuntime.processRecordBeforeCommit(input, message.record.partition, message.record.offset)(
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
    Resource
      .make(
        OperationalEventKafkaRuntime
          .resilientStream(diagnostics, consumer, 1.second, maxDelay = config.restartMaxDelaySeconds.seconds)
          .compile
          .drain
          .start
      )(_.cancel)
      .void
  }
}
