package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.config.{KafkaSaslMechanism, KafkaSaslSecurityProtocol}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.JobStatus
import com.example.graphQL.cats.infrastructure.events.OperationalEventWireCodec
import com.example.graphQL.cats.repository.mongo.KafkaIntegrationSuite
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.events.*
import com.example.graphQL.cats.service.port.{InterviewMessage, InterviewProducerGenerationFenced, InterviewStep}
import fs2.kafka.{ProducerRecord, ProducerRecords, TransactionalProducerSettings}
import fs2.kafka.producer.MkProducer

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** KFH-04.1: initializing a transactional id again fences the previous holder through Kafka's epoch bump. */
final class ProducerRestartFencingIntegrationSpec extends KafkaIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 3.minutes

  private def envelopeRecord(topic: String): ProducerRecords[String, Array[Byte]] = {
    val aggregate = UUID.randomUUID()
    val value = OperationalEventEnvelope(
      UUID.randomUUID(),
      OperationalEventType.JOB_CREATED,
      Instant.now(),
      OperationalAggregateType.Job,
      aggregate.toString,
      UserId(UUID.randomUUID()),
      OperationalEventPayload.JobFact(
        OperationalEventPayload.JobKind.Created,
        OperationalEventPayload.JobSnapshot(aggregate, List("Scala"), JobStatus.Open)
      )
    )
    ProducerRecords.one(ProducerRecord(topic, aggregate.toString, OperationalEventWireCodec.bytes(value)))
  }

  test("KFH-04.1 a second publisher generation with the same transactional id fences the first and publishes") {
    assume(kafkaEvidenceEnabled, "BLOCKED: isolated Kafka restart proof credentials absent")
    val manifest = kafkaNamespace.manifest
    val topic = kafkaNamespace.events
    val transactionalId =
      KafkaProducerGenerationRetirement.PublisherPrefix + "kfh-" + UUID.randomUUID().toString.take(8)
    val settings = KafkaClientSettings.producer(
      manifest.kafkaBootstrap,
      Some("hiring_publisher_v2"),
      Some(manifest.publisherPassword),
      KafkaSaslSecurityProtocol.Plaintext,
      1048576,
      KafkaSaslMechanism.ScramSha512,
      30
    )
    val generation = GuardedTransactionalProducer.resource(
      TransactionalProducerSettings(transactionalId, settings),
      MkProducer.mkProducerForSync[IO]
    )
    generation.use { first =>
      for {
        before <- first.produceWithoutOffsets(envelopeRecord(topic)).attempt
        outcome <- generation.use { second =>
          for {
            stale <- first.produceWithoutOffsets(envelopeRecord(topic)).attempt
            fresh <- second.produceWithoutOffsets(envelopeRecord(topic)).attempt
          } yield (stale, fresh)
        }
      } yield {
        assert(before.isRight, clue(before))
        assert(outcome._1.left.exists(KafkaRuntimeSupport.isProducerFenced), clue(outcome._1))
        assert(outcome._2.isRight, clue(outcome._2))
      }
    }
  }

  test("KFH-04.1 a restarted interview publisher with the same worker id fences its predecessor") {
    assume(kafkaEvidenceEnabled, "BLOCKED: isolated Kafka restart proof credentials absent")
    val config = com.example.graphQL.cats.infrastructure.kafka.InterviewKafkaConfig(
      kafkaNamespace.manifest.kafkaBootstrap,
      "interview_command_publisher",
      kafkaNamespace.manifest.orchestratorPassword,
      KafkaSaslSecurityProtocol.Plaintext,
      worker = false,
      topics = interviewTopics,
      workerGroup = kafkaNamespace.workers,
      orchestratorGroup = kafkaNamespace.orchestrator,
      workerId = com.example.graphQL.cats.service.port.ProducerWorkerId
        .parse("kfh-" + UUID.randomUUID().toString.take(8))
        .toOption
        .get
    )
    def message = InterviewMessage(
      UUID.randomUUID(),
      UUID.randomUUID(),
      "restart-fencing",
      InterviewStep.Reserve,
      0L,
      UUID.randomUUID(),
      Instant.now().plusSeconds(300),
      None,
      Instant.now()
    )
    InterviewKafkaRuntime.publisherResource(config, Diagnostics.noop[IO]).use { first =>
      for {
        before <- first.publish(message).attempt
        outcome <- InterviewKafkaRuntime.publisherResource(config, Diagnostics.noop[IO]).use { second =>
          (first.publish(message).attempt, second.publish(message).attempt).tupled
        }
      } yield {
        assert(before.isRight, clue(before))
        assert(outcome._1.left.exists(_.isInstanceOf[InterviewProducerGenerationFenced]), clue(outcome._1))
        assert(outcome._2.isRight, clue(outcome._2))
      }
    }
  }
}
