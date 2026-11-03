package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.config.{
  KafkaConfig,
  KafkaConsumerConfig,
  KafkaPublisherConfig,
  KafkaSaslSecurityProtocol
}
import com.example.graphQL.cats.repository.mongo.KafkaIntegrationSuite
import com.example.graphQL.cats.service.port.{
  ConsumerReceiptRepository,
  EventQuarantineRecord,
  EventQuarantineRepository,
  RepositoryF
}
import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import com.example.graphQL.cats.service.{Diagnostics, WorkerAlive}
import java.time.Instant
import scala.concurrent.duration.*

/** DDC-02.6: the provisioned reader principal can describe its events topic, and only that topic. */
class KafkaTopicRetentionReaderIntegrationSpec extends KafkaIntegrationSuite {
  override val munitIOTimeout = 2.minutes

  private def kafka(topic: String) = KafkaConfig(
    enabled = true,
    bootstrapServers = kafkaNamespace.manifest.kafkaBootstrap,
    topic = topic,
    consumerGroup = kafkaNamespace.eventGroup,
    publisher = KafkaPublisherConfig("test", 10, 30, 1, 3, 100),
    consumer = KafkaConsumerConfig(
      enabled = true,
      receiptTtlDays = 9,
      quarantineTtlDays = 7,
      saslUsername = Some("analytics_reader"),
      saslPassword = Some(kafkaNamespace.manifest.readerPassword)
    ),
    saslSecurityProtocol = KafkaSaslSecurityProtocol.Plaintext
  )

  test("DDC-02.6 the reader describes its events topic configuration") {
    assume(kafkaEvidenceEnabled, "BLOCKED: isolated Kafka credentials absent")
    KafkaTopicRetentionReader
      .read(kafka(kafkaNamespace.events))
      .map(assertEquals(_, TopicRetention(7.days.toMillis, 1.day.toMillis)))
  }

  test("DDC-02.6 describing another topic is denied and reported as unreadable") {
    assume(kafkaEvidenceEnabled, "BLOCKED: isolated Kafka credentials absent")
    KafkaTopicRetentionReader
      .read(kafka(kafkaNamespace.commands))
      .attempt
      .map {
        case Left(error: TopicConfigUnreadable) => assertEquals(error.topic, kafkaNamespace.commands)
        case other                              => fail(s"expected TopicConfigUnreadable, got $other")
      }
  }

  test("DDC-02.6 the operational consumer passes its start gate against the provisioned topic and beats") {
    assume(kafkaEvidenceEnabled, "BLOCKED: isolated Kafka credentials absent")
    val receipts = new ConsumerReceiptRepository[IO] {
      def record(group: String, event: OperationalEventEnvelope, now: Instant, expiresAt: Instant) =
        RepositoryF.pure[IO, Boolean](true)
    }
    val quarantine = new EventQuarantineRepository[IO] {
      def save(record: EventQuarantineRecord) = RepositoryF.pure[IO, Unit](())
    }
    for {
      beats <- Ref.of[IO, Int](0)
      alive = WorkerAlive(beats.update(_ + 1), 100.millis)
      _ <- OperationalEventKafkaRuntime
        .consumerResource(kafka(kafkaNamespace.events), receipts, quarantine, Diagnostics.noop[IO], alive = alive)
        .use(_ => beats.get.flatMap(n => IO.sleep(200.millis).as(n)).iterateUntil(_ > 0).timeout(30.seconds))
    } yield ()
  }
}
