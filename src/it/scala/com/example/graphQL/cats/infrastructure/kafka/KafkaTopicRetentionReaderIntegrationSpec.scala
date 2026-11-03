package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Ref}
import cats.syntax.all.*
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
import com.example.graphQL.cats.domain.model.JobStatus
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.events.{OperationalAggregateType, OperationalEventPayload, OperationalEventType}
import com.example.hiring.testing.LocalTestServices
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

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

  test("DDC-02.12 a group whose offsets are deleted rereads from earliest and stores no second receipt") {
    assume(kafkaEvidenceEnabled, "BLOCKED: isolated Kafka credentials absent")
    val manifest = kafkaNamespace.manifest
    val jobId = UUID.randomUUID()
    val envelope = OperationalEventEnvelope(
      UUID.randomUUID(),
      OperationalEventType.JOB_CREATED,
      Instant.now(),
      OperationalAggregateType.Job,
      jobId.toString,
      UserId(UUID.randomUUID()),
      OperationalEventPayload.JobFact(
        OperationalEventPayload.JobKind.Created,
        OperationalEventPayload.JobSnapshot(jobId, List("Scala"), JobStatus.Open)
      )
    )
    val quarantine = new EventQuarantineRepository[IO] {
      def save(record: EventQuarantineRecord) = RepositoryF.pure[IO, Unit](())
    }
    val brokerProperties = LocalTestServices.adminProperties(manifest, "broker", manifest.brokerPassword)
    val _ = brokerProperties.put(
      "key.serializer",
      classOf[org.apache.kafka.common.serialization.StringSerializer].getName
    )
    val _ = brokerProperties.put(
      "value.serializer",
      classOf[org.apache.kafka.common.serialization.ByteArraySerializer].getName
    )
    val _ = brokerProperties.put("acks", "all")
    val produce = IO.blocking {
      val producer = new org.apache.kafka.clients.producer.KafkaProducer[String, Array[Byte]](brokerProperties)
      try
        producer
          .send(
            new org.apache.kafka.clients.producer.ProducerRecord(
              kafkaNamespace.events,
              jobId.toString,
              com.example.graphQL.cats.infrastructure.events.OperationalEventWireCodec.bytes(envelope)
            )
          )
          .get(10L, java.util.concurrent.TimeUnit.SECONDS)
      finally producer.close()
    }.void
    val deleteOnce = IO.blocking {
      val admin = org.apache.kafka.clients.admin.Admin.create(brokerProperties)
      try
        admin
          .deleteConsumerGroups(List(kafkaNamespace.eventGroup).asJava)
          .all()
          .get(10L, java.util.concurrent.TimeUnit.SECONDS)
      finally admin.close()
    }.void
    // The group is deletable only once the closed consumer has left it, so retry while the broker reports members.
    val deleteGroup = deleteOnce.handleErrorWith(_ => IO.sleep(2.seconds) *> deleteOnce)

    for {
      deliveries <- Ref.of[IO, Int](0)
      stored <- Ref.of[IO, Set[UUID]](Set.empty)
      inserted <- Ref.of[IO, Int](0)
      receipts = new ConsumerReceiptRepository[IO] {
        def record(group: String, event: OperationalEventEnvelope, now: Instant, expiresAt: Instant) =
          RepositoryF.lift(
            stored.modify(set => (set + event.eventId, !set.contains(event.eventId))).flatTap { fresh =>
              (deliveries.update(_ + 1) *> inserted.update(_ + 1).whenA(fresh)).whenA(event.eventId == envelope.eventId)
            }
          )
      }
      consumeUntil = (target: Int) =>
        OperationalEventKafkaRuntime
          .consumerResource(kafka(kafkaNamespace.events), receipts, quarantine, Diagnostics.noop[IO])
          .use(_ =>
            deliveries.get
              .flatMap(n => IO.sleep(200.millis).as(n))
              .iterateUntil(_ >= target)
              .timeout(60.seconds) *> IO.sleep(2.seconds)
          )
      _ <- produce
      _ <- consumeUntil(1)
      _ <- deleteGroup
      _ <- consumeUntil(2)
      delivered <- deliveries.get
      receiptsStored <- inserted.get
    } yield {
      assert(delivered >= 2, s"expected a second delivery, saw $delivered")
      assertEquals(receiptsStored, 1)
    }
  }
}
