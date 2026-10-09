package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Ref, Deferred, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.repository.mongo.*
import com.example.graphQL.cats.repository.mongo.MongoRepositoryTestSupport.*
import com.mongodb.client.model.Filters
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.bson.types.Binary
import java.util.{Collections, Properties}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.events.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.config.{KafkaConfig, KafkaPublisherConfig, KafkaConsumerConfig}
import io.circe.Json
import com.example.graphQL.cats.config.KafkaSaslSecurityProtocol
import com.example.graphQL.cats.service.Diagnostics
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*
import fs2.kafka.{KafkaProducer, ProducerRecord, ProducerRecords, ProducerSettings, Serializer}

/** Opt-in local Kafka restart drill; callback durable storage is isolated from scheduling provider drills. */
class InterviewKafkaRestartIntegrationSpec extends KafkaIntegrationSuite {
  override val munitIOTimeout = 2.minutes
  private def enabled = kafkaEvidenceEnabled
  private def config(worker: Boolean) = InterviewKafkaConfig(
    kafkaNamespace.manifest.kafkaBootstrap,
    if (worker) "interview_result_publisher" else "interview_command_publisher",
    if (worker) kafkaNamespace.manifest.workerPassword else kafkaNamespace.manifest.orchestratorPassword,
    KafkaSaslSecurityProtocol.Plaintext,
    worker,
    topics = interviewTopics,
    workerGroup = kafkaNamespace.workers,
    orchestratorGroup = kafkaNamespace.orchestrator
  )
  test("failed record restarts consumer and is replayed before a later command") {
    assume(enabled, "BLOCKED: isolated Kafka restart proof credentials absent")
    val workflowId = UUID.randomUUID()
    val first = InterviewMessage(
      UUID.randomUUID(),
      workflowId,
      "restart-first",
      InterviewStep.Reserve,
      0L,
      UUID.randomUUID(),
      Instant.now().plusSeconds(300),
      None,
      Instant.now()
    )
    val second = first.copy(messageId = UUID.randomUUID(), stepId = "restart-second", revision = 1L)
    for {
      failures <- Ref.of[IO, Int](0)
      durable <- Ref.of[IO, Vector[String]](Vector.empty)
      _ <- InterviewKafkaRuntime
        .resource(config(worker = true), Diagnostics.noop) {
          case Right(message) if message.workflowId == workflowId =>
            if (message.stepId == first.stepId) failures.updateAndGet(_ + 1).flatMap {
              case 1 => IO.pure(false)
              case _ => durable.update(_ :+ message.stepId).as(true)
            }
            else durable.update(_ :+ message.stepId).as(true)
          case _ => IO.pure(true)
        }
        .use { _ =>
          InterviewKafkaRuntime.resource(config(worker = false), Diagnostics.noop)(_ => IO.pure(true)).use {
            publisher =>
              publisher.publish(first) *> publisher.publish(second) *>
                IO.sleep(10.seconds) *> durable.get.flatMap(values =>
                  IO {
                    assertEquals(values.take(2), Vector(first.stepId, second.stepId))
                  }
                )
          }
        }
      count <- failures.get
    } yield assert(count >= 2)
  }

  test("a null record is quarantined before the next valid record is acknowledged") {
    assume(enabled, "BLOCKED: isolated Kafka restart proof credentials absent")
    val workflowId = UUID.randomUUID()
    val message = InterviewMessage(
      UUID.randomUUID(),
      workflowId,
      "after-null",
      InterviewStep.Reserve,
      0L,
      UUID.randomUUID(),
      Instant.now().plusSeconds(300),
      None,
      Instant.now()
    )
    val principal = config(worker = false)
    val settings = ProducerSettings(Serializer[IO, String], Serializer[IO, Array[Byte]])
      .withBootstrapServers(principal.bootstrapServers)
      .withProperties(
        KafkaClientSettings.security(Some(principal.username), Some(principal.password), principal.protocol)
      )
    for {
      observed <- Ref.of[IO, Vector[String]](Vector.empty)
      _ <- InterviewKafkaRuntime
        .resource(config(worker = true), Diagnostics.noop) {
          case Left(identity) if identity.startsWith("transport:") =>
            observed.update(_ :+ "quarantined").as(true)
          case Right(value) if value.workflowId == workflowId => observed.update(_ :+ "valid").as(true)
          case _                                              => IO.pure(true)
        }
        .use { _ =>
          KafkaProducer.resource(settings).use { producer =>
            producer
              .produce(
                ProducerRecords
                  .one(ProducerRecord(interviewTopics.commands, workflowId.toString, null: Array[Byte]))
              )
              .flatten *>
              producer
                .produce(
                  ProducerRecords.one(
                    ProducerRecord(
                      interviewTopics.commands,
                      workflowId.toString,
                      InterviewMessageCodec.bytes(message)
                    )
                  )
                )
                .flatten *>
              IO.sleep(10.seconds) *> observed.get
                .flatMap(values => IO(assertEquals(values, Vector("quarantined", "valid"))))
          }
        }
    } yield ()
  }
  test("maximum-length topic rejection persists before acknowledgment and later work; replay deduplicates") {
    val topics = com.example.graphQL.cats.domain.workflow.InterviewTopicPair(
      "hiring.quarantine." + "a" * (249 - "hiring.quarantine.".length),
      "hiring.quarantine-results"
    )
    InterviewBrokerFixture.resource(topics).use { broker =>
      mongoResource.use { fixture =>
        val repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
        val workflowId = UUID.randomUUID()
        val now = Instant.now()
        val message = InterviewMessage(
          UUID.randomUUID(),
          workflowId,
          "after-invalid",
          InterviewStep.Reserve,
          0L,
          UUID.randomUUID(),
          now.plusSeconds(300),
          None,
          now
        )
        val principal = broker.config(worker = false)
        val settings = ProducerSettings(Serializer[IO, String], Serializer[IO, Array[Byte]])
          .withBootstrapServers(principal.bootstrapServers)
          .withProperties(
            KafkaClientSettings.security(Some(principal.username), Some(principal.password), principal.protocol)
          )
        for {
          _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop, topics)
          metadata <- KafkaProducer.resource(settings).use { producer =>
            for {
              first <- producer
                .produce(
                  ProducerRecords.one(
                    ProducerRecord(topics.commands, workflowId.toString, null: Array[Byte])
                  )
                )
                .flatten
              second <- producer
                .produce(
                  ProducerRecords.one(
                    ProducerRecord(topics.commands, workflowId.toString, InterviewMessageCodec.bytes(message))
                  )
                )
                .flatten
            } yield (first.toList.head._2, second.toList.head._2)
          }
          (first, second) = metadata
          identity = InterviewKafkaRuntime.rejectionIdentity(
            topics.commands,
            first.partition(),
            first.offset(),
            None,
            "invalid null interview message"
          )
          target = Filters.eq("_id", s"quarantine:$identity")
          _ = assertEquals(identity.length, 74)
          received <- Deferred[IO, Unit]
          saved <- Ref.of[IO, Vector[String]](Vector.empty)
          kafka = KafkaConfig(
            true,
            broker.bootstrap,
            topics.commands,
            broker.config(true).workerGroup,
            KafkaPublisherConfig("test", 1, 60, 1, 5, 1000),
            KafkaConsumerConfig(true, 8, 7, Some(broker.config(true).username), Some(broker.config(true).password)),
            KafkaSaslSecurityProtocol.Plaintext
          )
          partition = new TopicPartition(topics.commands, first.partition())
          _ <- InterviewKafkaRuntime
            .consumerResource(broker.config(worker = true), Diagnostics.noop) {
              case Left(observed) =>
                for {
                  _ <- IO(assertEquals(observed, identity))
                  before <- committed(kafka, partition)
                  _ <- IO(assert(before.forall(_ <= first.offset()), clues(before, first.offset())))
                  durable <- repository.quarantine(observed, now).value
                  _ <- IO(assertEquals(durable, Right(())))
                  rows <- count(fixture.database, "interview_workflow_inbox", target)
                  _ <- IO(assertEquals(rows, 1L))
                  _ <- saved.update(_ :+ "quarantined")
                } yield true
              case Right(value) if value.messageId == message.messageId =>
                count(fixture.database, "interview_workflow_inbox", target).flatMap { rows =>
                  IO(assertEquals(rows, 1L)) *> saved.update(_ :+ "valid") *> received.complete(()).as(true)
                }
              case _ => IO.pure(true)
            }
            .use { _ =>
              received.get.timeout(45.seconds) *>
                awaitCommitted(kafka, partition, second.offset() + 1L).timeout(30.seconds)
            }
          order <- saved.get
          _ = assertEquals(order.take(2), Vector("quarantined", "valid"))
          failedCommit <- OperationalEventKafkaRuntime
            .processRecord(topics.commands, first.partition(), first.offset())(
              repository.quarantine(identity, now)
            )(IO.raiseError(new IllegalStateException("synthetic acknowledgment failure")))
            .attempt
          _ = assert(failedCommit.isLeft)
          acknowledged <- Ref.of[IO, Boolean](false)
          _ <- OperationalEventKafkaRuntime.processRecord(
            topics.commands,
            first.partition(),
            first.offset()
          )(repository.quarantine(identity, now))(acknowledged.set(true))
          replayRows <- count(fixture.database, "interview_workflow_inbox", target)
          replayAck <- acknowledged.get
        } yield {
          assertEquals(replayRows, 1L)
          assert(replayAck)
        }
      }
    }
  }
  test("operational tombstones retry failed quarantine before later valid records progress") {
    assume(enabled, "BLOCKED: isolated Kafka restart proof credentials absent")
    val kafka = KafkaConfig(
      true,
      config(false).bootstrapServers,
      kafkaNamespace.events,
      kafkaNamespace.eventGroup,
      KafkaPublisherConfig("test", 1, 60, 1, 5, 1000),
      KafkaConsumerConfig(true, 8, 7, Some("analytics_reader"), Some(kafkaNamespace.manifest.readerPassword)),
      KafkaSaslSecurityProtocol.Plaintext
    )
    val jobId = UUID.randomUUID().toString
    val value = OperationalEventEnvelope(
      UUID.randomUUID(),
      OperationalEventType.JOB_CREATED,
      Instant.now(),
      OperationalAggregateType.Job,
      jobId,
      UserId(UUID.randomUUID()),
      Json.obj(
        "job" -> Json.obj(
          "jobId" -> Json.fromString(jobId),
          "skills" -> Json.arr(Json.fromString("Scala")),
          "status" -> Json.fromString("Open")
        )
      )
    )
    val settings = ProducerSettings(Serializer[IO, String], Serializer[IO, Array[Byte]])
      .withBootstrapServers(kafka.bootstrapServers)
      .withProperties(
        KafkaClientSettings.security(
          Some("hiring_publisher_v2"),
          Some(kafkaNamespace.manifest.publisherPassword),
          KafkaSaslSecurityProtocol.Plaintext
        )
      )
    operationalDatabase.use { database =>
      val storedReceipts = new MongoConsumerReceiptRepository(database, Diagnostics.noop)
      val storedQuarantine = new MongoEventQuarantineRepository(database, Diagnostics.noop)
      for {
        failedAttempt <- Deferred[IO, Unit]
        restore <- Deferred[IO, Unit]
        received <- Deferred[IO, Unit]
        attempts <- Ref.of[IO, Int](0)
        metadata <- KafkaProducer.resource(settings).use { producer =>
          for {
            first <- producer
              .produce(
                ProducerRecords.one(
                  ProducerRecord(kafka.topic, value.partitionKey, null: Array[Byte])
                )
              )
              .flatten
            second <- producer
              .produce(
                ProducerRecords.one(
                  ProducerRecord(kafka.topic, value.partitionKey, OperationalEventJson.bytes(value))
                )
              )
              .flatten
          } yield (first.toList.head._2, second.toList.head._2)
        }
        (first, second) = metadata
        partition = new TopicPartition(kafka.topic, first.partition())
        target = Filters.and(
          Filters.eq("topic", kafka.topic),
          Filters.eq("partition", first.partition()),
          Filters.eq("offset", first.offset())
        )
        receipts = new ConsumerReceiptRepository {
          def exists(group: String, id: UUID) = storedReceipts.exists(group, id)
          def record(group: String, event: OperationalEventEnvelope, at: Instant, expiresAt: Instant) =
            storedReceipts.record(group, event, at, expiresAt).flatMap { saved =>
              if (event.eventId == value.eventId) RepositoryIO.lift(received.complete(()).void).as(saved)
              else RepositoryIO.fromEither(Right(saved))
            }
        }
        quarantine = new EventQuarantineRepository {
          def save(record: EventQuarantineRecord) =
            if (record.partition != first.partition() || record.offset != first.offset()) storedQuarantine.save(record)
            else
              RepositoryIO.lift(attempts.updateAndGet(_ + 1)).flatMap {
                case 1 =>
                  RepositoryIO.lift(failedAttempt.complete(()).void *> restore.get) *>
                    RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
                case _ => storedQuarantine.save(record)
              }
        }
        _ <- OperationalEventKafkaRuntime.consumerResource(kafka, receipts, quarantine, Diagnostics.noop).use { _ =>
          for {
            _ <- failedAttempt.get.timeout(30.seconds)
            before <- committed(kafka, partition)
            receiptBefore <- storedReceipts.exists(kafka.consumerGroup, value.eventId).value
            rowsBefore <- count(database, "event_quarantine", target)
            _ = assert(before.forall(_ <= first.offset()), clues(before, first.offset()))
            _ = assertEquals(receiptBefore, Right(false))
            _ = assertEquals(rowsBefore, 0L)
            _ <- restore.complete(())
            _ <- received.get.timeout(30.seconds)
            _ <- awaitCommitted(kafka, partition, second.offset() + 1L).timeout(30.seconds)
          } yield ()
        }
        retries <- attempts.get
        rows <- count(database, "event_quarantine", target)
        document <- findOne(database, "event_quarantine", target)
        _ = assert(retries >= 2)
        _ = assertEquals(rows, 1L)
        _ = assertEquals(document.map(_.getString("reason")), Some("null event envelope"))
        _ = assertEquals(document.map(_.get("rawBytes", classOf[Binary]).getData.toList), Some(List.empty[Byte]))
        // A durable quarantine survives a failed acknowledgment; replay must remain idempotent.
        failedCommit <- OperationalEventKafkaRuntime
          .processRecord(kafka.topic, first.partition(), first.offset())(
            OperationalEventKafkaRuntime.recordDurably(
              kafka,
              storedReceipts,
              storedQuarantine,
              kafka.topic,
              first.partition(),
              first.offset(),
              None
            )
          )(IO.raiseError(new IllegalStateException("synthetic commit failure")))
          .attempt
        _ = assert(failedCommit.isLeft)
        acknowledged <- Ref.of[IO, Boolean](false)
        _ <- OperationalEventKafkaRuntime.processRecord(kafka.topic, first.partition(), first.offset())(
          OperationalEventKafkaRuntime.recordDurably(
            kafka,
            storedReceipts,
            storedQuarantine,
            kafka.topic,
            first.partition(),
            first.offset(),
            None
          )
        )(acknowledged.set(true))
        replayCount <- count(database, "event_quarantine", target)
        replayAcknowledged <- acknowledged.get
      } yield {
        assertEquals(replayCount, 1L)
        assert(replayAcknowledged)
      }
    }
  }

  private def operationalDatabase: Resource[IO, mongo4cats.database.MongoDatabase[IO]] =
    mongoResource
      .map(_.database)
      .evalTap(database => MongoHiringSetup.initialize(database, Diagnostics.noop, interviewTopics))

  private def committed(config: KafkaConfig, partition: TopicPartition): IO[Option[Long]] = IO.blocking {
    val properties = new Properties()
    properties.put("bootstrap.servers", config.bootstrapServers)
    properties.put("group.id", config.consumerGroup)
    properties.put("key.deserializer", classOf[StringDeserializer].getName)
    properties.put("value.deserializer", classOf[ByteArrayDeserializer].getName)
    KafkaClientSettings
      .security(config.consumer.saslUsername, config.consumer.saslPassword, config.saslSecurityProtocol)
      .foreach { case (key, value) => properties.put(key, value); () }
    val consumer = new org.apache.kafka.clients.consumer.KafkaConsumer[String, Array[Byte]](properties)
    try Option(consumer.committed(Collections.singleton(partition)).get(partition)).map(_.offset())
    finally consumer.close()
  }

  private def awaitCommitted(config: KafkaConfig, partition: TopicPartition, expected: Long): IO[Unit] =
    committed(config, partition).flatMap { offset =>
      if (offset.exists(_ >= expected)) IO.unit
      else IO.sleep(50.millis) *> awaitCommitted(config, partition, expected)
    }
}
