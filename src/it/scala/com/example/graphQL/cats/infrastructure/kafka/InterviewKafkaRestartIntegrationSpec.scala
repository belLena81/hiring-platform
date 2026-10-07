package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Ref, Deferred}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.events.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.config.{KafkaConfig, KafkaPublisherConfig, KafkaConsumerConfig}
import io.circe.Json
import com.example.graphQL.cats.config.KafkaSaslSecurityProtocol
import com.example.graphQL.cats.service.Diagnostics
import munit.CatsEffectSuite
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*
import fs2.kafka.{KafkaProducer, ProducerRecord, ProducerRecords, ProducerSettings, Serializer}

/** Opt-in local Kafka restart drill; callback durable storage is isolated from scheduling provider drills. */
class InterviewKafkaRestartIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout = 2.minutes
  private val enabled = sys.env.get("INTERVIEW_KAFKA_EVIDENCE").contains("true")
  private def config(worker: Boolean) = InterviewKafkaConfig(
    sys.env.getOrElse("INTERVIEW_KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
    if (worker) "interview_result_publisher" else "interview_command_publisher",
    sys.env.getOrElse(if (worker) "KAFKA_INTERVIEW_WORKER_PASSWORD" else "KAFKA_INTERVIEW_ORCHESTRATOR_PASSWORD", ""),
    KafkaSaslSecurityProtocol.Plaintext,
    worker
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
    val settings = OperationalEventKafkaRuntime
      .saslProperties(Some(principal.username), Some(principal.password), principal.protocol)
      .foldLeft(
        ProducerSettings(Serializer[IO, String], Serializer[IO, Array[Byte]])
          .withBootstrapServers(principal.bootstrapServers)
      ) { case (current, (key, value)) => current.withProperty(key, value) }
    for {
      observed <- Ref.of[IO, Vector[String]](Vector.empty)
      _ <- InterviewKafkaRuntime
        .resource(config(worker = true), Diagnostics.noop) {
          case Left(identity) if identity.endsWith("invalid null interview message") =>
            observed.update(_ :+ "quarantined").as(true)
          case Right(value) if value.workflowId == workflowId => observed.update(_ :+ "valid").as(true)
          case _                                              => IO.pure(true)
        }
        .use { _ =>
          KafkaProducer.resource(settings).use { producer =>
            producer
              .produce(
                ProducerRecords
                  .one(ProducerRecord(InterviewMessageCodec.CommandsTopic, workflowId.toString, null: Array[Byte]))
              )
              .flatten *>
              producer
                .produce(
                  ProducerRecords.one(
                    ProducerRecord(
                      InterviewMessageCodec.CommandsTopic,
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
  test("operational tombstones retry failed quarantine before later valid records progress") {
    assume(enabled, "BLOCKED: isolated Kafka restart proof credentials absent")
    val kafka = KafkaConfig(
      true,
      config(false).bootstrapServers,
      OperationalEventEnvelope.Topic,
      "hiring-operational-events-integration",
      KafkaPublisherConfig("test", 1, 60, 1, 5, 1000),
      KafkaConsumerConfig(true, 8, 7, Some("analytics_reader"), sys.env.get("KAFKA_READER_PASSWORD")),
      KafkaSaslSecurityProtocol.Plaintext
    )
    val value = OperationalEventEnvelope(
      UUID.randomUUID(),
      OperationalEventType.JOB_CREATED,
      Instant.now(),
      OperationalAggregateType.Job,
      UUID.randomUUID().toString,
      UserId(UUID.randomUUID()),
      Json.obj()
    )
    val settings = OperationalEventKafkaRuntime
      .saslProperties(
        Some("hiring_publisher_v2"),
        sys.env.get("KAFKA_PUBLISHER_V2_PASSWORD"),
        KafkaSaslSecurityProtocol.Plaintext
      )
      .foldLeft(
        ProducerSettings(Serializer[IO, String], Serializer[IO, Array[Byte]])
          .withBootstrapServers(kafka.bootstrapServers)
      ) { case (current, (key, property)) => current.withProperty(key, property) }
    for {
      attempts <- Ref.of[IO, Int](0)
      observed <- Ref.of[IO, Vector[String]](Vector.empty)
      done <- Deferred[IO, Unit]
      offsets <- KafkaProducer.resource(settings).use { producer =>
        for {
          first <- producer
            .produce(ProducerRecords.one(ProducerRecord(kafka.topic, value.partitionKey, null: Array[Byte])))
            .flatten
          _ <- producer
            .produce(
              ProducerRecords.one(ProducerRecord(kafka.topic, value.partitionKey, OperationalEventJson.bytes(value)))
            )
            .flatten
        } yield first.toList.map(_._2.offset())
      }
      receipts = new ConsumerReceiptRepository {
        def exists(group: String, id: UUID) = RepositoryIO.fromEither(Right(false))
        def record(group: String, event: OperationalEventEnvelope, at: Instant, expiresAt: Instant) =
          if (event.eventId == value.eventId)
            RepositoryIO.lift(observed.update(_ :+ "valid") *> done.complete(()).as(true))
          else RepositoryIO.fromEither(Right(true))
      }
      quarantine = new EventQuarantineRepository {
        def save(record: EventQuarantineRecord) =
          if (!offsets.contains(record.offset)) RepositoryIO.fromEither(Right(()))
          else
            RepositoryIO.lift(attempts.updateAndGet(_ + 1)).flatMap {
              case 1 => RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
              case _ => RepositoryIO.lift(observed.update(_ :+ "quarantined"))
            }
      }
      _ <- OperationalEventKafkaRuntime
        .consumerResource(kafka, receipts, quarantine, Diagnostics.noop)
        .use(_ => done.get.timeout(30.seconds))
      count <- attempts.get
      result <- observed.get
    } yield {
      assert(count >= 2)
      assertEquals(result.take(2), Vector("quarantined", "valid"))
    }
  }

}
