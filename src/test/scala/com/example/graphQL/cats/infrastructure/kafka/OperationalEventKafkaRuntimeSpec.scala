package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.config.{KafkaConfig, KafkaConsumerConfig, KafkaPublisherConfig}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.shared.events.*
import io.circe.Json
import munit.CatsEffectSuite

import java.time.Instant
import java.util.UUID

class OperationalEventKafkaRuntimeSpec extends CatsEffectSuite {
  private val now = Instant.parse("2026-09-20T00:00:00Z")
  private val actor = UserId(UUID.fromString("00000000-0000-0000-0000-000000000201"))
  private val config = KafkaConfig(
    enabled = true,
    bootstrapServers = "127.0.0.1:9092",
    topic = OperationalEventEnvelope.Topic,
    consumerGroup = "test-consumer",
    publisher = KafkaPublisherConfig("test", 10, 30, 1, 3, 100),
    consumer = KafkaConsumerConfig(enabled = true, receiptTtlDays = 8, quarantineTtlDays = 7)
  )

  private def event(
      eventType: OperationalEventType,
      aggregateId: String = "job-1",
      sequence: Long = 0L,
      schemaVersion: Int = OperationalEventEnvelope.SchemaVersion
  ): OperationalEventEnvelope =
    OperationalEventEnvelope(
      UUID.randomUUID(), eventType, schemaVersion, now, OperationalAggregateType.Job,
      aggregateId, sequence, sequence, actor, Json.obj("value" -> Json.fromString("fixture"))
    )

  private final case class Fakes(
      receipts: ConsumerReceiptRepository[IO],
      quarantines: EventQuarantineRepository[IO],
      quarantined: Ref[IO, Vector[EventQuarantineRecord]]
  )

  private def fakes: IO[Fakes] =
    for {
      receiptState <- Ref.of[IO, Map[(String, UUID), OperationalEventEnvelope]](Map.empty)
      quarantineState <- Ref.of[IO, Vector[EventQuarantineRecord]](Vector.empty)
    } yield {
      val receipts = new ConsumerReceiptRepository[IO] {
        override def exists(group: String, id: UUID): IO[Either[RepositoryError, Boolean]] =
          receiptState.get.map(values => Right(values.contains(group -> id)))
        override def latestSequence(group: String, aggregateType: String, aggregateId: String): IO[Either[RepositoryError, Option[Long]]] =
          receiptState.get.map(_.values.filter(value =>
            value.aggregateType.toString == aggregateType && value.aggregateId == aggregateId
          ).map(_.sequence).toList.sorted.lastOption).map(Right(_))
        override def record(group: String, value: OperationalEventEnvelope, createdAt: Instant, expiresAt: Instant): IO[Either[RepositoryError, Boolean]] =
          receiptState.modify { values =>
            val key = group -> value.eventId
            if (values.contains(key)) (values, Right(false))
            else (values.updated(key, value), Right(true))
          }
      }
      val quarantines = new EventQuarantineRepository[IO] {
        override def save(record: EventQuarantineRecord): IO[Either[RepositoryError, Unit]] =
          quarantineState.update(_ :+ record).as(Right(()))
      }
      Fakes(receipts, quarantines, quarantineState)
    }

  test("malformed, unsupported, and invalid ordering records quarantine without commit") {
    fakes.flatMap { values =>
      val unsupported = OperationalEventJson.json(event(OperationalEventType.JOB_UPDATED, schemaVersion = 99)).noSpaces.getBytes
      val invalid = OperationalEventJson.bytes(event(OperationalEventType.JOB_UPDATED, sequence = -1L))
      for {
        unsupportedCommit <- OperationalEventKafkaRuntime.handleRecord(config, values.receipts, values.quarantines, config.topic, 0, 1L, unsupported)
        malformedCommit <- OperationalEventKafkaRuntime.handleRecord(config, values.receipts, values.quarantines, config.topic, 0, 2L, "not-json".getBytes)
        invalidCommit <- OperationalEventKafkaRuntime.handleRecord(config, values.receipts, values.quarantines, config.topic, 0, 3L, invalid)
        records <- values.quarantined.get
      } yield {
        assertEquals((unsupportedCommit, malformedCommit, invalidCommit), (false, false, false))
        assertEquals(records.map(_.category), Vector(
          OperationalEventFailureCategory.UnsupportedVersion,
          OperationalEventFailureCategory.MalformedEnvelope,
          OperationalEventFailureCategory.InvalidOrdering
        ))
      }
    }
  }

  test("duplicate delivery acknowledges once and a gap is quarantined") {
    fakes.flatMap { values =>
      val first = event(OperationalEventType.JOB_CREATED, sequence = 0L)
      val gap = event(OperationalEventType.JOB_UPDATED, sequence = 2L)
      for {
        firstCommit <- OperationalEventKafkaRuntime.handleRecord(config, values.receipts, values.quarantines, config.topic, 0, 4L, OperationalEventJson.bytes(first))
        duplicateCommit <- OperationalEventKafkaRuntime.handleRecord(config, values.receipts, values.quarantines, config.topic, 0, 5L, OperationalEventJson.bytes(first))
        gapCommit <- OperationalEventKafkaRuntime.handleRecord(config, values.receipts, values.quarantines, config.topic, 0, 6L, OperationalEventJson.bytes(gap))
        records <- values.quarantined.get
      } yield {
        assert(firstCommit)
        assert(duplicateCommit)
        assert(!gapCommit)
        assertEquals(records.map(_.category), Vector(OperationalEventFailureCategory.InvalidOrdering))
      }
    }
  }

  test("candidate hired cannot be the first observed application fact") {
    fakes.flatMap { values =>
      val hired = event(OperationalEventType.CANDIDATE_HIRED, aggregateId = "application-1", sequence = 9L).copy(aggregateType = OperationalAggregateType.Application)
      OperationalEventKafkaRuntime.handleRecord(config, values.receipts, values.quarantines, config.topic, 0, 7L, OperationalEventJson.bytes(hired)).flatMap { committed =>
        values.quarantined.get.map { records =>
          assert(!committed)
          assertEquals(records.headOption.map(_.category), Some(OperationalEventFailureCategory.InvalidOrdering))
        }
      }
    }
  }
}
