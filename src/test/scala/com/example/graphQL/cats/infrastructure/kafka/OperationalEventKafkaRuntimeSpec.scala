package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.config.{
  KafkaConfig,
  KafkaConsumerConfig,
  KafkaPublisherConfig,
  KafkaSaslSecurityProtocol
}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField}
import com.example.graphQL.cats.service.events.*
import io.circe.Json
import fs2.Stream
import munit.CatsEffectSuite
import org.apache.kafka.common.errors.ProducerFencedException

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

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

  test("publisher and reader use independent SASL principals") {
    val publisher = OperationalEventKafkaRuntime.saslProperties(Some("publisher"), Some("publish-secret"))
    val reader = OperationalEventKafkaRuntime.saslProperties(Some("analytics_reader"), Some("read-secret"))
    assertEquals(publisher.get("security.protocol"), Some("SASL_SSL"))
    assert(publisher.getOrElse("sasl.jaas.config", "").contains("username=\"publisher\""))
    assert(reader.getOrElse("sasl.jaas.config", "").contains("username=\"analytics_reader\""))
    assert(!publisher.getOrElse("sasl.jaas.config", "").contains("read-secret"))
  }

  test("plaintext SASL transport requires explicit local configuration") {
    val properties = OperationalEventKafkaRuntime.saslProperties(
      Some("publisher"),
      Some("publish-secret"),
      KafkaSaslSecurityProtocol.Plaintext
    )
    assertEquals(properties.get("security.protocol"), Some("SASL_PLAINTEXT"))
  }

  test("Kafka clients without credentials receive no SASL properties") {
    assertEquals(OperationalEventKafkaRuntime.saslProperties(None, None), Map.empty)
  }

  private def event(
      eventType: OperationalEventType,
      aggregateId: String = "job-1",
      occurredAt: Instant = now
  ): OperationalEventEnvelope =
    OperationalEventEnvelope(
      UUID.randomUUID(),
      eventType,
      occurredAt,
      OperationalAggregateType.Job,
      aggregateId,
      actor,
      Json.obj("value" -> Json.fromString("fixture"))
    )

  private def claim(partitionKey: String, eventId: UUID): ClaimedOperationalEvent = {
    val value = event(OperationalEventType.JOB_UPDATED, aggregateId = partitionKey).copy(eventId = eventId)
    ClaimedOperationalEvent(value, OperationalEventJson.bytes(value), partitionKey, s"lease-$partitionKey-$eventId", 1)
  }

  private final case class Fakes(
      receipts: ConsumerReceiptRepository,
      quarantines: EventQuarantineRepository,
      quarantined: Ref[IO, Vector[EventQuarantineRecord]]
  )

  private def fakes: IO[Fakes] =
    for {
      receiptState <- Ref.of[IO, Map[(String, UUID), OperationalEventEnvelope]](Map.empty)
      quarantineState <- Ref.of[IO, Vector[EventQuarantineRecord]](Vector.empty)
    } yield {
      val receipts = new ConsumerReceiptRepository {
        override def exists(group: String, id: UUID): RepositoryIO[Boolean] =
          com.example.graphQL.cats.service.port.RepositoryIO
            .fromIOEither(receiptState.get.map(values => Right(values.contains(group -> id))))
        override def record(
            group: String,
            value: OperationalEventEnvelope,
            createdAt: Instant,
            expiresAt: Instant
        ): RepositoryIO[Boolean] =
          com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(receiptState.modify { values =>
            val key = group -> value.eventId
            if (values.contains(key)) (values, Right(false))
            else (values.updated(key, value), Right(true))
          })
      }
      val quarantines = new EventQuarantineRepository {
        override def save(record: EventQuarantineRecord): RepositoryIO[Unit] =
          com.example.graphQL.cats.service.port.RepositoryIO
            .fromIOEither(quarantineState.update(_ :+ record).as(Right(())))
      }
      Fakes(receipts, quarantines, quarantineState)
    }

  test("tombstones quarantine safely before committing and failures retain the offset") {
    fakes.flatMap { values =>
      val failing = new EventQuarantineRepository {
        def save(record: EventQuarantineRecord): RepositoryIO[Unit] =
          RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
      }
      for {
        success <- OperationalEventKafkaRuntime.handleRecord(
          config,
          values.receipts,
          values.quarantines,
          config.topic,
          0,
          1L,
          null
        )
        failure <- OperationalEventKafkaRuntime.handleRecord(
          config,
          values.receipts,
          failing,
          config.topic,
          0,
          2L,
          null
        )
        records <- values.quarantined.get
      } yield {
        assert(success)
        assert(!failure)
        assertEquals(records.size, 1)
        assertEquals(records.head.rawBytes.toList, List.empty[Byte])
      }
    }
  }

  test("malformed records commit after durable quarantine") {
    fakes.flatMap { values =>
      for {
        malformedCommit <- OperationalEventKafkaRuntime.handleRecord(
          config,
          values.receipts,
          values.quarantines,
          config.topic,
          0,
          1L,
          "not-json".getBytes
        )
        records <- values.quarantined.get
      } yield {
        assert(malformedCommit)
        assertEquals(records.map(_.category), Vector(OperationalEventFailureCategory.MalformedEnvelope))
      }
    }
  }

  test("malformed records remain uncommitted when quarantine persistence fails") {
    val failedQuarantine = new EventQuarantineRepository {
      override def save(record: EventQuarantineRecord): RepositoryIO[Unit] =
        com.example.graphQL.cats.service.port.RepositoryIO.fromIOEither(IO.pure(Left(RepositoryError.Unavailable)))
    }
    fakes.flatMap { values =>
      OperationalEventKafkaRuntime
        .handleRecord(
          config,
          values.receipts,
          failedQuarantine,
          config.topic,
          0,
          2L,
          "not-json".getBytes
        )
        .map(commit => assert(!commit))
    }
  }

  test("an undurable record stops sequential processing before a later offset can commit") {
    for {
      processed <- cats.effect.Ref.of[IO, Vector[Long]](Vector.empty)
      committed <- cats.effect.Ref.of[IO, Vector[Long]](Vector.empty)
      result <- List(10L, 11L).traverse_ { offset =>
        OperationalEventKafkaRuntime.processRecordBeforeCommit("hiring.events", 0, offset)(
          processed.update(_ :+ offset).as(offset != 10L)
        )(committed.update(_ :+ offset))
      }.attempt
      handled <- processed.get
      offsets <- committed.get
    } yield {
      assert(result.isLeft)
      assertEquals(handled, Vector(10L))
      assertEquals(offsets, Vector.empty)
    }
  }

  test("duplicate delivery acknowledges once and accepts events without revision gaps") {
    fakes.flatMap { values =>
      val first = event(OperationalEventType.JOB_CREATED)
      val independent = event(OperationalEventType.JOB_UPDATED)
      for {
        firstCommit <- OperationalEventKafkaRuntime.handleRecord(
          config,
          values.receipts,
          values.quarantines,
          config.topic,
          0,
          4L,
          OperationalEventJson.bytes(first)
        )
        duplicateCommit <- OperationalEventKafkaRuntime.handleRecord(
          config,
          values.receipts,
          values.quarantines,
          config.topic,
          0,
          5L,
          OperationalEventJson.bytes(first)
        )
        independentCommit <- OperationalEventKafkaRuntime.handleRecord(
          config,
          values.receipts,
          values.quarantines,
          config.topic,
          0,
          6L,
          OperationalEventJson.bytes(independent)
        )
        records <- values.quarantined.get
      } yield {
        assert(firstCommit)
        assert(duplicateCommit)
        assert(independentCommit)
        assertEquals(records, Vector.empty)
      }
    }
  }

  test("publisher preserves ordering within each partition key") {
    for {
      first = UUID.fromString("00000000-0000-0000-0000-000000000101")
      second = UUID.fromString("00000000-0000-0000-0000-000000000102")
      seen <- Ref.of[IO, Vector[UUID]](Vector.empty)
      _ <- OperationalEventKafkaRuntime.publishClaims(List(claim("job-1", first), claim("job-1", second))) { value =>
        seen.update(_ :+ value.event.eventId)
      }
      result <- seen.get
    } yield assertEquals(result, Vector(first, second))
  }

  test("publisher overlaps independent partition keys") {
    for {
      arrivals <- Ref.of[IO, Int](0)
      barrier <- Deferred[IO, Unit]
      _ <- OperationalEventKafkaRuntime.publishClaims(
        List(claim("job-1", UUID.randomUUID()), claim("job-2", UUID.randomUUID()))
      ) { value =>
        arrivals.modify(count => (count + 1, count + 1)).flatMap { count =>
          if (count == 2) barrier.complete(()).void else barrier.get
        } *> IO(assert(value.event.aggregateId == "job-1" || value.event.aggregateId == "job-2"))
      }
      count <- arrivals.get
    } yield assertEquals(count, 2)
  }

  test("resilient stream retries failed effects until they recover") {
    for {
      attempts <- Ref.of[IO, Int](0)
      stream = OperationalEventKafkaRuntime.resilientStream(
        Diagnostics.noop,
        fs2.Stream.eval {
          attempts.updateAndGet(_ + 1).flatMap { count =>
            if (count < 3) IO.raiseError[Unit](new IllegalStateException("transient kafka failure"))
            else IO.unit
          }
        },
        1.millis
      )
      _ <- stream.compile.drain
      result <- attempts.get
    } yield assertEquals(result, 3)
  }

  test("producer fencing is detected through wrapped Kafka errors") {
    assert(
      OperationalEventKafkaRuntime.isProducerFenced(
        new IllegalStateException("send failed", new ProducerFencedException("producer fenced"))
      )
    )
    assert(!OperationalEventKafkaRuntime.isProducerFenced(new IllegalStateException("ordinary send failure")))
  }

  test("a fenced producer generation is released before the next generation starts") {
    for {
      acquired <- Ref.of[IO, Int](0)
      released <- Ref.of[IO, Int](0)
      transactionalIds <- Ref.of[IO, Vector[String]](Vector.empty)
      secondGeneration <- Deferred[IO, Unit]
      generation = Stream.suspend {
        Stream.eval(UUIDGen[IO].randomUUID.map(id => s"hiring-publisher-$id")).flatMap { transactionalId =>
          Stream.eval(transactionalIds.update(_ :+ transactionalId)) >>
            Stream
              .resource(Resource.make(acquired.update(_ + 1))(_ => released.update(_ + 1)))
              .flatMap { _ =>
                Stream.eval(acquired.get.flatMap {
                  case 1 =>
                    IO.raiseError[Unit](
                      new IllegalStateException("wrapped fence", new ProducerFencedException("fenced"))
                    )
                  case _ => secondGeneration.complete(()).void *> IO.never[Unit]
                })
              }
        }
      }
      supervised = Stream
        .suspend(
          OperationalEventKafkaRuntime
            .resilientStream(
              Diagnostics.noop,
              generation,
              1.millis,
              OperationalEventKafkaRuntime.isProducerFenced
            )
            .handleErrorWith {
              case error if OperationalEventKafkaRuntime.isProducerFenced(error) => Stream.empty
              case error                                                         => Stream.raiseError[IO](error)
            }
        )
        .repeat
      fiber <- supervised.compile.drain.start
      _ <- secondGeneration.get
      _ <- fiber.cancel
      observedAcquired <- acquired.get
      observedReleased <- released.get
      observedTransactionalIds <- transactionalIds.get
    } yield {
      assertEquals(observedAcquired, 2)
      assertEquals(observedReleased, 2)
      assertEquals(observedTransactionalIds.size, 2)
      assertEquals(observedTransactionalIds.distinct.size, 2)
    }
  }

  test("resilient stream emits sanitized runtime failure diagnostics") {
    for {
      attempts <- Ref.of[IO, Int](0)
      records <- Ref.of[IO, Vector[(LogEvent, Map[LogField, String])]](Vector.empty)
      diagnostics = new Diagnostics {
        override def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
          records.update(_ :+ (event -> fields))
      }
      _ <- OperationalEventKafkaRuntime
        .resilientStream(
          diagnostics,
          fs2.Stream.eval {
            attempts.updateAndGet(_ + 1).flatMap { count =>
              if (count == 1) IO.raiseError[Unit](new IllegalStateException("transient kafka failure"))
              else IO.unit
            }
          },
          1.millis
        )
        .compile
        .drain
      emitted <- records.get
    } yield {
      assertEquals(emitted.map(_._1), Vector(LogEvent.RuntimeFailed))
      assertEquals(emitted.head._2.get(LogField.ErrorType), Some("java.lang.IllegalStateException"))
    }
  }

  test("resilient stream cancellation interrupts retry backoff") {
    for {
      attempted <- Deferred[IO, Unit]
      fiber <- OperationalEventKafkaRuntime
        .resilientStream(
          Diagnostics.noop,
          fs2.Stream.eval(
            attempted.complete(()) *> IO.raiseError[Unit](new IllegalStateException("broker unavailable"))
          ),
          1.hour
        )
        .compile
        .drain
        .start
      _ <- attempted.get
      _ <- fiber.cancel
    } yield assert(true)
  }
}
