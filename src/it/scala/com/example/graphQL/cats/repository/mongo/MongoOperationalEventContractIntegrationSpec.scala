package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.events.*
import com.example.graphQL.cats.service.port.PendingSearchSessionWork
import org.bson.Document
import org.bson.types.Binary
import java.time.Instant
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*
import scala.concurrent.duration.*
import io.circe.Json

/** Invalid immutable facts retain deletion evidence while later valid publication remains recoverable. */
class MongoOperationalEventContractIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 3.minutes
  private val now = Instant.parse("2026-10-07T12:00:00Z")

  test("claims preserve immutable nanosecond event bytes while validating millisecond BSON metadata") {
    mongoResource.use { fixture =>
      val value = event(UserId(UUID.randomUUID())).copy(occurredAt = now.plusNanos(123456789L))
      val repository =
        MongoOperationalEventOutboxRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.EventOutbox, document(value))
        claimed <- repository
          .claim("precision-test", s"hiring-publisher-${UUID.randomUUID()}", now.plusSeconds(1), now.plusSeconds(30), 1)
          .value
          .flatMap(result)
      } yield {
        assertEquals(claimed.map(_.event), List(value))
        assertEquals(claimed.map(_.envelopeBytes.toVector), List(OperationalEventJson.bytes(value).toVector))
      }
    }
  }

  test("invalid or mismatched search events reject before durable work enqueue") {
    mongoResource.use { fixture =>
      val actor = UserId(UUID.randomUUID())
      val session =
        SearchSession(UUID.randomUUID(), actor, "jobs", None, Json.obj(), None, Nil, now, now.plusSeconds(60))
      val valid =
        OperationalEvents.searchPerformed(UUID.randomUUID(), session).fold(error => fail(error.toString), identity)
      val invalid =
        List(valid.copy(payload = Json.obj()), valid.copy(actorId = UserId(UUID.randomUUID())), event(actor))
      val repository =
        MongoSearchSessionWorkRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        results <- invalid.traverse(value => repository.enqueue(PendingSearchSessionWork(session, value), now).value)
        count <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.SearchSessionWork)
      } yield {
        assert(results.forall(_ == Left(RepositoryError.InvalidEvent)))
        assertEquals(count, 0L)
      }
    }
  }

  private def event(actor: UserId): OperationalEventEnvelope = {
    val id = UUID.randomUUID()
    OperationalEventEnvelope(
      UUID.randomUUID(),
      OperationalEventType.JOB_CREATED,
      now,
      OperationalAggregateType.Job,
      id.toString,
      actor,
      Json.obj(
        "job" -> Json.obj(
          "jobId" -> Json.fromString(id.toString),
          "skills" -> Json.arr(Json.fromString("Scala")),
          "status" -> Json.fromString("Open")
        )
      )
    )
  }

  private def document(value: OperationalEventEnvelope): Document =
    MongoHiringCodecs.outboxRecord(value, now).fold(message => fail(message), identity)

  private def result[A](value: Either[RepositoryError, A]): IO[A] =
    value.fold(error => IO.raiseError(new AssertionError(s"Unexpected repository result: $error")), IO.pure)

  private def wireBytes(row: Document): Vector[Byte] = row.get("envelopeBytes") match {
    case value: Binary      => value.getData.toVector
    case value: Array[Byte] => value.toVector
    case _                  => fail("Expected stored envelope bytes")
  }

  private val corruptions: List[(String, Document => Document)] = List(
    "malformed payload" -> (row => row.append("payload", "{\"unexpected\":\"retained\"}")),
    "malformed wire bytes" -> (row => row.append("envelopeBytes", new Binary(Array[Byte](1, 2, 3)))),
    "oversized wire bytes" -> (row => row.append("envelopeBytes", new Binary(Array.fill[Byte](262145)(32)))),
    "different wire event" -> (row =>
      row.append("envelopeBytes", new Binary(OperationalEventJson.bytes(event(UserId(UUID.randomUUID())))))
    )
  )

  corruptions.foreach { case (description, corrupt) =>
    test(s"$description fails terminally without changing evidence or blocking a healthy fact") {
      mongoResource.use { fixture =>
        val bad = corrupt(document(event(UserId(UUID.randomUUID()))))
          .append("availableAt", Date.from(now.minusSeconds(1)))
        val healthy = event(UserId(UUID.randomUUID()))
        val originalPayload = bad.getString("payload")
        val originalBytes = wireBytes(bad)
        val originalSubjects = bad.getList("subjectIds", classOf[String]).asScala.toVector
        val repository =
          MongoOperationalEventOutboxRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
        for {
          _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
          _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.EventOutbox, bad)
          _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.EventOutbox, document(healthy))
          claimed <- repository
            .claim("contract-test", s"hiring-publisher-${UUID.randomUUID()}", now, now.plusSeconds(30), 1)
            .value
            .flatMap(result)
          retained <- MongoRepositoryTestSupport.findOne(
            fixture.database,
            MongoCollections.EventOutbox,
            new Document("_id", bad.getString("_id"))
          )
          saved <- retained.fold(IO.raiseError[Document](new AssertionError("Invalid event evidence disappeared")))(
            IO.pure
          )
        } yield {
          assertEquals(claimed.map(_.event.eventId), List(healthy.eventId))
          assertEquals(saved.getString("state"), "Failed")
          assertEquals(saved.getString("lastError"), "INVALID_EVENT_CONTRACT")
          assertEquals(saved.getString("payload"), originalPayload)
          assertEquals(wireBytes(saved), originalBytes)
          assertEquals(saved.getList("subjectIds", classOf[String]).asScala.toVector, originalSubjects)
          assert(!saved.containsKey("leaseToken"))
        }
      }
    }
  }

  List[(String, Document => Document)](
    "invalid deletion attribution" -> (_.append("subjectIds", java.util.List.of("invalid-subject"))),
    "overflowing attempt counter" -> (_.append("attempts", Long.box(Int.MaxValue.toLong)))
  ).foreach { case (description, corrupt) =>
    test(s"$description fails closed and leaves the fact unchanged") {
      mongoResource.use { fixture =>
        val bad = corrupt(document(event(UserId(UUID.randomUUID()))))
        val repository =
          MongoOperationalEventOutboxRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
        for {
          _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
          _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.EventOutbox, bad)
          claimed <- repository
            .claim("contract-test", s"hiring-publisher-${UUID.randomUUID()}", now, now.plusSeconds(30), 1)
            .value
          retained <- MongoRepositoryTestSupport.findOne(
            fixture.database,
            MongoCollections.EventOutbox,
            new Document("_id", bad.getString("_id"))
          )
        } yield {
          assertEquals(claimed, Left(RepositoryError.InvalidStoredData))
          assertEquals(retained.map(_.getString("state")), Some("Retryable"))
          assert(retained.exists(row => row.get("leaseToken") == null))
        }
      }
    }
  }
}
