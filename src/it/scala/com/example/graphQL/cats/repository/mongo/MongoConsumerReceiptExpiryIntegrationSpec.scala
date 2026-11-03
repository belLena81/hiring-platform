package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.JobStatus
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.events.{
  OperationalAggregateType,
  OperationalEventEnvelope,
  OperationalEventPayload,
  OperationalEventType
}
import com.mongodb.client.model.Filters
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import scala.concurrent.duration.*

/** A duplicate delivery extends the receipt to the latest delivery and never shortens it (DDC-02.13). */
final class MongoConsumerReceiptExpiryIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val now = Instant.now().truncatedTo(ChronoUnit.MILLIS)

  private def event: OperationalEventEnvelope = {
    val jobId = UUID.randomUUID()
    OperationalEventEnvelope(
      UUID.randomUUID(),
      OperationalEventType.JOB_CREATED,
      now,
      OperationalAggregateType.Job,
      jobId.toString,
      UserId(UUID.randomUUID()),
      OperationalEventPayload.JobFact(
        OperationalEventPayload.JobKind.Created,
        OperationalEventPayload.JobSnapshot(jobId, List("Scala"), JobStatus.Open)
      )
    )
  }

  private def storedExpiry(
      database: mongo4cats.database.MongoDatabase[IO],
      envelope: OperationalEventEnvelope
  ): IO[Instant] =
    MongoRepositoryTestSupport
      .findOne(database, MongoCollections.ConsumerReceipts, Filters.eq(MongoFields.Id, s"g:${envelope.eventId}"))
      .flatMap(IO.fromOption(_)(new AssertionError("Expected stored receipt")))
      .map(_.getDate(MongoFields.ExpiresAt).toInstant)

  test("DDC-02.13 a duplicate delivery a day later extends expiresAt to the later delivery") {
    mongoResource.use { fixture =>
      val database = fixture.database
      val envelope = event
      val receipts = new MongoConsumerReceiptRepository(database, Diagnostics.noop[IO])
      val firstExpiry = now.plus(9, ChronoUnit.DAYS)
      val laterExpiry = now.plus(10, ChronoUnit.DAYS)
      for {
        _ <- MongoHiringSetup.initialize(database, Diagnostics.noop[IO])
        first <- receipts.record("g", envelope, now, firstExpiry).value
        second <- receipts.record("g", envelope, now.plus(1, ChronoUnit.DAYS), laterExpiry).value
        expiry <- storedExpiry(database, envelope)
        rows <- MongoRepositoryTestSupport.count(
          database,
          MongoCollections.ConsumerReceipts,
          Filters.eq(MongoFields.EventId, envelope.eventId.toString)
        )
      } yield {
        assertEquals(first, Right(true))
        assertEquals(second, Right(false))
        assertEquals(expiry, laterExpiry)
        assertEquals(rows, 1L)
      }
    }
  }

  test("DDC-02.13 an earlier expiry never shortens a stored receipt") {
    mongoResource.use { fixture =>
      val database = fixture.database
      val envelope = event
      val receipts = new MongoConsumerReceiptRepository(database, Diagnostics.noop[IO])
      val storedUntil = now.plus(10, ChronoUnit.DAYS)
      for {
        _ <- MongoHiringSetup.initialize(database, Diagnostics.noop[IO])
        _ <- receipts.record("g", envelope, now, storedUntil).value
        duplicate <- receipts.record("g", envelope, now, now.plus(9, ChronoUnit.DAYS)).value
        expiry <- storedExpiry(database, envelope)
      } yield {
        assertEquals(duplicate, Right(false))
        assertEquals(expiry, storedUntil)
      }
    }
  }
}
