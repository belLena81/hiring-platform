package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.events.*
import io.circe.Json
import munit.CatsEffectSuite
import org.bson.Document
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*

final class MongoOutboxTransactionRetryIntegrationSpec extends CatsEffectSuite {
  test("a transient outbox claim error reaches the transaction retry owner and claims once") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val now = Instant.parse("2026-10-05T00:00:00Z")
      val jobId = UUID.randomUUID().toString
      val event = OperationalEventEnvelope(
        UUID.randomUUID(),
        OperationalEventType.JOB_VIEWED,
        now,
        OperationalAggregateType.Job,
        jobId,
        UserId(UUID.randomUUID()),
        Json.obj("jobId" -> Json.fromString(jobId))
      )
      val outbox =
        MongoOperationalEventOutboxRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
      val document = MongoHiringCodecs.outboxRecord(event, now).fold(error => fail(error), identity)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.EventOutbox, document)
        admin <- fixture.client.getDatabase("admin")
        _ <- MongoAccessEvaluationSupport.command(
          admin,
          new Document("configureFailPoint", "failCommand")
            .append("mode", new Document("times", 1))
            .append(
              "data",
              new Document("failCommands", List("findAndModify").asJava)
                .append("errorCode", 112)
                .append("errorLabels", List("TransientTransactionError").asJava)
            )
        )
        claimed <- outbox.claim("retry-fixture", "retry-transaction", now, now.plusSeconds(30), 1).value
        _ = assertEquals(claimed.map(_.map(_.event.eventId)), Right(List(event.eventId)))
        repeated <- outbox.claim("other-fixture", "other-transaction", now, now.plusSeconds(30), 1).value
        _ = assertEquals(repeated.map(_.size), Right(0))
        stored <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.EventOutbox,
          new Document("_id", event.eventId.toString)
        )
        _ = assertEquals(stored.map(_.getInteger("attempts").intValue()), Some(1))
      } yield ()
    }
  }
  test("a transient deleted-subject suppression error retries and leaves the event failed without a claim") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val now = Instant.parse("2026-10-05T00:00:00Z")
      val actor = User(
        UserId(UUID.randomUUID()),
        None,
        "Deleted synthetic actor",
        UserRole.Candidate,
        None,
        now,
        accountStatus = AccountStatus.Deleted,
        deletedAt = Some(now)
      )
      val jobId = UUID.randomUUID().toString
      val event = OperationalEventEnvelope(
        UUID.randomUUID(),
        OperationalEventType.JOB_VIEWED,
        now,
        OperationalAggregateType.Job,
        jobId,
        actor.id,
        Json.obj("jobId" -> Json.fromString(jobId))
      )
      val outbox =
        MongoOperationalEventOutboxRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
      val document = MongoHiringCodecs.outboxRecord(event, now).fold(error => fail(error), identity)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.Users,
          MongoHiringCodecs.user(actor)
        )
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.EventOutbox, document)
        admin <- fixture.client.getDatabase("admin")
        _ <- MongoAccessEvaluationSupport.command(
          admin,
          new Document("configureFailPoint", "failCommand")
            .append("mode", new Document("times", 1))
            .append(
              "data",
              new Document("failCommands", List("update").asJava)
                .append("errorCode", 112)
                .append("errorLabels", List("TransientTransactionError").asJava)
            )
        )
        claimed <- outbox.claim("retry-fixture", "retry-transaction", now, now.plusSeconds(30), 1).value
        _ = assertEquals(claimed.map(_.size), Right(0))
        stored <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.EventOutbox,
          new Document("_id", event.eventId.toString)
        )
        _ = assertEquals(stored.map(_.getString("state")), Some("Failed"))
        _ = assertEquals(stored.map(_.getString("lastError")), Some("SUBJECT_DELETED"))
        _ = assertEquals(stored.map(_.getInteger("attempts").intValue()), Some(1))
      } yield ()
    }
  }

}
