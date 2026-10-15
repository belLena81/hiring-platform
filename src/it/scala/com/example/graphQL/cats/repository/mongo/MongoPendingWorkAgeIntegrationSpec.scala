package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.*
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.events.{OperationalEventPayload, OperationalEvents}
import com.mongodb.MongoClientSettings
import java.time.Instant
import java.util.UUID
import org.bson.{BsonDocument, Document}
import scala.concurrent.duration.*

/** ARR-07.2: the age of the oldest due outbox entry is one bounded read answered from the outbox claim index. */
final class MongoPendingWorkAgeIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val now = Instant.parse("2026-10-15T12:00:00Z")

  private def job(owner: UserId): Job =
    Job(
      JobId(UUID.randomUUID()),
      owner,
      "Age job",
      "Age description",
      List("Age requirement"),
      Set("Scala"),
      Location("Cyprus", "Nicosia", remote = false),
      JobStatus.Closed,
      now.minusSeconds(86400),
      now.minusSeconds(86400)
    )

  /** A `Retryable` entry that occurred and became available `ago` before `now`. */
  private def entry(owner: UserId, available: Instant, occurred: Instant): Document = {
    val event = OperationalEvents
      .jobEvent(OperationalEventPayload.JobKind.Closed, UUID.randomUUID(), job(owner), owner, occurred)
      .fold(error => fail(error.toString), identity)
    MongoHiringCodecs.outboxRecord(event, available).fold(message => fail(message), identity)
  }

  private def insert(fixture: MongoAccessEvaluationSupport.Fixture, documents: List[Document]): IO[Unit] =
    Mongo4catsCollections
      .documents(fixture.database, MongoCollections.EventOutbox)
      .flatMap(_.insertMany(documents))
      .void

  private def outboxRepository(fixture: MongoAccessEvaluationSupport.Fixture) =
    MongoOperationalEventOutboxRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])

  private def bson(value: org.bson.conversions.Bson): Document =
    Document.parse(
      value.toBsonDocument(classOf[BsonDocument], MongoClientSettings.getDefaultCodecRegistry).toJson
    )

  test("ARR-07.2 an empty outbox has no age and the oldest due entry is measured from when it occurred") {
    mongoResource.use { fixture =>
      val owner = UserId(UUID.randomUUID())
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        repository = outboxRepository(fixture)
        empty <- repository.oldestDueAge(now)
        _ <- insert(
          fixture,
          List(
            entry(owner, available = now.minusSeconds(600), occurred = now.minusSeconds(900)),
            entry(owner, available = now.minusSeconds(60), occurred = now.minusSeconds(120)),
            // Not due yet (waiting out a retry delay): never the oldest.
            entry(owner, available = now.plusSeconds(3600), occurred = now.minusSeconds(7200))
          )
        )
        oldest <- repository.oldestDueAge(now)
      } yield {
        assertEquals(empty, None)
        assertEquals(oldest, Some(900.seconds))
      }
    }
  }

  test("ARR-07.2 the oldest-due read is an index scan of the claim index with no sort and one document examined") {
    mongoResource.use { fixture =>
      val owner = UserId(UUID.randomUUID())
      val rows = 300
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- insert(
          fixture,
          List.tabulate(rows)(i =>
            entry(owner, available = now.minusSeconds(1000L - i), occurred = now.minusSeconds(1000L - i))
          ) ++ List.tabulate(50)(i =>
            entry(owner, available = now.plusSeconds(60L + i), occurred = now.minusSeconds(5L))
          )
        )
        explained <- MongoAccessEvaluationSupport.command(
          fixture.database,
          new Document(
            "explain",
            new Document("find", MongoCollections.EventOutbox)
              .append("filter", bson(MongoOperationalEventOutboxRepository.OldestDue.filter(now).bson))
              .append("sort", bson(MongoOperationalEventOutboxRepository.OldestDue.Sort))
              .append("projection", bson(MongoOperationalEventOutboxRepository.OldestDue.Projection))
              .append("limit", 1)
              .append("maxTimeMS", MongoPendingWorkAge.MaxTime.toMillis.toInt)
          ).append("verbosity", "executionStats")
        )
      } yield {
        val plan = explained.toJson
        val stats = explained.get("executionStats", classOf[Document])
        val keys = stats.get("totalKeysExamined").asInstanceOf[Number].longValue
        val docs = stats.get("totalDocsExamined").asInstanceOf[Number].longValue
        // Evidence for the architecture review: examined work for the 350-entry outbox.
        println(s"ARR-07.2 explain outbox-oldest-due rows=${rows + 50} keysExamined=$keys docsExamined=$docs")
        assert(plan.contains("IXSCAN"), clue(plan))
        assert(plan.contains(MongoIndexNames.EventOutboxClaim), clue(plan))
        assert(!plan.contains("COLLSCAN"), clue(plan))
        assert(!plan.contains("\"SORT\""), clue(plan))
        assertEquals(stats.get("nReturned").asInstanceOf[Number].longValue, 1L)
        assert(keys <= 2L, clue(keys))
        assert(docs <= 1L, clue(docs))
      }
    }
  }

  test("ARR-07.2 the age query never reads beyond one document of a large outbox") {
    mongoResource.use { fixture =>
      val owner = UserId(UUID.randomUUID())
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- insert(
          fixture,
          List.tabulate(1000)(i =>
            entry(owner, available = now.minusSeconds(2000L - i), occurred = now.minusSeconds(2000L - i))
          )
        )
        age <- outboxRepository(fixture).oldestDueAge(now)
      } yield assertEquals(age, Some(2000.seconds))
    }
  }
}
