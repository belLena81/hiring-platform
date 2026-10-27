package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.*
import com.example.graphQL.cats.infrastructure.telemetry.BacklogSnapshot
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.events.{OperationalEventPayload, OperationalEvents}
import com.example.graphQL.cats.service.port.{EmbeddingWorkFailure, EmbeddingWorkKey, EmbeddingWorkKind}
import java.time.Instant
import java.util.UUID
import com.mongodb.MongoClientSettings
import org.bson.{BsonDocument, Document}
import scala.concurrent.duration.*

/** Backlog sampling reads pending counts and the oldest pending age from the durable queues. */
final class MongoBacklogSourcesIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val now = Instant.parse("2026-10-15T12:00:00Z")

  private def job(owner: UserId): Job =
    Job(
      JobId(UUID.randomUUID()),
      owner,
      "Backlog job",
      "Backlog description",
      List("Backlog requirement"),
      Set("Scala"),
      Location("Cyprus", Some("Nicosia"), remote = false),
      JobState.Closed(now.minusSeconds(86400)),
      now.minusSeconds(86400),
      now.minusSeconds(86400)
    )

  private def outboxEntry(
      owner: UserId,
      available: Instant,
      occurred: Instant,
      state: String = "Retryable",
      leaseUntil: Option[Instant] = None
  ): Document = {
    val event = OperationalEvents
      .jobEvent(OperationalEventPayload.JobKind.Closed, UUID.randomUUID(), job(owner), owner, occurred)
      .fold(error => fail(error.toString), identity)
    val record = MongoHiringCodecs.outboxRecord(event, available).fold(message => fail(message), identity)
    record.put(MongoFields.State, state)
    leaseUntil.foreach { until =>
      record.put(MongoFields.LeaseOwner, "crashed-worker")
      record.put(MongoFields.LeaseToken, UUID.randomUUID().toString)
      record.put(MongoFields.LeaseUntil, java.util.Date.from(until))
    }
    record
  }

  private def insertOutbox(fixture: MongoAccessEvaluationSupport.Fixture, documents: List[Document]): IO[Unit] =
    Mongo4catsCollections
      .documents(fixture.database, MongoCollections.EventOutbox)
      .flatMap(_.insertMany(documents))
      .void

  private def bson(value: org.bson.conversions.Bson): Document =
    Document.parse(value.toBsonDocument(classOf[BsonDocument], MongoClientSettings.getDefaultCodecRegistry).toJson)

  /** The server's plan for the capped count: an index scan only, with no collection scan. */
  private def assertIndexedCount(
      fixture: MongoAccessEvaluationSupport.Fixture,
      collection: String,
      filter: MongoFilter,
      index: String
  ): IO[Unit] =
    MongoAccessEvaluationSupport
      .command(
        fixture.database,
        new Document(
          "explain",
          new Document("count", collection)
            .append("query", bson(filter.bson))
            .append("limit", MongoBacklogSources.MaxCounted)
            .append("maxTimeMS", MongoPendingWorkAge.MaxTime.toMillis.toInt)
        ).append("verbosity", "executionStats")
      )
      .map { explained =>
        val plan = explained.toJson
        assert(plan.contains("IXSCAN"), clue(plan))
        assert(plan.contains(index), clue(plan))
        assert(!plan.contains("COLLSCAN"), clue(plan))
      }

  private def unwrap[A](result: Either[com.example.graphQL.cats.service.RepositoryError, A]): A =
    result.fold(error => fail(error.toString), identity)

  private def key(n: Int) = EmbeddingWorkKey(EmbeddingWorkKind.Job, s"backlog-$n")

  /** The plan of one `limit(1)` age read: an index scan on the named index, no collection scan, one row returned. */
  private def explainAgeRead(
      fixture: MongoAccessEvaluationSupport.Fixture,
      collection: String,
      read: MongoBacklogSources.AgeRead,
      index: String
  ): IO[String] =
    MongoAccessEvaluationSupport
      .command(
        fixture.database,
        new Document(
          "explain",
          new Document("find", collection)
            .append("filter", bson(read.filter(now).bson))
            .append("sort", bson(MongoBacklogSources.ByAvailability))
            .append("projection", bson(MongoBacklogSources.ageProjection(read)))
            .append("limit", 1)
            .append("maxTimeMS", MongoPendingWorkAge.MaxTime.toMillis.toInt)
        ).append("verbosity", "executionStats")
      )
      .map { explained =>
        val plan = explained.toJson
        val stats = explained.get("executionStats", classOf[Document])
        println(
          s"backlog age read plan collection=$collection filter=${read.filter(now).bson} keys=${stats.get("totalKeysExamined")} docs=${stats.get("totalDocsExamined")} sort=${plan.contains("\"SORT\"")}"
        )
        assert(plan.contains("IXSCAN"), clue(plan))
        assert(plan.contains(index), clue(plan))
        assert(!plan.contains("COLLSCAN"), clue(plan))
        plan
      }

  test("each limit(1) age read is an index scan on the queue claim index, for both capabilities and both parts") {
    mongoResource.use { fixture =>
      val owner = UserId(UUID.randomUUID())
      val work = new MongoEmbeddingWorkRepository(fixture.database, Diagnostics.noop[IO])
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        // A populated queue, so the plan is not the trivial empty-collection plan.
        _ <- (1 to 60).toList.traverse_(i => work.enqueue(key(i), now.minusSeconds(i.toLong)).value.map(unwrap))
        _ <- insertOutbox(
          fixture,
          List.tabulate(60)(i =>
            outboxEntry(
              owner,
              now.minusSeconds(i.toLong + 1),
              now.minusSeconds(i.toLong + 1),
              state = if (i % 3 == 0) "InFlight" else "Retryable",
              leaseUntil = Option.when(i % 3 == 0)(now.minusSeconds(5))
            )
          )
        )
        _ <- MongoBacklogSources.embeddingAgeReads.traverse_(
          explainAgeRead(fixture, MongoCollections.EmbeddingWork, _, MongoIndexNames.EmbeddingWorkAvailable)
        )
        _ <- MongoBacklogSources.outboxAgeReads.traverse_(
          explainAgeRead(fixture, MongoCollections.EventOutbox, _, MongoIndexNames.EventOutboxClaim)
        )
      } yield ()
    }
  }

  private val expiredAt = now.plusSeconds(120)

  test("an empty embedding work queue and an empty outbox are zero pending with no oldest age") {
    mongoResource.use { fixture =>
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        embedding <- MongoBacklogSources.embeddingWork(fixture.database).sample(now)
        outbox <- MongoBacklogSources.operationalEventOutbox(fixture.database).sample(now)
      } yield {
        assertEquals(embedding, BacklogSnapshot(0, None))
        assertEquals(outbox, BacklogSnapshot(0, None))
      }
    }
  }

  test("embedding work is pending when due, and when its lease expired after a crash, but not under a live lease") {
    mongoResource.use { fixture =>
      val work = new MongoEmbeddingWorkRepository(fixture.database, Diagnostics.noop[IO])
      val source = MongoBacklogSources.embeddingWork(fixture.database)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- work.enqueue(key(1), now.minusSeconds(300)).value.map(unwrap)
        _ <- work.enqueue(key(2), now.minusSeconds(60)).value.map(unwrap)
        _ <- work.enqueue(key(3), now.plusSeconds(3600)).value.map(unwrap)
        due <- source.sample(now)
        // The oldest row is leased and its worker then crashes: the lease is live, then expires.
        claimed <- work.claim("crashing-worker", now, now.plusSeconds(60)).value.map(unwrap)
        live <- source.sample(now)
        expired <- source.sample(expiredAt)
        _ <- assertIndexedCount(
          fixture,
          MongoCollections.EmbeddingWork,
          MongoBacklogSources.embeddingClaimable(now),
          MongoIndexNames.EmbeddingWorkAvailable
        )
      } yield {
        assertEquals(due, BacklogSnapshot(2, Some(300.seconds)))
        assertEquals(claimed.map(_.key), Some(key(1)))
        assertEquals(live, BacklogSnapshot(1, Some(60.seconds)))
        // Row 3 became due too; the expired-lease row is the oldest at 420 s.
        assertEquals(expired, BacklogSnapshot(2, Some(420.seconds)))
      }
    }
  }

  test("embedding work in Retry is pending and terminal Failed work is not") {
    mongoResource.use { fixture =>
      val work = new MongoEmbeddingWorkRepository(fixture.database, Diagnostics.noop[IO])
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- work.enqueue(key(1), now.minusSeconds(100)).value.map(unwrap)
        _ <- work.enqueue(key(2), now.minusSeconds(50)).value.map(unwrap)
        first <- work.claim("worker", now, now.plusSeconds(60)).value.map(unwrap)
        _ <- work.fail(first.get, EmbeddingWorkFailure.RetryExhausted, now).value.map(unwrap)
        second <- work.claim("worker", now, now.plusSeconds(60)).value.map(unwrap)
        _ <- work.retry(second.get, now.minusSeconds(20)).value.map(unwrap)
        snapshot <- MongoBacklogSources.embeddingWork(fixture.database).sample(now)
      } yield {
        assertEquals(first.map(_.key), Some(key(1)))
        assertEquals(second.map(_.key), Some(key(2)))
        assertEquals(snapshot, BacklogSnapshot(1, Some(20.seconds)))
      }
    }
  }

  test("outbox counts due and expired-lease entries and ages the oldest, excluding live leases and finished entries") {
    mongoResource.use { fixture =>
      val owner = UserId(UUID.randomUUID())
      val source = MongoBacklogSources.operationalEventOutbox(fixture.database)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- insertOutbox(
          fixture,
          List(
            outboxEntry(owner, available = now.minusSeconds(600), occurred = now.minusSeconds(900)),
            // Not due yet: waiting out a retry delay.
            outboxEntry(owner, available = now.plusSeconds(3600), occurred = now.minusSeconds(7200)),
            // Live lease: a publisher is handling it.
            outboxEntry(
              owner,
              now.minusSeconds(5000),
              now.minusSeconds(5000),
              state = "InFlight",
              leaseUntil = Some(now.plusSeconds(60))
            ),
            // Crashed publisher: the lease expired, so the entry is claimable again and ages from when it occurred.
            outboxEntry(
              owner,
              now.minusSeconds(1200),
              now.minusSeconds(1200),
              state = "InFlight",
              leaseUntil = Some(now.minusSeconds(10))
            ),
            outboxEntry(owner, now.minusSeconds(9000), now.minusSeconds(9000), state = "Published"),
            outboxEntry(owner, now.minusSeconds(9000), now.minusSeconds(9000), state = "Failed")
          )
        )
        snapshot <- source.sample(now)
        _ <- assertIndexedCount(
          fixture,
          MongoCollections.EventOutbox,
          MongoBacklogSources.outboxClaimable(now),
          MongoIndexNames.EventOutboxClaim
        )
      } yield assertEquals(snapshot, BacklogSnapshot(2, Some(1200.seconds)))
    }
  }

  test("the pending count stops at the cap, so a value equal to the cap means at least that many") {
    mongoResource.use { fixture =>
      val owner = UserId(UUID.randomUUID())
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- insertOutbox(
          fixture,
          List.tabulate(5)(i => outboxEntry(owner, now.minusSeconds(100L + i), now.minusSeconds(100L + i)))
        )
        capped <- MongoBacklogSources.operationalEventOutbox(fixture.database, cap = 3).sample(now)
        uncapped <- MongoBacklogSources.operationalEventOutbox(fixture.database).sample(now)
      } yield {
        assertEquals(capped.pending, 3L)
        assertEquals(uncapped.pending, 5L)
      }
    }
  }
}
