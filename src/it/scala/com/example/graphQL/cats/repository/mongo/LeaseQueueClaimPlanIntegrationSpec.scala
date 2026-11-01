package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.MongoClientSettings
import com.mongodb.client.model.{InsertManyOptions, Sorts}
import org.bson.conversions.Bson
import org.bson.{BsonDocument, Document}

import java.time.Instant
import scala.concurrent.duration.*

/** DDR-01.5: the claimable-work queries of the four lease queues are answered from their claim indexes. Each query is
  * explained twice over the same seeded data (mixed due, expired, live and future rows): the `caller-clock` predicate
  * that DDR-01 replaced (a literal `leaseUntil <= <caller now>`, kept here only as the baseline) and the production
  * `$expr` predicate on `$$NOW`. The test fails if the database-clock query loses its index, falls back to a collection
  * scan, or examines more keys than the baseline.
  */
final class LeaseQueueClaimPlanIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val rows = 4000
  private val now = Instant.now()

  private def bson(value: Bson): Document =
    Document.parse(value.toBsonDocument(classOf[BsonDocument], MongoClientSettings.getDefaultCodecRegistry).toJson)

  private def date(at: Instant): java.util.Date = java.util.Date.from(at)

  /** 5% due, 5% expired, 40% live lease, 50% waiting out a retry delay. */
  private def queueRow(i: Int, readyState: String, inProgress: String, leaseUntil: String): Document = {
    val base = new Document("_id", s"row-${"%05d".format(i)}")
    i % 20 match {
      case 0 => base.append("state", readyState).append("availableAt", date(now.minusSeconds(600L + i)))
      case 1 =>
        base
          .append("state", inProgress)
          .append("availableAt", date(now.minusSeconds(900L + i)))
          .append(leaseUntil, date(now.minusSeconds(60L)))
      case n if n < 10 =>
        base
          .append("state", inProgress)
          .append("availableAt", date(now.minusSeconds(900L + i)))
          .append(leaseUntil, date(now.plusSeconds(3600L)))
      case _ => base.append("state", readyState).append("availableAt", date(now.plusSeconds(3600L + i)))
    }
  }

  private def insert(fixture: MongoAccessEvaluationSupport.Fixture, collection: String, documents: List[Document]) =
    MongoRepositoryTestSupport
      .collection(fixture.database, collection)
      .flatMap(_.insertMany(documents, new InsertManyOptions().bypassDocumentValidation(true)))
      .void

  private final case class Plan(
      label: String,
      indexes: List[String],
      stages: List[String],
      keys: Long,
      docs: Long,
      returned: Long
  ) {
    def line: String =
      s"DDR-01.5 plan $label indexes=${indexes.distinct.mkString(",")} stages=${stages.distinct.mkString(",")} " +
        s"keysExamined=$keys docsExamined=$docs returned=$returned of=$rows"
  }

  private def summarize(label: String, explained: Document): Plan = {
    val json = explained.toJson
    val stats = explained.get("executionStats", classOf[Document])
    def number(name: String) = stats.get(name).asInstanceOf[Number].longValue
    Plan(
      label,
      "\"indexName\": \"([^\"]+)\"".r.findAllMatchIn(json).map(_.group(1)).toList,
      "\"stage\": \"([A-Z_]+)\"".r.findAllMatchIn(json).map(_.group(1)).toList,
      number("totalKeysExamined"),
      number("totalDocsExamined"),
      number("nReturned")
    )
  }

  private def explainClaim(
      fixture: MongoAccessEvaluationSupport.Fixture,
      label: String,
      collection: String,
      filter: Bson,
      sort: Bson
  ): IO[Plan] =
    MongoAccessEvaluationSupport
      .command(
        fixture.database,
        new Document(
          "explain",
          new Document("findAndModify", collection)
            .append("query", bson(filter))
            .append("sort", bson(sort))
            .append("update", new Document("$set", new Document("probe", 1)))
        ).append("verbosity", "executionStats")
      )
      .map(summarize(label, _))

  private def explainFind(
      fixture: MongoAccessEvaluationSupport.Fixture,
      label: String,
      collection: String,
      filter: Bson,
      sort: Bson,
      limit: Int
  ): IO[Plan] =
    MongoAccessEvaluationSupport
      .command(
        fixture.database,
        new Document(
          "explain",
          new Document("find", collection)
            .append("filter", bson(filter))
            .append("sort", bson(sort))
            .append("limit", limit)
        ).append("verbosity", "executionStats")
      )
      .map(summarize(label, _))

  /** The baseline predicate DDR-01 replaced: a literal expiry bound taken from the caller's clock. */
  private def callerClockExpired(field: String): MongoFilter = MongoFilter.lte(field, date(now))

  private val readyStates = List("Ready", "Retry")

  private def legacy(ready: MongoFilter, stateField: String, inProgress: String, until: String): MongoFilter =
    MongoFilter.or(ready, MongoFilter.and(MongoFilter.eq(stateField, inProgress), callerClockExpired(until)))

  private def check(baseline: Plan, current: Plan, expected: Set[String]): Unit = {
    println(baseline.line)
    println(current.line)
    assert(current.stages.contains("IXSCAN"), clue(current.line))
    assert(!current.stages.contains("COLLSCAN"), clue(current.line))
    assertEquals(current.indexes.toSet, expected, clue(current.line))
    assertEquals(baseline.indexes.toSet, expected, clue(baseline.line))
    assert(current.keys <= baseline.keys, clue(current.line -> baseline.line))
    assertEquals(current.returned, baseline.returned, clue(current.line))
  }

  /** A backlog read has no caller-clock twin: it must use the pinned index and never scan the collection. */
  private def checkBacklog(plan: Plan, expected: Set[String]): Unit = {
    println(plan.line)
    assert(plan.stages.contains("IXSCAN"), clue(plan.line))
    assert(!plan.stages.contains("COLLSCAN"), clue(plan.line))
    assertEquals(plan.indexes.toSet, expected, clue(plan.line))
  }

  test("DDR-01.5 the claimable-work query of every lease queue is served by its claim index") {
    mongoResource.use { fixture =>
      val dueWork = MongoLeaseQueue.readyDue(readyStates, now)
      val dueEvents = MongoLeaseQueue.readyDue(List("Retryable"), now)
      val processing = MongoFilter.eq(MongoFields.State, "Processing")
      val after = date(now.minusSeconds(500))
      val occurred = date(now.minusSeconds(4000))
      // The page after a persisted cursor adds the (availableAt, occurredAt, _id) keyset bound to the same predicate.
      val cursor = MongoFilter.or(
        MongoFilter.gt(MongoFields.AvailableAt, after),
        MongoFilter
          .and(MongoFilter.eq(MongoFields.AvailableAt, after), MongoFilter.gt(MongoFields.OccurredAt, occurred)),
        MongoFilter.and(
          MongoFilter.eq(MongoFields.AvailableAt, after),
          MongoFilter.eq(MongoFields.OccurredAt, occurred),
          MongoFilter.gt(MongoFields.Id, "row-00000")
        )
      )
      val workSort = Sorts.ascending(MongoFields.AvailableAt, MongoFields.Id)
      val searchSort = Sorts.ascending(MongoFields.AvailableAt, MongoFields.CreatedAt, MongoFields.Id)
      val outboxSort = Sorts.ascending(MongoFields.AvailableAt, MongoFields.OccurredAt, MongoFields.Id)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- insert(
          fixture,
          MongoCollections.EmbeddingWork,
          List.tabulate(rows)(queueRow(_, "Retry", "Processing", "leaseUntil"))
        )
        _ <- insert(
          fixture,
          MongoCollections.SearchSessionWork,
          List.tabulate(rows)(queueRow(_, "Retry", "Processing", "leaseUntil").append("createdAt", date(now)))
        )
        _ <- insert(
          fixture,
          MongoCollections.EventOutbox,
          List.tabulate(rows)(i =>
            queueRow(i, "Retryable", "InFlight", "leaseUntil")
              .append("occurredAt", date(now.minusSeconds(3600L + i)))
              .append("partitionKey", s"key-${i % 50}")
          )
        )
        _ <- insert(
          fixture,
          MongoCollections.InterviewWorkflowCommands,
          List.tabulate(rows) { i =>
            val row = queueRow(i, "Pending", "Claimed", "claimUntil")
            row.append("commandState", row.remove("state"))
          }
        )
        embeddingBase <- explainClaim(
          fixture,
          "caller-clock embeddingWork.claim",
          MongoCollections.EmbeddingWork,
          legacy(dueWork, "state", "Processing", "leaseUntil").bson,
          workSort
        )
        embeddingNow <- explainClaim(
          fixture,
          "database-clock embeddingWork.claim",
          MongoCollections.EmbeddingWork,
          MongoLeaseQueue.claimable(dueWork, processing).bson,
          workSort
        )
        searchBase <- explainClaim(
          fixture,
          "caller-clock searchSessionWork.claim",
          MongoCollections.SearchSessionWork,
          legacy(dueWork, "state", "Processing", "leaseUntil").bson,
          searchSort
        )
        searchNow <- explainClaim(
          fixture,
          "database-clock searchSessionWork.claim",
          MongoCollections.SearchSessionWork,
          MongoLeaseQueue.claimable(dueWork, processing).bson,
          searchSort
        )
        outboxBase <- explainFind(
          fixture,
          "caller-clock eventOutbox.claimPage",
          MongoCollections.EventOutbox,
          legacy(dueEvents, "state", "InFlight", "leaseUntil").bson,
          outboxSort,
          MongoProducerRegistrations.BatchSize
        )
        outboxNow <- explainFind(
          fixture,
          "database-clock eventOutbox.claimPage",
          MongoCollections.EventOutbox,
          MongoOperationalEventOutboxRepository.Claimable.filter(now).bson,
          outboxSort,
          MongoProducerRegistrations.BatchSize
        )
        cursorBase <- explainFind(
          fixture,
          "caller-clock eventOutbox.claimPage.afterCursor",
          MongoCollections.EventOutbox,
          MongoFilter.and(legacy(dueEvents, "state", "InFlight", "leaseUntil"), cursor).bson,
          outboxSort,
          MongoProducerRegistrations.BatchSize
        )
        cursorNow <- explainFind(
          fixture,
          "database-clock eventOutbox.claimPage.afterCursor",
          MongoCollections.EventOutbox,
          MongoFilter.and(MongoOperationalEventOutboxRepository.Claimable.filter(now), cursor).bson,
          outboxSort,
          MongoProducerRegistrations.BatchSize
        )
        interviewBase <- explainClaim(
          fixture,
          "caller-clock interviewCommands.claim",
          MongoCollections.InterviewWorkflowCommands,
          MongoFilter
            .or(
              MongoFilter.and(
                MongoFilter.in("commandState", List("Pending", "ResultPending")),
                MongoFilter.lte(MongoFields.AvailableAt, date(now))
              ),
              MongoFilter.and(MongoFilter.eq("commandState", "Claimed"), callerClockExpired("claimUntil"))
            )
            .bson,
          workSort
        )
        interviewNow <- explainClaim(
          fixture,
          "database-clock interviewCommands.claim",
          MongoCollections.InterviewWorkflowCommands,
          MongoInterviewCommandQueue.claimable(now).bson,
          workSort
        )
        embeddingCount <- explainFind(
          fixture,
          "backlog embeddingWork.count",
          MongoCollections.EmbeddingWork,
          MongoBacklogSources.embeddingClaimable(now).bson,
          new Document(),
          MongoBacklogSources.MaxCounted
        )
        embeddingAges <- MongoBacklogSources.embeddingAgeReads.zipWithIndex.traverse { case (read, i) =>
          explainFind(
            fixture,
            s"backlog embeddingWork.age$i",
            MongoCollections.EmbeddingWork,
            read.filter(now).bson,
            MongoBacklogSources.ByAvailability,
            1
          )
        }
        outboxCount <- explainFind(
          fixture,
          "backlog eventOutbox.count",
          MongoCollections.EventOutbox,
          MongoBacklogSources.outboxClaimable(now).bson,
          new Document(),
          MongoBacklogSources.MaxCounted
        )
        outboxAges <- MongoBacklogSources.outboxAgeReads.zipWithIndex.traverse { case (read, i) =>
          explainFind(
            fixture,
            s"backlog eventOutbox.age$i",
            MongoCollections.EventOutbox,
            read.filter(now).bson,
            MongoBacklogSources.ByAvailability,
            1
          )
        }
        backlog = List(embeddingCount) ++ embeddingAges
        outboxBacklog = List(outboxCount) ++ outboxAges
      } yield {
        outboxBacklog.foreach(checkBacklog(_, Set("event_outbox_claim")))
        check(embeddingBase, embeddingNow, Set("embedding_work_available_lease"))
        check(searchBase, searchNow, Set("search_session_work_claim"))
        check(outboxBase, outboxNow, Set("event_outbox_claim"))
        check(cursorBase, cursorNow, Set("event_outbox_claim"))
        check(
          interviewBase,
          interviewNow,
          Set("interview_workflow_commands_due", "interview_workflow_commands_lease")
        )
        backlog.foreach(checkBacklog(_, Set("embedding_work_available_lease")))
      }
    }
  }
}
