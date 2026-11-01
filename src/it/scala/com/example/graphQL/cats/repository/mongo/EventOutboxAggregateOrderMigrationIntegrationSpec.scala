package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.{Filters, IndexOptions, Indexes}
import mongo4cats.database.MongoDatabase
import org.bson.Document

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** `035_event_outbox_aggregate_order`: the partial index behind the outbox claim's per-aggregate order check. */
final class EventOutboxAggregateOrderMigrationIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val ledgerId = MigrationIds.EventOutboxAggregateOrder.value
  private val indexName = MongoIndexNames.EventOutboxAggregateOrder

  private def initialize(database: MongoDatabase[IO]): IO[Unit] =
    MongoHiringSetup.initialize(database, Diagnostics.noop[IO])

  private def outboxIndexes(database: MongoDatabase[IO]): IO[List[Document]] =
    MongoRepositoryTestSupport
      .collection(database, MongoCollections.EventOutbox)
      .flatMap(_.listIndexes[Document].map(_.toList))

  private def ledgerState(database: MongoDatabase[IO]): IO[Option[String]] =
    MongoRepositoryTestSupport
      .findOne(database, MongoCollections.HiringMigrationLedger, Filters.eq(MongoFields.Id, ledgerId))
      .map(_.map(_.getString(MongoFields.State)))

  private def dropIndex(database: MongoDatabase[IO]): IO[Unit] =
    MongoRepositoryTestSupport
      .collection(database, MongoCollections.EventOutbox)
      .flatMap(_.dropIndex(indexName))
      .void

  private def proof(database: MongoDatabase[IO]): IO[CompletedProof] =
    MongoHiringSetup.setupDatabase(database).flatMap(MongoEventOutboxAggregateOrderMigrations.step.whenComplete)

  test("DDR-06.1 a fresh start records 035 and creates the exact partial index") {
    mongoResource.use { fixture =>
      for {
        _ <- initialize(fixture.database)
        indexes <- outboxIndexes(fixture.database)
        state <- ledgerState(fixture.database)
        verdict <- proof(fixture.database)
      } yield {
        val index = indexes.find(_.getString("name") == indexName)
        assertEquals(state, Some("Complete"))
        assertEquals(
          index.map(_.get("key", classOf[Document]).keySet().toArray.toList.map(_.toString)).map(_.toList),
          Some(
            List(MongoFields.PartitionKey, MongoFields.OccurredAt, MongoFields.Id)
          )
        )
        assertEquals(
          index.map(_.get("partialFilterExpression", classOf[Document]).toJson),
          Some(new Document(MongoFields.State, new Document("$in", MongoOutboxClaimBatch.PendingStates.asJava)).toJson)
        )
        assertEquals(verdict, CompletedProof.Trusted)
      }
    }
  }

  test("DDR-06.1 a dropped index reopens the completed step and the next start recreates it") {
    mongoResource.use { fixture =>
      for {
        _ <- initialize(fixture.database)
        _ <- dropIndex(fixture.database)
        verdict <- proof(fixture.database)
        _ <- initialize(fixture.database)
        indexes <- outboxIndexes(fixture.database)
        after <- proof(fixture.database)
        state <- ledgerState(fixture.database)
      } yield {
        assertEquals(verdict, CompletedProof.Reopen)
        assertEquals(indexes.count(_.getString("name") == indexName), 1)
        assertEquals(after, CompletedProof.Trusted)
        assertEquals(state, Some("Complete"))
      }
    }
  }

  test("DDR-06.1 an index of the same name with another definition fails startup closed and is not replaced") {
    mongoResource.use { fixture =>
      for {
        _ <- initialize(fixture.database)
        _ <- dropIndex(fixture.database)
        collection <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.EventOutbox)
        _ <- collection.createIndex(
          Indexes.ascending(MongoFields.PartitionKey, MongoFields.Id),
          new IndexOptions().name(indexName)
        )
        result <- initialize(fixture.database).attempt
        indexes <- outboxIndexes(fixture.database)
      } yield {
        assert(
          result.left.exists(_.isInstanceOf[MigrationError.IndexMismatch]),
          clue(result)
        )
        val kept = indexes.find(_.getString("name") == indexName).map(_.get("key", classOf[Document]).keySet().size())
        assertEquals(kept, Some(2), "the mismatched definition is left for explicit repair")
      }
    }
  }

  test("DDR-06.1 concurrent starts complete once and leave one index") {
    mongoResource.use { fixture =>
      for {
        results <- List.fill(4)(initialize(fixture.database).attempt).parSequence
        _ <- initialize(fixture.database)
        indexes <- outboxIndexes(fixture.database)
        state <- ledgerState(fixture.database)
      } yield {
        assert(results.forall(_.isRight), clue(results))
        assertEquals(indexes.count(_.getString("name") == indexName), 1)
        assertEquals(state, Some("Complete"))
      }
    }
  }
}
