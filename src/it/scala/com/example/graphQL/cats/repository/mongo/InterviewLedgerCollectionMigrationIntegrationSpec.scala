package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.Filters
import mongo4cats.database.MongoDatabase
import org.bson.Document
import scala.concurrent.duration.*

/** `017_interview_ledger_collections` on a real replica set: rename with data preserved, fail-closed conflicts, restart
  * after interruption and concurrent starts.
  */
final class InterviewLedgerCollectionMigrationIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val renames = MongoInterviewLedgerCollectionMigrations.Renames
  private val ledgerId = MigrationIds.InterviewLedgerCollections.value

  private def initialize(database: MongoDatabase[IO]): IO[Unit] =
    MongoHiringSetup.initialize(database, Diagnostics.noop)

  private def present(database: MongoDatabase[IO], names: List[String]): IO[Set[String]] =
    MongoHiringSetup.setupDatabase(database).flatMap(MongoHiringValidators.collectionOptions(_, names)).map(_.keySet)

  private def indexNames(database: MongoDatabase[IO], collection: String): IO[List[String]] =
    MongoRepositoryTestSupport
      .collection(database, collection)
      .flatMap(_.listIndexes[Document])
      .map(_.toList.map(_.getString("name")))

  private def ledgerState(database: MongoDatabase[IO]): IO[Option[String]] =
    MongoRepositoryTestSupport
      .findOne(database, MongoCollections.HiringMigrationLedger, Filters.eq(MongoFields.Id, ledgerId))
      .map(_.map(_.getString("state")))

  private def reservation(id: String): Document =
    new Document(MongoFields.Id, id)
      .append("workflowId", id)
      .append("reserveKey", s"$id:reserve")
      .append("releaseKey", s"$id:release")

  private def documents(database: MongoDatabase[IO], collection: String): IO[List[Document]] =
    MongoRepositoryTestSupport
      .collection(database, collection)
      .flatMap(_.find.all)
      .map(_.toList.sortBy(_.getString("_id")))

  /** Puts the three ledgers back under their original names, with stale original-named indexes and no ledger row. */
  private def downgrade(database: MongoDatabase[IO], pairs: List[(String, String)] = renames): IO[Unit] =
    pairs.traverse_ { case (oldName, newName) =>
      for {
        collection <- MongoRepositoryTestSupport.collection(database, newName)
        _ <- collection.dropIndexes
        _ <- collection.renameCollection(mongo4cats.models.collection.MongoNamespace(database.name, oldName))
        legacy <- MongoRepositoryTestSupport.collection(database, oldName)
        _ <- legacy.createIndex(
          com.mongodb.client.model.Indexes.ascending("marker"),
          new com.mongodb.client.model.IndexOptions().name(s"${oldName}_marker")
        )
      } yield ()
    } *> MongoRepositoryTestSupport
      .collection(database, MongoCollections.HiringMigrationLedger)
      .flatMap(_.deleteOne(Filters.eq(MongoFields.Id, ledgerId)))
      .void

  private val newNames = renames.map(_._2)
  private val oldNames = renames.map(_._1)

  test("a fresh installation creates only the business-named ledgers and records the proof") {
    mongoResource.use { fixture =>
      for {
        _ <- initialize(fixture.database)
        created <- present(fixture.database, newNames ++ oldNames)
        indexes <- newNames.flatTraverse(indexNames(fixture.database, _))
        state <- ledgerState(fixture.database)
      } yield {
        assertEquals(created, newNames.toSet)
        assert(indexes.forall(!_.startsWith("fake_")), clue(indexes))
        assertEquals(state, Some("Complete"))
      }
    }
  }

  test("an upgrade renames every ledger, preserves its documents and recreates indexes under business names") {
    mongoResource.use { fixture =>
      val (reservations, locks, receipts) = (newNames(0), newNames(1), newNames(2))
      for {
        _ <- initialize(fixture.database)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          reservations,
          reservation("00000000-0000-0000-0000-000000000001")
        )
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          locks,
          new Document("_id", "participant").append("fence", Long.box(3L))
        )
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, receipts, new Document("_id", "receipt-1"))
        before <- newNames.traverse(documents(fixture.database, _))
        _ <- downgrade(fixture.database)
        legacyOnly <- present(fixture.database, newNames ++ oldNames)
        _ = assertEquals(legacyOnly, oldNames.toSet)
        _ <- initialize(fixture.database)
        after <- newNames.traverse(documents(fixture.database, _))
        remaining <- present(fixture.database, newNames ++ oldNames)
        indexes <- newNames.flatTraverse(indexNames(fixture.database, _))
        state <- ledgerState(fixture.database)
      } yield {
        assertEquals(after, before)
        assertEquals(remaining, newNames.toSet)
        assert(indexes.forall(!_.startsWith("fake_")), clue(indexes))
        assert(indexes.contains(MongoIndexNames.InterviewCalendarRelease), clue(indexes))
        assertEquals(state, Some("Complete"))
      }
    }
  }

  test("when an original-named and a business-named ledger both exist the migration fails closed and keeps the data") {
    mongoResource.use { fixture =>
      val oldReservations = oldNames.head
      for {
        _ <- initialize(fixture.database)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          newNames.head,
          reservation("00000000-0000-0000-0000-000000000002")
        )
        _ <- downgrade(fixture.database, renames.take(1))
        _ <- MongoHiringSetup.setupDatabase(fixture.database).flatMap(_.ensureCollection(newNames.head))
        outcome <- initialize(fixture.database).attempt
        legacyRows <- documents(fixture.database, oldReservations)
        state <- ledgerState(fixture.database)
      } yield {
        assertEquals(outcome.left.toOption.map(_.getClass.getSimpleName), Some("CollectionRenameConflict"))
        assertEquals(legacyRows.size, 1)
        assertNotEquals(state, Some("Complete"))
      }
    }
  }

  test("an interrupted run resumes: ledgers already renamed stay, the rest are renamed and the proof completes") {
    mongoResource.use { fixture =>
      for {
        _ <- initialize(fixture.database)
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, newNames(2), new Document("_id", "receipt-2"))
        _ <- downgrade(fixture.database)
        // The first ledger was already renamed when the previous process stopped.
        _ <- MongoRepositoryTestSupport
          .collection(fixture.database, oldNames.head)
          .flatMap(_.dropIndexes)
        _ <- MongoRepositoryTestSupport
          .collection(fixture.database, oldNames.head)
          .flatMap(
            _.renameCollection(mongo4cats.models.collection.MongoNamespace(fixture.database.name, newNames.head))
          )
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.HiringMigrationLedger,
          new Document(MongoFields.Id, ledgerId).append("version", Long.box(1L)).append("state", "Running")
        )
        _ <- initialize(fixture.database)
        remaining <- present(fixture.database, newNames ++ oldNames)
        rows <- documents(fixture.database, newNames(2))
        state <- ledgerState(fixture.database)
      } yield {
        assertEquals(remaining, newNames.toSet)
        assertEquals(rows.map(_.getString("_id")), List("receipt-2"))
        assertEquals(state, Some("Complete"))
      }
    }
  }

  test("concurrent starts over original-named ledgers both succeed with one consistent result") {
    mongoResource.use { fixture =>
      for {
        _ <- initialize(fixture.database)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          newNames.head,
          reservation("00000000-0000-0000-0000-000000000003")
        )
        _ <- downgrade(fixture.database)
        results <- List.fill(4)(initialize(fixture.database).attempt).parSequence
        remaining <- present(fixture.database, newNames ++ oldNames)
        rows <- documents(fixture.database, newNames.head)
        state <- ledgerState(fixture.database)
      } yield {
        assert(results.forall(_.isRight), clue(results))
        assertEquals(remaining, newNames.toSet)
        assertEquals(rows.size, 1)
        assertEquals(state, Some("Complete"))
      }
    }
  }
}
