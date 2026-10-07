package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.mongodb.client.model.Filters
import org.bson.Document
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class MongoDeletedAccountEmbeddingMigrationIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout = 5.minutes
  private val migration = Filters.eq("_id", MongoDeletedAccountEmbeddingMigrations.MigrationId)

  private def setup(fixture: MongoAccessEvaluationSupport.Fixture): IO[MongoHiringSetup.SetupDatabase] =
    List(MongoCollections.Users, MongoCollections.HiringMigrationLedger)
      .traverse { name =>
        MongoRepositoryTestSupport.collection(fixture.database, name).map(name -> _)
      }
      .map(rows => MongoHiringSetup.SetupDatabase(fixture.database, rows.toMap))

  private def tombstone(id: String): Document =
    new Document("_id", id)
      .append("accountStatus", "Deleted")
      .append("version", Long.box(Long.MaxValue))
      .append("safe", true)
      .append("retained", "tombstone")

  test(
    "cleanup removes either field including malformed values in bounded projected batches and preserves other fields"
  ) {
    mongoResource.use { fixture =>
      for {
        database <- setup(fixture)
        users = database.getCollection(MongoCollections.Users)
        _ <- (0 until 503).toList.traverse_ { index =>
          val row = tombstone(f"deleted-$index%04d")
          if (index % 3 != 0) {
            val _ = row.append("embedding", "malformed")
          }
          if (index % 3 != 1) {
            val _ = row.append("embeddingMeta", Int.box(42))
          }
          users.insertOne(row).void
        }
        active = new Document("_id", "active")
          .append("accountStatus", "Active")
          .append("version", Long.box(8L))
          .append("embedding", "keep")
          .append("embeddingMeta", "keep")
        _ <- users.insertOne(active)
        _ <- fixture.commands.clear
        _ <- MongoDeletedAccountEmbeddingMigrations.initialize(database)
        _ <- MongoDeletedAccountEmbeddingMigrations.initialize(database)
        commands <- fixture.commands.snapshot
        rows <- users.find(Filters.eq("accountStatus", "Deleted")).all
        storedActive <- users.find(Filters.eq("_id", "active")).first
        ledger <- database.getCollection(MongoCollections.HiringMigrationLedger).find(migration).first
      } yield {
        assertEquals(rows.size, 503)
        assert(rows.forall(row => !row.containsKey("embedding") && !row.containsKey("embeddingMeta")))
        assert(rows.forall(row => row.getLong("version") == Long.MaxValue && row.getString("retained") == "tombstone"))
        assertEquals(storedActive, Some(active))
        assertEquals(ledger.map(_.get("version")), Some(Long.box(1L)))
        assertEquals(ledger.map(_.getString("state")), Some("Complete"))
        val reads = commands.filter(command =>
          Option(command.get("find")).exists(_.asString().getValue == MongoCollections.Users)
        )
        assert(reads.nonEmpty)
        assert(reads.forall(_.getDocument("projection").keySet().asScala.toSet == Set("_id")))
        assert(reads.forall(command => command.getNumber("limit").intValue() <= 500))
      }
    }
  }

  test("concurrent initialization removes raw non-string identities without resetting completed ledger") {
    mongoResource.use { fixture =>
      for {
        database <- setup(fixture)
        _ <- database
          .getCollection(MongoCollections.Users)
          .insertOne(
            tombstone("temporary").append("_id", new org.bson.types.ObjectId()).append("embedding", "malformed")
          )
        _ <- (
          MongoDeletedAccountEmbeddingMigrations.initialize(database),
          MongoDeletedAccountEmbeddingMigrations.initialize(database)
        ).parTupled
        remaining <- database.getCollection(MongoCollections.Users).find(Filters.exists("embedding", true)).first
        ledger <- database.getCollection(MongoCollections.HiringMigrationLedger).find(migration).first
      } yield {
        assertEquals(remaining, None)
        assertEquals(ledger.map(_.getString("state")), Some("Complete"))
      }
    }
  }

  test("failed batch leaves Running and restart replays remaining rows with validators enabled") {
    mongoResource.use { fixture =>
      for {
        database <- setup(fixture)
        users = database.getCollection(MongoCollections.Users)
        _ <- (0 until 501).toList.traverse_ { index =>
          users
            .insertOne(tombstone(f"deleted-$index%04d").append("safe", index < 500).append("embedding", "erase"))
            .void
        }
        _ <- fixture.database.runCommand(
          new Document("collMod", MongoCollections.Users)
            .append("validator", new Document("safe", true))
            .append("validationLevel", "strict")
            .append("validationAction", "error")
        )
        failed <- MongoDeletedAccountEmbeddingMigrations.initialize(database).attempt
        remaining <- users.count(Filters.exists("embedding", true))
        running <- database.getCollection(MongoCollections.HiringMigrationLedger).find(migration).first
        _ <- fixture.database.runCommand(
          new Document("collMod", MongoCollections.Users)
            .append("validator", new Document())
        )
        _ <- MongoDeletedAccountEmbeddingMigrations.initialize(database)
        after <- users.count(Filters.exists("embedding", true))
      } yield {
        assert(failed.isLeft)
        assertEquals(remaining, 1L)
        assertEquals(running.map(_.getString("state")), Some("Running"))
        assertEquals(after, 0L)
      }
    }
  }

  test("unsupported ledger states and versions fail closed without erasing data") {
    mongoResource.use { fixture =>
      for {
        database <- setup(fixture)
        ledger = database.getCollection(MongoCollections.HiringMigrationLedger)
        _ <- database.getCollection(MongoCollections.Users).insertOne(tombstone("deleted").append("embedding", "erase"))
        _ <- List(
          new Document("version", Int.box(1)).append("state", "Running"),
          new Document("version", Long.box(2L)).append("state", "Running"),
          new Document("version", Long.box(1L)).append("state", "Unknown"),
          new Document("version", Long.box(1L)),
          new Document("state", "Complete")
        ).traverse_ { row =>
          ledger.deleteOne(migration) *> ledger.insertOne(
            row.append("_id", MongoDeletedAccountEmbeddingMigrations.MigrationId)
          ) *>
            MongoDeletedAccountEmbeddingMigrations
              .initialize(database)
              .attempt
              .flatMap(result => IO(assert(result.isLeft)))
        }
        remaining <- database.getCollection(MongoCollections.Users).count(Filters.exists("embedding", true))
      } yield assertEquals(remaining, 1L)
    }
  }

  test("completed migration detects reintroduced fields and refuses to silently repair them") {
    mongoResource.use { fixture =>
      for {
        database <- setup(fixture)
        _ <- MongoDeletedAccountEmbeddingMigrations.initialize(database)
        _ <- database
          .getCollection(MongoCollections.Users)
          .insertOne(tombstone("deleted").append("embeddingMeta", "retained"))
        failed <- MongoDeletedAccountEmbeddingMigrations.initialize(database).attempt
        remaining <- database.getCollection(MongoCollections.Users).count(Filters.exists("embeddingMeta", true))
        ledger <- database.getCollection(MongoCollections.HiringMigrationLedger).find(migration).first
      } yield {
        assert(failed.isLeft)
        assertEquals(remaining, 1L)
        assertEquals(ledger.map(_.getString("state")), Some("Complete"))
      }
    }
  }
}
