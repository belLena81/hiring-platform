package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.MongoWriteException
import com.mongodb.client.model.{Filters, IndexOptions, Indexes}
import mongo4cats.database.MongoDatabase
import org.bson.Document

import java.time.Instant
import java.util.{Date, UUID}
import scala.concurrent.duration.*

/** `036_producer_generation_registrations`: registrations are keyed by subject, transactional id and generation id. */
final class ProducerGenerationRegistrationMigrationIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val Registry = MongoProducerRegistrations.Collection
  private val ledgerId = MigrationIds.ProducerGenerationRegistrations.value
  private val registeredAt = Instant.parse("2026-10-01T08:00:00Z")
  private val fencedAt = Instant.parse("2026-10-02T08:00:00Z")
  private val expiresAt = fencedAt.plusSeconds(8.days.toSeconds)

  private def init(database: MongoDatabase[IO]): IO[Unit] = MongoHiringSetup.initialize(database, Diagnostics.noop[IO])

  private def rows(database: MongoDatabase[IO]): IO[List[Document]] =
    MongoRepositoryTestSupport.collection(database, Registry).flatMap(_.find(Filters.empty()).all).map(_.toList)

  private def legacy(
      subject: String,
      transactionalId: String,
      kind: String,
      state: String
  ): Document = {
    val base = new Document(MongoFields.Id, s"$subject:$transactionalId")
      .append(MongoFields.SubjectId, subject)
      .append(MongoFields.TransactionalId, transactionalId)
      .append(MongoFields.Kind, kind)
      .append(MongoFields.State, state)
      .append("registeredAt", Date.from(registeredAt))
    if (state == "Fenced")
      base.append(MongoFields.FencedAt, Date.from(fencedAt)).append("expiresAt", Date.from(expiresAt))
    else base
  }

  /** A database that completed 012 under the earlier binary: legacy validator and index, no 036 proof. */
  private def rewindTo035(database: MongoDatabase[IO], seeded: List[Document]): IO[Unit] =
    for {
      setup <- MongoHiringSetup.setupDatabase(database)
      collection <- MongoRepositoryTestSupport.collection(database, Registry)
      _ <- collection.deleteMany(Filters.empty())
      _ <- MongoHiringValidators.install(setup, Registry, MongoProducerRegistrationMigrations.registrationValidatorV1)
      _ <- collection.dropIndex(MongoIndexNames.ProducerRegistrationGenerationState)
      _ <- collection.createIndex(
        Indexes.ascending(MongoFields.TransactionalId, MongoFields.State, MongoFields.Id),
        new IndexOptions().name(MongoIndexNames.ProducerRegistrationGenerationState)
      )
      _ <- collection.insertMany(seeded)
      ledger <- MongoRepositoryTestSupport.collection(database, MongoCollections.HiringMigrationLedger)
      _ <- ledger.deleteMany(Filters.eq(MongoFields.Id, ledgerId))
    } yield ()

  private val publisher = "hiring-publisher-" + UUID.randomUUID()
  private val worker = "hiring-interview-worker-" + UUID.randomUUID()
  private val orchestrator = "hiring-interview-orchestrator-" + UUID.randomUUID()

  private def suffixOf(id: String): String = id.takeRight(36)

  test("KFH-04.8 migration 036 rewrites legacy rows to generation keys and preserves state, kind and dates") {
    mongoResource.use { fixture =>
      val subject = UUID.randomUUID().toString
      val seeded = List(
        legacy(subject, publisher, "Operational", "Active"),
        legacy(subject, worker, "Interview", "Active"),
        legacy(subject, orchestrator, "Interview", "Fenced")
      )
      for {
        _ <- init(fixture.database)
        _ <- rewindTo035(fixture.database, seeded)
        _ <- init(fixture.database)
        migrated <- rows(fixture.database)
        _ <- init(fixture.database)
        again <- rows(fixture.database)
        ledger <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.HiringMigrationLedger,
          Filters.eq(MongoFields.Id, ledgerId)
        )
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        validator <- MongoHiringValidators.validatorMatches(
          setup,
          Registry,
          MongoProducerGenerationRegistrationMigrations.registrationValidator
        )
        collection <- MongoRepositoryTestSupport.collection(fixture.database, Registry)
        indexes <- collection.listIndexes[Document].map(_.toList)
        legacyWrite <- collection.insertOne(legacy(subject, worker + "x", "Interview", "Active")).attempt
      } yield {
        val byId = migrated.map(row => row.getString(MongoFields.Id) -> row).toMap
        assertEquals(
          byId.keySet,
          seeded
            .map(row =>
              s"$subject:${row.getString(MongoFields.TransactionalId)}:${suffixOf(row.getString(MongoFields.TransactionalId))}"
            )
            .toSet
        )
        migrated.foreach { row =>
          val id = row.getString(MongoFields.TransactionalId)
          assertEquals(row.getString(MongoFields.GenerationId), suffixOf(id))
        }
        val fenced = byId(s"$subject:$orchestrator:${suffixOf(orchestrator)}")
        assertEquals(fenced.getString(MongoFields.State), "Fenced")
        assertEquals(fenced.getDate(MongoFields.FencedAt).toInstant, fencedAt)
        assertEquals(fenced.getDate("expiresAt").toInstant, expiresAt)
        assertEquals(fenced.getDate("registeredAt").toInstant, registeredAt)
        val active = byId(s"$subject:$worker:${suffixOf(worker)}")
        assertEquals(active.getString(MongoFields.State), "Active")
        assertEquals(active.getString(MongoFields.Kind), "Interview")
        assert(!active.containsKey("expiresAt"))
        assertEquals(again.map(_.getString(MongoFields.Id)).toSet, byId.keySet)
        assertEquals(ledger.map(_.getString(MongoFields.State)), Some("Complete"))
        assert(validator)
        val index = indexes.find(_.getString("name") == MongoIndexNames.ProducerRegistrationGenerationState)
        assertEquals(
          index.map(_.get("key", classOf[Document]).keySet().toArray.toList.map(_.toString)),
          Some(List(MongoFields.TransactionalId, MongoFields.State, "registeredAt", MongoFields.Id))
        )
        assert(legacyWrite.left.exists {
          case error: MongoWriteException => error.getError.getCode == 121
          case _                          => false
        })
      }
    }
  }

  test("KFH-04.8 a restart between the copy and the delete converges on one row per generation") {
    mongoResource.use { fixture =>
      val subject = UUID.randomUUID().toString
      val partial = legacy(subject, worker, "Interview", "Active")
      val copied = legacy(subject, worker, "Interview", "Active")
        .append(MongoFields.Id, s"$subject:$worker:${suffixOf(worker)}")
        .append(MongoFields.GenerationId, suffixOf(worker))
      for {
        _ <- init(fixture.database)
        _ <- rewindTo035(fixture.database, List(partial))
        collection <- MongoRepositoryTestSupport.collection(fixture.database, Registry)
        _ <- collection.insertOne(
          copied,
          new com.mongodb.client.model.InsertOneOptions().bypassDocumentValidation(true)
        )
        _ <- init(fixture.database)
        migrated <- rows(fixture.database)
      } yield assertEquals(migrated.map(_.getString(MongoFields.Id)), List(s"$subject:$worker:${suffixOf(worker)}"))
    }
  }

  test("KFH-04.8 a legacy row written through a bypassed validator reopens the completed step") {
    mongoResource.use { fixture =>
      val subject = UUID.randomUUID().toString
      for {
        _ <- init(fixture.database)
        collection <- MongoRepositoryTestSupport.collection(fixture.database, Registry)
        _ <- collection.insertOne(
          legacy(subject, publisher, "Operational", "Active"),
          new com.mongodb.client.model.InsertOneOptions().bypassDocumentValidation(true)
        )
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        verdict <- MongoProducerGenerationRegistrationMigrations.step.whenComplete(setup)
        _ <- init(fixture.database)
        migrated <- rows(fixture.database)
        after <- MongoProducerGenerationRegistrationMigrations.step.whenComplete(setup)
      } yield {
        assertEquals(verdict, CompletedProof.Reopen)
        assertEquals(migrated.map(_.getString(MongoFields.Id)), List(s"$subject:$publisher:${suffixOf(publisher)}"))
        assertEquals(after, CompletedProof.Trusted)
      }
    }
  }

  test(
    "KFH-04.8 a completed step whose index is still the legacy key reopens and repairs it; other drift fails closed"
  ) {
    mongoResource.use { fixture =>
      def replaceIndex(keys: org.bson.conversions.Bson): IO[Unit] =
        for {
          collection <- MongoRepositoryTestSupport.collection(fixture.database, Registry)
          _ <- collection.dropIndex(MongoIndexNames.ProducerRegistrationGenerationState)
          _ <- collection.createIndex(
            keys,
            new IndexOptions().name(MongoIndexNames.ProducerRegistrationGenerationState)
          )
        } yield ()
      def keysOf(): IO[Option[List[String]]] =
        for {
          collection <- MongoRepositoryTestSupport.collection(fixture.database, Registry)
          indexes <- collection.listIndexes[Document].map(_.toList)
        } yield indexes
          .find(_.getString("name") == MongoIndexNames.ProducerRegistrationGenerationState)
          .map(_.get("key", classOf[Document]).keySet().toArray.toList.map(_.toString))
      for {
        _ <- init(fixture.database)
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        _ <- replaceIndex(Indexes.ascending(MongoFields.TransactionalId, MongoFields.State, MongoFields.Id))
        verdict <- MongoProducerGenerationRegistrationMigrations.step.whenComplete(setup)
        _ <- init(fixture.database)
        repaired <- keysOf()
        trusted <- MongoProducerGenerationRegistrationMigrations.step.whenComplete(setup)
        _ <- replaceIndex(Indexes.ascending(MongoFields.TransactionalId, MongoFields.Id))
        drift <- MongoProducerGenerationRegistrationMigrations.step.whenComplete(setup).attempt
      } yield {
        assertEquals(verdict, CompletedProof.Reopen)
        assertEquals(
          repaired,
          Some(List(MongoFields.TransactionalId, MongoFields.State, "registeredAt", MongoFields.Id))
        )
        assertEquals(trusted, CompletedProof.Trusted)
        assert(drift.left.exists(_.isInstanceOf[MigrationError.IndexMismatch]), clue(drift))
      }
    }
  }

  test("KFH-04.8 an identity that is not prefix plus UUID fails closed without rewriting") {
    mongoResource.use { fixture =>
      val subject = UUID.randomUUID().toString
      val stable = legacy(subject, "hiring-publisher-stable-slot", "Operational", "Active")
      for {
        _ <- init(fixture.database)
        _ <- rewindTo035(fixture.database, List(stable))
        failed <- init(fixture.database).attempt
        migrated <- rows(fixture.database)
      } yield {
        assert(failed.left.exists(_.isInstanceOf[MigrationError]), clue(failed))
        assertEquals(migrated.map(_.getString(MongoFields.Id)), List(s"$subject:hiring-publisher-stable-slot"))
      }
    }
  }
}
