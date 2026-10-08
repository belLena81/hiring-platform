package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.MongoWriteException
import com.mongodb.client.model.{Filters, Updates}
import org.bson.Document
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*

final class MongoCandidateResidenceIntegrityIntegrationSpec extends MongoIntegrationSuite {
  private val legacyMigration = "002_candidate_search_profile_verification"
  private val migration = Filters.eq("_id", MongoCandidateResidenceIntegrityMigrations.MigrationId)

  private def setup(fixture: MongoAccessEvaluationSupport.Fixture): IO[MongoHiringSetup.SetupDatabase] =
    MongoHiringMigrations.ownedCollections.toList
      .traverse(name => MongoRepositoryTestSupport.collection(fixture.database, name).map(name -> _))
      .map(values => MongoHiringSetup.SetupDatabase(fixture.database, values.toMap))

  private def located(index: Int, value: Document): Document = {
    val row = candidate(index)
    val _ = row.get("profile", classOf[Document]).append("currentResidence", value)
    row
  }

  private def candidate(index: Int): Document = MongoHiringCodecs.user(
    User(
      UserId(new UUID(0L, index.toLong + 1L)),
      None,
      s"Candidate $index",
      UserRole.Candidate,
      Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
      Instant.parse("2026-10-07T12:00:00Z")
    )
  )

  private def residence(city: Boolean = true): Document = {
    val result = new Document("country", "Cyprus").append("countryCanonical", "cyprus")
    if (city) {
      val _ = result.append("city", "Nicosia").append("cityCanonical", "nicosia")
    }
    result
  }

  test("startup rejects missing canonical city even when candidate profile migration already completed") {
    mongoResource.use { fixture =>
      val row = candidate(0)
      val broken = residence()
      val _ = broken.remove("cityCanonical")
      val _ = row.get("profile", classOf[Document]).append("currentResidence", broken)
      for {
        users <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Users)
        ledger <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.HiringMigrationLedger)
        _ <- users.insertOne(row)
        _ <- ledger.insertOne(
          new Document("_id", legacyMigration).append("version", Long.box(1L)).append("state", "Complete")
        )
        outcome <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop).attempt
        after <- users.find(Filters.eq("_id", row.getString("_id"))).first
      } yield {
        assert(outcome.isLeft)
        assertEquals(after, Some(row))
      }
    }
  }

  test("absent and paired residences audit without changing data or revisions; Complete startup skips user scans") {
    mongoResource.use { fixture =>
      val rows = List(candidate(0), located(1, residence(city = false)), located(2, residence()))
      for {
        database <- setup(fixture)
        users = database.getCollection(MongoCollections.Users)
        _ <- users.insertMany(rows)
        _ <- MongoHiringValidators.createUserValidator(fixture.database)
        _ <- MongoCandidateResidenceIntegrityMigrations.initialize(database)
        stored <- users.find.sort(com.mongodb.client.model.Sorts.ascending("_id")).all
        proof <- database.getCollection(MongoCollections.HiringMigrationLedger).find(migration).first
        _ <- fixture.commands.clear
        _ <- MongoCandidateResidenceIntegrityMigrations.verifyCompleted(database)
        _ <- MongoCandidateResidenceIntegrityMigrations.initialize(database)
        commands <- fixture.commands.snapshot
      } yield {
        assertEquals(stored.toList, rows)
        assertEquals(proof.map(_.get("version")), Some(Long.box(1L)))
        assertEquals(proof.map(_.getString("state")), Some("Complete"))
        assert(proof.forall(!_.containsKey("lastId")))
        assert(
          !commands.exists(command =>
            Option(command.get("find")).exists(_.asString().getValue == MongoCollections.Users)
          )
        )
      }
    }
  }

  test("strict paired validator rejects new unpaired residences and retains valid absent and paired cities") {
    mongoResource.use { fixture =>
      val missing = residence()
      val _ = missing.remove("cityCanonical")
      val orphan = residence(city = false).append("cityCanonical", "nicosia")
      for {
        users <- MongoRepositoryTestSupport.collection(fixture.database, MongoCollections.Users)
        _ <- MongoHiringValidators.createUserValidator(fixture.database)
        _ <- users.insertMany(List(located(0, residence(city = false)), located(1, residence())))
        rejected <- List(located(2, missing), located(3, orphan)).traverse(row => users.insertOne(row).attempt)
        count <- users.count(Filters.empty())
      } yield {
        assertEquals(count, 2L)
        assert(rejected.forall(_.left.toOption.exists {
          case error: MongoWriteException => error.getError.getCode == 121
          case _                          => false
        }))
      }
    }
  }

  test("bounded residence audit retains its checkpoint on failure and resumes after explicit repair") {
    mongoResource.use { fixture =>
      val rows = (0 until 501).toList.map(index =>
        located(index, if (index == 500) residence().append("cityCanonical", "limassol") else residence())
      )
      for {
        database <- setup(fixture)
        users = database.getCollection(MongoCollections.Users)
        ledger = database.getCollection(MongoCollections.HiringMigrationLedger)
        _ <- users.insertMany(rows)
        _ <- MongoHiringValidators.createUserValidator(fixture.database)
        _ <- fixture.commands.clear
        failed <- MongoCandidateResidenceIntegrityMigrations.initialize(database).attempt
        running <- ledger.find(migration).first
        commands <- fixture.commands.snapshot
        unchanged <- users.find.sort(com.mongodb.client.model.Sorts.ascending("_id")).all
        _ <- users.updateOne(
          Filters.eq("_id", rows.last.getString("_id")),
          Updates.set("profile.currentResidence", residence())
        )
        _ <- MongoCandidateResidenceIntegrityMigrations.initialize(database)
        proof <- ledger.find(migration).first
        stored <- users.find.sort(com.mongodb.client.model.Sorts.ascending("_id")).all
      } yield {
        assert(failed.isLeft)
        assertEquals(running.map(_.getString("state")), Some("Running"))
        assertEquals(running.map(_.getString("lastId")), Some(rows(499).getString("_id")))
        assertEquals(unchanged.toList, rows)
        assertEquals(stored.toList.take(500), rows.take(500))
        assert(stored.forall(_.getLong("version") == 0L))
        assertEquals(proof.map(_.getString("state")), Some("Complete"))
        val reads = commands.filter(command =>
          Option(command.get("find")).exists(_.asString().getValue == MongoCollections.Users)
        )
        assert(reads.size >= 2)
        assert(reads.forall(_.getNumber("limit").intValue() <= 500))
        assert(
          reads.forall(_.getDocument("projection").keySet().asScala.toSet == Set("_id", "profile.currentResidence"))
        )
      }
    }
  }

  test("concurrent audits complete one proof and preserve candidate rows") {
    mongoResource.use { fixture =>
      val rows = (0 until 503).toList.map(index => located(index, residence()))
      for {
        database <- setup(fixture)
        users = database.getCollection(MongoCollections.Users)
        _ <- users.insertMany(rows)
        _ <- MongoHiringValidators.createUserValidator(fixture.database)
        _ <- (
          MongoCandidateResidenceIntegrityMigrations.initialize(database),
          MongoCandidateResidenceIntegrityMigrations.initialize(database)
        ).parTupled
        proof <- database.getCollection(MongoCollections.HiringMigrationLedger).find(migration).first
        stored <- users.find.sort(com.mongodb.client.model.Sorts.ascending("_id")).all
      } yield {
        assertEquals(proof.map(_.getString("state")), Some("Complete"))
        assert(proof.forall(!_.containsKey("lastId")))
        assertEquals(stored.toList, rows)
      }
    }
  }

  test("unsupported ledger versions, states and checkpoints fail closed without auditing or changing users") {
    mongoResource.use { fixture =>
      val row = located(0, residence())
      val invalid = List(
        new Document("version", Int.box(1)).append("state", "Running"),
        new Document("version", Long.box(2L)).append("state", "Running"),
        new Document("version", Long.box(1L)).append("state", "Unknown"),
        new Document("version", Long.box(1L)).append("state", "Running").append("lastId", Int.box(2)),
        new Document("version", Long.box(1L)).append("state", "Complete").append("lastId", row.getString("_id"))
      )
      for {
        database <- setup(fixture)
        users = database.getCollection(MongoCollections.Users)
        ledger = database.getCollection(MongoCollections.HiringMigrationLedger)
        _ <- users.insertOne(row)
        _ <- MongoHiringValidators.createUserValidator(fixture.database)
        _ <- invalid.traverse_ { entry =>
          ledger.deleteOne(migration) *> ledger.insertOne(
            entry.append("_id", MongoCandidateResidenceIntegrityMigrations.MigrationId)
          ) *> MongoCandidateResidenceIntegrityMigrations
            .initialize(database)
            .attempt
            .flatMap(result => IO(assert(result.isLeft)))
        }
        stored <- users.find(Filters.eq("_id", row.getString("_id"))).first
      } yield assertEquals(stored, Some(row))
    }
  }

  test("completed proof rejects validator drift before startup can replace it") {
    mongoResource.use { fixture =>
      for {
        database <- setup(fixture)
        _ <- MongoHiringValidators.createUserValidator(fixture.database)
        _ <- MongoCandidateResidenceIntegrityMigrations.initialize(database)
        _ <- fixture.database.runCommand(
          new Document("collMod", MongoCollections.Users).append("validator", new Document())
        )
        failed <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop).attempt
        stillDrifted <- MongoHiringValidators.userValidatorMatches(fixture.database)
        proof <- database.getCollection(MongoCollections.HiringMigrationLedger).find(migration).first
      } yield {
        assert(failed.isLeft)
        assert(!stillDrifted)
        assertEquals(proof.map(_.getString("state")), Some("Complete"))
      }
    }
  }

  private val malformedProfiles: List[(String, Document => Unit)] = List(
    "residence" -> ((profile: Document) => {
      val _ =
        profile.append("currentResidence", new Document("country", "Cyprus").append("countryCanonical", "invalid"))
    }),
    "availability" -> ((profile: Document) => { val _ = profile.append("availabilityStatus", "INVALID") }),
    "consent" -> ((profile: Document) => { val _ = profile.append("recruiterSearchOptIn", "true") })
  )

  malformedProfiles.foreach { case (name, corrupt) =>
    test(s"existing profile migration retains an interrupted checkpoint for malformed $name and resumes after repair") {
      mongoResource.use { fixture =>
        val rows = (0 until 501).toList.map(candidate)
        val _ = rows.head.get("profile", classOf[Document]).remove("recruiterSearchOptIn")
        corrupt(rows.last.get("profile", classOf[Document]))
        val legacyMarker = Filters.eq("_id", legacyMigration)
        for {
          database <- setup(fixture)
          users = database.getCollection(MongoCollections.Users)
          ledger = database.getCollection(MongoCollections.HiringMigrationLedger)
          _ <- users.insertMany(rows)
          failed <- MongoHiringMigrations
            .initialize(database, resetOnStart = false, diagnostics = Diagnostics.noop)
            .attempt
          running <- ledger.find(legacyMarker).first
          beforeRepair <- users.find.sort(com.mongodb.client.model.Sorts.ascending("_id")).all
          _ <- users.replaceOne(Filters.eq("_id", rows.last.getString("_id")), candidate(500))
          _ <- MongoHiringMigrations.initialize(database, resetOnStart = false, diagnostics = Diagnostics.noop)
          proof <- ledger.find(legacyMarker).first
          stored <- users.find.sort(com.mongodb.client.model.Sorts.ascending("_id")).all
          legacy <- users.find(Filters.eq("_id", rows.head.getString("_id"))).first
        } yield {
          assert(failed.isLeft)
          assertEquals(running.map(_.getString("state")), Some("Running"))
          assertEquals(running.map(_.getString("lastId")), Some(rows(499).getString("_id")))
          assertEquals(beforeRepair.toList, rows)
          assertEquals(proof.map(_.getString("state")), Some("Complete"))
          assert(proof.forall(!_.containsKey("lastId")))
          assert(stored.forall(_.getLong("version") == 0L))
          assertEquals(stored.toList.take(500), rows.take(500))
          assertEquals(
            legacy
              .flatMap(document => MongoHiringCodecs.readUser(document).toOption)
              .flatMap(_.candidateProfile)
              .map(_.recruiterSearchOptIn),
            Some(false)
          )
        }
      }
    }
  }
}
