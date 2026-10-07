package com.example.graphQL.cats.repository.mongo

import cats.syntax.all.*
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.Filters
import org.bson.Document
import java.time.Instant
import java.util.{Date, UUID}
import scala.concurrent.duration.*

final class MongoWorkflowIntegrityIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout = 5.minutes

  private def command(workflow: String): Document = {
    val now = Date.from(Instant.parse("2026-10-07T08:00:00Z"))
    new Document("_id", s"$workflow:repair")
      .append("workflowId", workflow)
      .append("stepId", "repair")
      .append("revision", Long.box(0L))
      .append("attempts", Int.box(0))
      .append("executionAttempts", Int.box(0))
      .append("commandState", "Pending")
      .append("availableAt", now)
      .append("occurredAt", now)
      .append("command", new Document("kind", "requireRepair").append("reason", "Manual repair"))
  }

  test("strict command validation rejects malformed claims and completed cutover fails closed on validator drift") {
    mongoResource.use { fixture =>
      val valid = command(UUID.randomUUID().toString)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.InterviewWorkflowCommands, valid)
        bad = command(UUID.randomUUID().toString).append("commandState", "Claimed")
        rejected <- MongoRepositoryTestSupport
          .insertOne(fixture.database, MongoCollections.InterviewWorkflowCommands, bad)
          .attempt
        _ = assert(rejected.isLeft)
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoAccessEvaluationSupport.command(
          fixture.database,
          new Document("collMod", MongoCollections.InterviewWorkflowCommands).append("validator", new Document())
        )
        restart <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop).attempt
      } yield assert(restart.isLeft)
    }
  }

  test("explicit audit can be rerun and records completion without changing valid commands") {
    mongoResource.use { fixture =>
      val valid = command(UUID.randomUUID().toString)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.InterviewWorkflowCommands, valid)
        _ <- MongoWorkflowIntegrityAudit.audit(fixture.database)
        _ <- MongoWorkflowIntegrityAudit.audit(fixture.database)
        checkpoint <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.HiringMigrationLedger,
          Filters.eq("_id", "audit_hiring_workflow_integrity")
        )
        stored <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.InterviewWorkflowCommands,
          Filters.eq("_id", valid.getString("_id"))
        )
      } yield {
        assertEquals(checkpoint.map(_.getString("state")), Some("Complete"))
        assertEquals(stored, Some(valid))
      }
    }
  }

  test("failed audit retains a bounded checkpoint and resumes after explicit repair") {
    mongoResource.use { fixture =>
      val rows = (1L to 501L).toList.map(n => command(new UUID(0L, n).toString))
      val invalid = command(new UUID(0L, 501L).toString).append("commandState", "Unknown")
      for {
        _ <- rows
          .take(500)
          .traverse_(row =>
            MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.InterviewWorkflowCommands, row)
          )
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.InterviewWorkflowCommands, invalid)
        failed <- MongoWorkflowIntegrityAudit.audit(fixture.database).attempt
        checkpoint <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.HiringMigrationLedger,
          Filters.eq("_id", "audit_hiring_workflow_integrity")
        )
        _ = assert(failed.isLeft)
        _ = assertEquals(checkpoint.map(_.getString("lastId")), rows.lift(499).map(_.getString("_id")))
        collection <- Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewWorkflowCommands)
        _ <- collection.updateOne(
          Filters.eq("_id", invalid.getString("_id")),
          new Document("$set", new Document("commandState", "Pending"))
        )
        _ <- MongoWorkflowIntegrityAudit.audit(fixture.database)
        finished <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.HiringMigrationLedger,
          Filters.eq("_id", "audit_hiring_workflow_integrity")
        )
      } yield assertEquals(finished.map(_.getString("state")), Some("Complete"))
    }
  }

  test("interrupted integrity cutover preserves command data and completes on restart") {
    mongoResource.use { fixture =>
      val valid = command(UUID.randomUUID().toString)
      for {
        _ <- MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.InterviewWorkflowCommands, valid)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.HiringMigrationLedger,
          new Document("_id", MongoWorkflowIntegrityMigrations.MigrationId)
            .append("version", Long.box(1L))
            .append("state", "Running")
        )
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        stored <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.InterviewWorkflowCommands,
          Filters.eq("_id", valid.getString("_id"))
        )
        marker <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.HiringMigrationLedger,
          Filters.eq("_id", MongoWorkflowIntegrityMigrations.MigrationId)
        )
      } yield {
        assertEquals(stored, Some(valid))
        assertEquals(marker.map(_.getString("state")), Some("Complete"))
      }
    }
  }

  test("migration proofs reject BSON Int versions instead of numeric equality with Long") {
    mongoResource.use { fixture =>
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        ledger <- Mongo4catsCollections.documents(fixture.database, MongoCollections.HiringMigrationLedger)
        _ <- List("012_attributable_producer_registrations", MongoWorkflowIntegrityMigrations.MigrationId).traverse_ {
          id =>
            for {
              _ <- ledger.updateOne(Filters.eq("_id", id), new Document("$set", new Document("version", Int.box(1))))
              rejected <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop).attempt
              _ = assert(rejected.isLeft, clues(id))
              _ <- ledger.updateOne(Filters.eq("_id", id), new Document("$set", new Document("version", Long.box(1L))))
            } yield ()
        }
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
      } yield ()
    }
  }
}
