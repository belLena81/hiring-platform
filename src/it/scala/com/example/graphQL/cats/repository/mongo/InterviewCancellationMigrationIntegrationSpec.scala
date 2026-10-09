package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.{Filters, IndexOptions, Indexes}
import mongo4cats.database.MongoDatabase
import org.bson.Document
import java.time.Instant
import java.util.{Date, UUID}
import scala.concurrent.duration.*

/** `018_interview_cancellation_reschedule`: validators, reservation identity and fail-closed verification. */
final class InterviewCancellationMigrationIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val ledgerId = MigrationIds.InterviewCancellationReschedule.value
  private val at = Date.from(Instant.parse("2026-10-09T08:00:00Z"))

  private def initialize(database: MongoDatabase[IO]): IO[Unit] =
    MongoHiringSetup.initialize(database, Diagnostics.noop)

  private def command(workflow: String, step: String, body: Document, availableAt: Date = at): Document =
    new Document("_id", s"$workflow:$step")
      .append("workflowId", workflow)
      .append("stepId", step)
      .append("revision", Long.box(1L))
      .append("attempts", Int.box(0))
      .append("executionAttempts", Int.box(0))
      .append("commandState", "Pending")
      .append("availableAt", availableAt)
      .append("occurredAt", at)
      .append("command", body)

  private def insertCommand(database: MongoDatabase[IO], value: Document) =
    MongoRepositoryTestSupport.insertOne(database, MongoCollections.InterviewWorkflowCommands, value).attempt

  private def interval = new Document("startsAt", at).append("endsAt", Date.from(at.toInstant.plusSeconds(3600)))

  private def reservation(workflow: String, key: String, release: String): Document =
    new Document("_id", s"$workflow/$key")
      .append("workflowId", workflow)
      .append("reserveKey", key)
      .append("releaseKey", release)

  private def deleteLedgerRow(database: MongoDatabase[IO]): IO[Unit] =
    MongoRepositoryTestSupport
      .collection(database, MongoCollections.HiringMigrationLedger)
      .flatMap(_.deleteOne(Filters.eq(MongoFields.Id, ledgerId)))
      .void

  test("the extended validator accepts well-formed intents and rejects intents bound to another workflow or shape") {
    mongoResource.use { fixture =>
      val wf = UUID.randomUUID().toString
      val other = UUID.randomUUID().toString
      def kind(name: String) = new Document("kind", name)
      for {
        _ <- initialize(fixture.database)
        cancel <- insertCommand(
          fixture.database,
          command(wf, "s1", kind("cancelCalendar").append("idempotencyKey", s"$wf:cancel:g0"))
        )
        hold <- insertCommand(
          fixture.database,
          command(
            wf,
            "s2",
            kind("holdReplacement").append("idempotencyKey", s"$wf:reserve:g1").append("interval", interval)
          )
        )
        notify <- insertCommand(
          fixture.database,
          command(
            wf,
            "s3",
            kind("notifyKind")
              .append("notificationKind", "RescheduleProposed")
              .append("participant", "Candidate")
              .append("idempotencyKey", s"$wf:rescheduleProposed:r4:notify:Candidate")
          )
        )
        expiry <- insertCommand(
          fixture.database,
          command(wf, "s4", kind("expireProposal").append("availableAt", at))
        )
        foreign <- insertCommand(
          fixture.database,
          command(wf, "s5", kind("cancelCalendar").append("idempotencyKey", s"$other:cancel:g0"))
        )
        originalGeneration <- insertCommand(
          fixture.database,
          command(
            wf,
            "s6",
            kind("holdReplacement").append("idempotencyKey", s"$wf:reserve").append("interval", interval)
          )
        )
        wrongKind <- insertCommand(
          fixture.database,
          command(
            wf,
            "s7",
            kind("notifyKind")
              .append("notificationKind", "Cancelled")
              .append("participant", "Candidate")
              .append("idempotencyKey", s"$wf:rescheduled:g1:notify:Candidate")
          )
        )
        wrongDue <- insertCommand(
          fixture.database,
          command(wf, "s8", kind("expireProposal").append("availableAt", at), Date.from(at.toInstant.plusSeconds(5)))
        )
        unknown <- insertCommand(fixture.database, command(wf, "s9", kind("cancelEverything")))
        scheduling <- insertCommand(
          fixture.database,
          command(wf, "s10", kind("reserveCalendar").append("idempotencyKey", s"$wf:reserve"))
        )
      } yield {
        List(cancel, hold, notify, expiry, scheduling).foreach(result => assert(result.isRight, clue(result)))
        List(foreign, originalGeneration, wrongKind, wrongDue, unknown).foreach(result =>
          assert(result.isLeft, clue(result))
        )
      }
    }
  }

  test("reservation identity is unique per workflow and reserve key while a workflow may hold several generations") {
    mongoResource.use { fixture =>
      val wf = UUID.randomUUID().toString
      def insert(value: Document) =
        MongoRepositoryTestSupport
          .insertOne(fixture.database, MongoCollections.InterviewCalendarReservations, value)
          .attempt
      for {
        _ <- initialize(fixture.database)
        first <- insert(reservation(wf, s"$wf:reserve", s"$wf:release"))
        second <- insert(reservation(wf, s"$wf:reserve:g1", s"$wf:release:g1"))
        duplicate <- insert(reservation(wf, s"$wf:reserve", s"$wf:release:other").append("_id", "another"))
      } yield {
        assert(first.isRight && second.isRight)
        assert(duplicate.isLeft)
      }
    }
  }

  test("an unknown reservation index fails closed on restart and an unreadable stored workflow blocks the cutover") {
    mongoResource.use { fixture =>
      for {
        _ <- initialize(fixture.database)
        collection <- MongoRepositoryTestSupport.collection(
          fixture.database,
          MongoCollections.InterviewCalendarReservations
        )
        _ <- collection.createIndex(
          Indexes.ascending("workflowId"),
          new IndexOptions().name("workflow_only").unique(true)
        )
        restart <- initialize(fixture.database).attempt
        _ <- collection.dropIndex("workflow_only")
        _ <- deleteLedgerRow(fixture.database)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewWorkflows,
          new Document("_id", UUID.randomUUID().toString)
            .append("documentType", "workflow")
            .append("phase", "ProposalPending")
        )
        blocked <- initialize(fixture.database).attempt
      } yield {
        assertEquals(restart.left.toOption.map(_.getClass.getSimpleName), Some("IndexMismatch"))
        assertEquals(blocked.left.toOption.map(_.getClass.getSimpleName), Some("StepFailed"))
      }
    }
  }

  test("upgrading from the previous validator keeps stored commands and installs the exact active definition") {
    mongoResource.use { fixture =>
      val wf = UUID.randomUUID().toString
      val existing = command(wf, "s1", new Document("kind", "reserveCalendar").append("idempotencyKey", s"$wf:reserve"))
      for {
        _ <- initialize(fixture.database)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewWorkflowCommands,
          existing
        )
        _ <- MongoAccessEvaluationSupport.command(
          fixture.database,
          MongoHiringValidators.strictValidation(
            MongoCollections.InterviewWorkflowCommands,
            MongoWorkflowIntegrityMigrations.baselineCommandValidator
          )
        )
        collection <- MongoRepositoryTestSupport.collection(
          fixture.database,
          MongoCollections.InterviewCalendarReservations
        )
        _ <- collection.dropIndex(MongoIndexNames.InterviewCalendarReservationIdentity)
        _ <- deleteLedgerRow(fixture.database)
        _ <- initialize(fixture.database)
        _ <- initialize(fixture.database)
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        _ <- MongoHiringValidators.assertStrictValidators(
          setup,
          List(MongoCollections.InterviewWorkflowCommands -> MongoWorkflowIntegrityMigrations.commandValidator)
        )
        stored <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.InterviewWorkflowCommands,
          Filters.eq("_id", existing.getString("_id"))
        )
        indexes <- collection.listIndexes[Document].map(_.toList.map(_.getString("name")))
      } yield {
        assertEquals(stored, Some(existing))
        assert(indexes.contains(MongoIndexNames.InterviewCalendarReservationIdentity), clue(indexes))
      }
    }
  }

  test("concurrent starts complete and drift of the active validator fails closed afterwards") {
    mongoResource.use { fixture =>
      for {
        results <- List.fill(4)(initialize(fixture.database).attempt).parSequence
        _ <- MongoAccessEvaluationSupport.command(
          fixture.database,
          new Document("collMod", MongoCollections.InterviewWorkflowCommands).append("validator", new Document())
        )
        drifted <- initialize(fixture.database).attempt
      } yield {
        assert(results.forall(_.isRight), clue(results))
        assert(drifted.isLeft)
      }
    }
  }
}
