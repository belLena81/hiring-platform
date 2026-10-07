package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.InterviewTopicPair
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.{ReadPreference, MongoWriteException}
import com.mongodb.client.model.{Filters, Updates}
import mongo4cats.database.MongoDatabase
import org.bson.Document
import java.time.Instant
import java.util.{Date, UUID}
import scala.concurrent.duration.*

final class MongoInterviewCleanupIntegrityIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val at = Instant.parse("2026-10-07T12:00:00Z")
  private def row(id: AnyRef = UUID.randomUUID().toString): Document = new Document("_id", id)
    .append("revision", Long.box(0L))
    .append("state", "Pending")
    .append("requestedAt", Date.from(at))
    .append("producerRegistry", true)
    .append("interviewTransactionalIds", java.util.List.of[String]())
  private def barrier(topic: String, partition: Int = 0, offset: Long = 4L): Document =
    new Document("topic", topic).append("partition", Int.box(partition)).append("endOffset", Long.box(offset))
  private def waiting(topics: InterviewTopicPair = InterviewTopicPair.Default): Document = row()
    .append("state", "AwaitingRetention")
    .append("barriers", java.util.List.of(barrier(topics.commands), barrier(topics.results)))
  private def setup(database: MongoDatabase[IO]): IO[MongoHiringSetup.SetupDatabase] =
    MongoHiringMigrations.ownedCollections.toList
      .traverse { name =>
        Mongo4catsCollections.documents(database, name).map(name -> _)
      }
      .map(values => MongoHiringSetup.SetupDatabase(database, values.toMap))
  private def initialize(database: MongoDatabase[IO], topics: InterviewTopicPair): IO[Unit] =
    MongoHiringSetup.initialize(database, Diagnostics.noop, topics)
  private def disableValidation(database: MongoDatabase[IO]): IO[Unit] =
    database
      .runCommand(
        new Document("collMod", MongoCollections.InterviewSubjectCleanup)
          .append("validationLevel", "off"),
        ReadPreference.primary()
      )
      .void

  test("strict native validator matches cleanup decoder for states and malformed physical proof") {
    mongoResource.use { fixture =>
      val valid = List(
        row(),
        row().append("state", "ProducersFenced"),
        row().append("state", "MongoPurged"),
        waiting(),
        row().append("state", "Complete").append("completedAt", Date.from(at)),
        row().append("barriers", "ignored outside AwaitingRetention").append("completedAt", "ignored")
      )
      val invalid = List(
        row(Int.box(3)),
        row(new Document("mixed", "identity")),
        row("1-1-1-1-1"),
        row().append("revision", Int.box(0)),
        row().append("revision", Long.box(-1L)),
        row().append("revision", Long.box(Long.MaxValue)),
        row().append("requestedAt", "wrong"),
        row().append("producerRegistry", false),
        row().append("state", "Unknown"),
        row().append("interviewTransactionalIds", java.util.List.of("hiring-publisher-wrong")),
        row().append("interviewTransactionalIds", java.util.List.of(Int.box(3))),
        row().append("state", "Complete"),
        waiting().append("barriers", "wrong"),
        waiting().append("barriers", java.util.List.of[String]()),
        waiting().append("barriers", java.util.List.of("wrong")),
        waiting()
          .append("barriers", java.util.List.of(barrier("foreign"), barrier(InterviewTopicPair.Default.results))),
        waiting().append("barriers", java.util.List.of(barrier(InterviewTopicPair.Default.commands))),
        waiting().append(
          "barriers",
          java.util.List.of(
            barrier(InterviewTopicPair.Default.commands),
            barrier(InterviewTopicPair.Default.results),
            barrier(InterviewTopicPair.Default.commands, 0, 99L)
          )
        ),
        waiting().append(
          "barriers",
          java.util.List
            .of(barrier(InterviewTopicPair.Default.commands, -1), barrier(InterviewTopicPair.Default.results))
        ),
        waiting().append(
          "barriers",
          java.util.List.of(
            barrier(InterviewTopicPair.Default.commands).append("partition", Long.box(0L)),
            barrier(InterviewTopicPair.Default.results)
          )
        ),
        waiting().append(
          "barriers",
          java.util.List.of(
            barrier(InterviewTopicPair.Default.commands).append("endOffset", Int.box(0)),
            barrier(InterviewTopicPair.Default.results)
          )
        )
      )
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        queue <- Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewSubjectCleanup)
        _ <- valid.traverse_(value =>
          IO(assert(MongoInterviewCleanupCodec.decodeCurrent(value).isRight)) *> queue.insertOne(value).void
        )
        _ <- invalid.traverse_ { value =>
          IO(assert(MongoInterviewCleanupCodec.decodeCurrent(value).isLeft)) *> queue.insertOne(value).attempt.flatMap {
            case Left(error: MongoWriteException) => IO(assertEquals(error.getError.getCode, 121))
            case other => IO(fail(s"Expected document validation failure, got ${other.map(_ => ())}"))
          }
        }
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
      } yield ()
    }
  }

  test("completed cleanup proof rejects validator drift and a different physical topic pair") {
    mongoResource.use { fixture =>
      val topics = InterviewTopicPair("hiring-test-isolated.commands", "hiring-test-isolated.results")
      for {
        _ <- initialize(fixture.database, topics)
        queue <- Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewSubjectCleanup)
        _ <- queue.insertOne(waiting(topics))
        _ <- initialize(fixture.database, topics)
        wrongTopics <- initialize(fixture.database, InterviewTopicPair.Default).attempt
        _ <- IO(assert(wrongTopics.isLeft))
        _ <- disableValidation(fixture.database)
        drift <- initialize(fixture.database, topics).attempt
        _ <- IO(assert(drift.isLeft))
      } yield ()
    }
  }

  test("cleanup integrity cutover resumes a 500-row audit without skipping mixed BSON identities") {
    mongoResource.use { fixture =>
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        ledger <- Mongo4catsCollections.documents(fixture.database, MongoCollections.HiringMigrationLedger)
        queue <- Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewSubjectCleanup)
        _ <- ledger.deleteMany(Filters.eq("_id", MongoInterviewCleanupIntegrityMigrations.MigrationId))
        _ <- (1L to 500L).toList.traverse_(n => queue.insertOne(row(new UUID(0L, n).toString)).void)
        _ <- disableValidation(fixture.database)
        corrupt = row(new Document("mixed", "identity"))
        _ <- queue.insertOne(corrupt)
        fixtureSetup <- setup(fixture.database)
        failed <- MongoInterviewCleanupIntegrityMigrations.initialize(fixtureSetup, InterviewTopicPair.Default).attempt
        checkpoint <- ledger.find(Filters.eq("_id", MongoInterviewCleanupIntegrityMigrations.MigrationId)).first
        _ <- IO {
          assert(failed.isLeft)
          assertEquals(checkpoint.flatMap(value => Option(value.get("lastId"))), Some(new UUID(0L, 500L).toString))
          assertEquals(checkpoint.flatMap(value => Option(value.get("state"))), Some("Running"))
        }
        _ <- queue.deleteMany(Filters.eq("_id", corrupt.get("_id")))
        _ <- MongoInterviewCleanupIntegrityMigrations.initialize(fixtureSetup, InterviewTopicPair.Default)
        complete <- MongoInterviewCleanupIntegrityMigrations.trusted(fixtureSetup, InterviewTopicPair.Default)
        _ <- IO(assert(complete))
        _ <- MongoWorkflowIntegrityAudit.audit(fixture.database)
      } yield ()
    }
  }

  test("concurrent cleanup integrity initialization preserves completed proof and strict validation") {
    mongoResource.use { fixture =>
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        ledger <- Mongo4catsCollections.documents(fixture.database, MongoCollections.HiringMigrationLedger)
        _ <- ledger.deleteMany(Filters.eq("_id", MongoInterviewCleanupIntegrityMigrations.MigrationId))
        fixtureSetup <- setup(fixture.database)
        _ <- List
          .fill(3)(MongoInterviewCleanupIntegrityMigrations.initialize(fixtureSetup, InterviewTopicPair.Default))
          .parSequence_
        _ <- MongoInterviewCleanupIntegrityMigrations
          .trusted(fixtureSetup, InterviewTopicPair.Default)
          .flatMap(value => IO(assert(value)))
        _ <- ledger.updateOne(
          Filters.eq("_id", MongoInterviewCleanupIntegrityMigrations.MigrationId),
          Updates.set("version", Int.box(1))
        )
        rejected <- MongoInterviewCleanupIntegrityMigrations.trusted(fixtureSetup, InterviewTopicPair.Default).attempt
        _ <- IO(assert(rejected.isLeft))
      } yield ()
    }
  }

  test("completed cleanup proof checks exact definitions and covered ledgers without rescanning cleanup") {
    mongoResource.use { fixture =>
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        queue <- Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewSubjectCleanup)
        _ <- queue.insertOne(waiting())
        _ <- fixture.commands.clear
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        reads <- fixture.commands.snapshot
        _ <- IO(
          assert(
            !reads.exists(value =>
              value.getFirstKey == "find" &&
                Option(value.get("find")).exists(item =>
                  item.isString && item.asString().getValue == MongoCollections.InterviewSubjectCleanup
                )
            )
          )
        )
        ledger <- Mongo4catsCollections.documents(fixture.database, MongoCollections.HiringMigrationLedger)
        _ <- ledger.deleteMany(Filters.eq("_id", MongoInterviewCleanupMigrations.MigrationId))
        rejected <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop).attempt
        _ <- IO(assert(rejected.isLeft))
      } yield ()
    }
  }

  test("explicit cleanup audit preserves the successful raw identity checkpoint across repair") {
    mongoResource.use { fixture =>
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        queue <- Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewSubjectCleanup)
        _ <- (1L to 500L).toList.traverse_(n => queue.insertOne(row(new UUID(0L, n).toString)).void)
        _ <- disableValidation(fixture.database)
        corrupt = row(new Document("mixed", "identity"))
        _ <- queue.insertOne(corrupt)
        failed <- MongoWorkflowIntegrityAudit.audit(fixture.database).attempt
        ledger <- Mongo4catsCollections.documents(fixture.database, MongoCollections.HiringMigrationLedger)
        checkpoint <- ledger.find(Filters.eq("_id", "audit_hiring_workflow_integrity")).first
        _ <- IO {
          assert(failed.isLeft)
          assertEquals(checkpoint.flatMap(value => Option(value.get("lastId"))), Some(new UUID(0L, 500L).toString))
          assertEquals(
            checkpoint.flatMap(value => Option(value.get("collection"))),
            Some(MongoCollections.InterviewSubjectCleanup)
          )
        }
        _ <- queue.deleteMany(Filters.eq("_id", corrupt.get("_id")))
        _ <- MongoWorkflowIntegrityAudit.audit(fixture.database)
        complete <- ledger.find(Filters.eq("_id", "audit_hiring_workflow_integrity")).first
        _ <- IO(assertEquals(complete.flatMap(value => Option(value.get("state"))), Some("Complete")))
      } yield ()
    }
  }
}
