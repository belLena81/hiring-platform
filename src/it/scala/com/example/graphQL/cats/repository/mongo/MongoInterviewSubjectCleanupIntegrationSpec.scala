package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.application.InterviewSubjectCleanupWorker
import com.example.graphQL.cats.service.port.{
  InterviewPublisherFencer,
  InterviewRetentionBarrier,
  RepositoryIO,
  InterviewCleanupUpdate
}
import com.example.graphQL.cats.domain.workflow.{
  InterviewCleanupObservation,
  InterviewCleanupState,
  InterviewSubjectCleanup
}
import org.bson.Document
import com.mongodb.client.model.Filters
import java.time.Instant
import java.util.{Date, UUID}
import munit.CatsEffectSuite
import scala.concurrent.duration.*

final class MongoInterviewSubjectCleanupIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  private val barriers = Vector(
    InterviewRetentionBarrier("hiring.interview-commands", 0, 4L),
    InterviewRetentionBarrier("hiring.interview-results", 0, 5L)
  )
  private val fencer = new InterviewPublisherFencer {
    override def fence(ids: Vector[String]): RepositoryIO[Unit] =
      RepositoryIO.fromEither(
        InterviewSubjectCleanup
          .validateProducerIds(ids)
          .left
          .map(_ => com.example.graphQL.cats.service.RepositoryError.InvalidStoredData)
          .map(_ => ())
      )
  }
  private def worker(cleanup: MongoInterviewSubjectCleanup, passed: Boolean): InterviewSubjectCleanupWorker =
    new InterviewSubjectCleanupWorker(cleanup, fencer, IO.pure(barriers), _ => IO.pure(passed), Diagnostics.noop)
  private def step(cleanup: MongoInterviewSubjectCleanup, passed: Boolean = false): IO[Unit] =
    worker(cleanup, passed).runOnce.value.flatMap(result => IO(assert(result.isRight)))
  test("workflow deletion requires confirmed fencing and physical retention, purges both participants' effects") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val subject = UserId(UUID.randomUUID())
      val other = UUID.randomUUID().toString
      val workflowId = UUID.randomUUID().toString
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      val at = Instant.now()
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewWorkflows,
          new Document("_id", workflowId)
            .append("workflowId", workflowId)
            .append("candidateId", subject.value.toString)
            .append("recruiterId", other)
        )
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.FakeInterviewCalendarReservations,
          new Document("_id", workflowId)
            .append("workflowId", workflowId)
            .append("releaseKey", workflowId + ":release")
            .append("participants", java.util.List.of(subject.value.toString, other))
            .append("startsAt", Date.from(at.plusSeconds(10)))
            .append("endsAt", Date.from(at.plusSeconds(20)))
        )
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.FakeInterviewNotificationReceipts,
          new Document("_id", workflowId + ":notify:Recruiter")
            .append("workflowId", workflowId)
            .append("recipientId", other)
        )
        _ <- cleanup.enqueue(subject, at.minusSeconds(3600), None).value.flatMap(result => IO(assert(result.isRight)))
        initial <- cleanup.complete(subject)
        _ <- step(cleanup)
        beforePurge <- cleanup.find(subject).value
        _ = assertEquals(beforePurge.toOption.flatten.map(_.state), Some(InterviewCleanupState.ProducersFenced))
        _ <- step(cleanup)
        purged <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.FakeInterviewCalendarReservations)
        notifications <- MongoRepositoryTestSupport.count(
          fixture.database,
          MongoCollections.FakeInterviewNotificationReceipts
        )
        _ <- step(cleanup)
        pending <- cleanup.complete(subject)
        _ <- new InterviewSubjectCleanupWorker(
          cleanup,
          fencer,
          IO.raiseError(new AssertionError("barrier must be reused")),
          _ => IO.pure(false),
          Diagnostics.noop
        ).runOnce.value
        pendingAfterRestart <- new MongoInterviewSubjectCleanup(fixture.database).complete(subject)
        _ <- new InterviewSubjectCleanupWorker(
          cleanup,
          fencer,
          IO.raiseError(new AssertionError("barrier must be reused")),
          _ => IO.pure(true),
          Diagnostics.noop
        ).runOnce.value
        complete <- cleanup.complete(subject)
        row <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.InterviewSubjectCleanup,
          Filters.eq("_id", subject.value.toString)
        )
      } yield {
        assert(!initial)
        assertEquals(purged, 0L)
        assertEquals(notifications, 0L)
        assert(!pending)
        assert(!pendingAfterRestart)
        assert(complete)
        assertEquals(row.map(_.getString("state")), Some("Complete"))
      }
    }
  }

  test("a future reservation surviving completed workflow expiry still requires deletion cleanup") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val subject = UserId(UUID.randomUUID())
      val id = UUID.randomUUID().toString
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      val at = Instant.now()
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.FakeInterviewCalendarReservations,
          new Document("_id", id)
            .append("workflowId", id)
            .append("releaseKey", id + ":release")
            .append("participants", java.util.List.of(subject.value.toString, UUID.randomUUID().toString))
            .append("startsAt", Date.from(at.plusSeconds(1000000)))
            .append("endsAt", Date.from(at.plusSeconds(1003600)))
        )
        _ <- cleanup.enqueue(subject, at.minusSeconds(3600), None).value.flatMap(result => IO(assert(result.isRight)))
        pending <- cleanup.complete(subject)
        _ <- step(cleanup)
        _ <- step(cleanup)
        reservations <- MongoRepositoryTestSupport.count(
          fixture.database,
          MongoCollections.FakeInterviewCalendarReservations
        )
      } yield {
        assert(!pending)
        assertEquals(reservations, 0L)
      }
    }
  }

  test("concurrent migration replaces the known unfiltered inbox index and permits distinct quarantine identities") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val inbox = Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewWorkflowInbox)
      for {
        _ <- inbox.flatMap(
          _.createIndex(
            com.mongodb.client.model.Indexes.ascending("workflowId", "messageId"),
            new com.mongodb.client.model.IndexOptions()
              .name(MongoHiringSetup.InterviewWorkflowInboxIdentityIndex)
              .unique(true)
          )
        )
        _ <- (
          MongoHiringSetup.initialize(fixture.database, Diagnostics.noop),
          MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        ).parTupled
        repository = MongoInterviewWorkflowRepository.live(fixture.database, fixture.client, Diagnostics.noop)
        _ <- repository
          .quarantine("first_invalid_record", Instant.now())
          .value
          .flatMap(result => IO(assert(result.isRight)))
        _ <- repository
          .quarantine("second_invalid_record", Instant.now())
          .value
          .flatMap(result => IO(assert(result.isRight)))
        indexes <- inbox.flatMap(_.listIndexes[Document])
      } yield {
        val identity = indexes.find(_.getString("name") == MongoHiringSetup.InterviewWorkflowInboxIdentityIndex)
        assert(
          identity.exists(
            _.get("partialFilterExpression", classOf[Document]).getString("documentType") == "inboxReceipt"
          )
        )
      }
    }
  }

  test("accounts with no attributable workflows do not acquire an unnecessary cleanup barrier") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val subject = UserId(UUID.randomUUID())
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- cleanup.enqueue(subject, Instant.now(), None).value.flatMap(result => IO(assert(result.isRight)))
        complete <- cleanup.complete(subject)
      } yield assert(complete)
    }
  }

  test("publisher generations alone create cleanup and preserve an immutable deletion snapshot") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val subject = UserId(UUID.randomUUID())
      val generation = "hiring-interview-worker-" + UUID.randomUUID().toString
      val unrelated = "hiring-publisher-" + UUID.randomUUID().toString
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.OutboxSubjectFences,
          new Document("_id", subject.value.toString)
            .append("deleted", true)
            .append("interviewTransactionalIds", java.util.List.of(generation))
            .append("transactionalIds", java.util.List.of(unrelated))
        )
        _ <- cleanup.enqueue(subject, Instant.now(), None).value.flatMap(result => IO(assert(result.isRight)))
        current <- cleanup.find(subject).value
        complete <- cleanup.complete(subject)
        fence <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.OutboxSubjectFences,
          Filters.eq("_id", subject.value.toString)
        )
      } yield {
        assertEquals(current.toOption.flatten.map(_.transactionalIds), Some(Vector(generation)))
        assertEquals(current.toOption.flatten.map(_.state), Some(InterviewCleanupState.Pending))
        assert(!complete)
        assertEquals(fence.map(_.getList("transactionalIds", classOf[String])), Some(java.util.List.of(unrelated)))
      }
    }
  }

  test("competing cleaners cannot overwrite a newer durable phase with a stale observation") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val subject = UserId(UUID.randomUUID())
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      val at = Instant.now()
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.FakeInterviewCalendarParticipantLocks,
          new Document("_id", subject.value.toString).append("fence", Long.box(1L))
        )
        _ <- cleanup.enqueue(subject, at, None).value.flatMap(result => IO(assert(result.isRight)))
        initial <- cleanup
          .find(subject)
          .value
          .flatMap(result =>
            IO.fromEither(result.left.map(error => new AssertionError(error.toString)))
              .flatMap(value => IO.fromOption(value)(new AssertionError("cleanup missing")))
          )
        fenced <- IO.fromEither(
          InterviewSubjectCleanup
            .decide(initial, InterviewCleanupObservation.ProducersFenced, at)
            .left
            .map(error => new AssertionError(error.toString))
        )
        raced <- (cleanup.transition(initial, fenced).value, cleanup.transition(initial, fenced).value).parTupled
        purged <- IO.fromEither(
          InterviewSubjectCleanup
            .decide(fenced, InterviewCleanupObservation.MongoPurged, at)
            .left
            .map(error => new AssertionError(error.toString))
        )
        applied <- cleanup.transition(fenced, purged).value
        stale <- cleanup.transition(initial, fenced).value
        stored <- cleanup.find(subject).value
      } yield {
        assertEquals(List(raced._1, raced._2).count(_ == Right(InterviewCleanupUpdate.Applied)), 1)
        assertEquals(List(raced._1, raced._2).count(_ == Right(InterviewCleanupUpdate.StaleRevision)), 1)
        assertEquals(applied, Right(InterviewCleanupUpdate.Applied))
        assertEquals(stale, Right(InterviewCleanupUpdate.StaleRevision))
        assertEquals(stored.toOption.flatten.map(_.state), Some(InterviewCleanupState.MongoPurged))
        assertEquals(stored.toOption.flatten.map(_.revision), Some(2L))
      }
    }
  }

  test("malformed completion evidence fails closed instead of accepting a raw Complete string") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val subject = UserId(UUID.randomUUID())
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewSubjectCleanup,
          new Document("_id", subject.value.toString).append("state", "Complete")
        )
        complete <- cleanup.complete(subject)
      } yield assert(!complete)
    }
  }

  test("request receipts remain attributable after workflow TTL and purge their linked orphan commands") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val subject = UserId(UUID.randomUUID())
      val workflowId = UUID.randomUUID().toString
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewWorkflows,
          new Document("_id", s"request:${subject.value}:${UUID.randomUUID()}")
            .append("documentType", "requestReceipt")
            .append("requestWorkflowId", workflowId)
        )
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewWorkflowCommands,
          new Document("_id", s"$workflowId:command").append("workflowId", workflowId)
        )
        _ <- cleanup.enqueue(subject, Instant.now(), None).value.flatMap(result => IO(assert(result.isRight)))
        absentBefore <- cleanup.absent(subject).value
        _ <- step(cleanup)
        _ <- step(cleanup)
        absentAfter <- cleanup.absent(subject).value
        receipts <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.InterviewWorkflows)
        commands <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.InterviewWorkflowCommands)
      } yield {
        assertEquals(absentBefore, Right(false))
        assertEquals(absentAfter, Right(true))
        assertEquals(receipts, 0L)
        assertEquals(commands, 0L)
      }
    }
  }

  test("legacy cleanup completion is reopened with a fresh snapshot and migration is restartable") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val subject = UserId(UUID.randomUUID())
      val generation = "hiring-interview-orchestrator-" + UUID.randomUUID().toString
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        ledger <- Mongo4catsCollections.documents(fixture.database, MongoCollections.HiringMigrationLedger)
        _ <- ledger.deleteMany(Filters.eq("_id", MongoInterviewCleanupMigrations.MigrationId))
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.OutboxSubjectFences,
          new Document("_id", subject.value.toString)
            .append("deleted", true)
            .append("interviewTransactionalIds", java.util.List.of(generation))
        )
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewSubjectCleanup,
          new Document("_id", subject.value.toString)
            .append("state", "Complete")
            .append("requestedAt", Date.from(Instant.now()))
            .append("completedAt", Date.from(Instant.now()))
        )
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        reopened <- cleanup.find(subject).value
        _ <- step(cleanup)
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        resumed <- cleanup.find(subject).value
      } yield {
        assertEquals(reopened.toOption.flatten.map(_.state), Some(InterviewCleanupState.Pending))
        assertEquals(reopened.toOption.flatten.map(_.transactionalIds), Some(Vector(generation)))
        assertEquals(resumed.toOption.flatten.map(_.state), Some(InterviewCleanupState.ProducersFenced))
        assertEquals(resumed.toOption.flatten.map(_.revision), Some(1L))
      }
    }
  }

  test("startup validates every current cleanup row even after migration completion") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val at = Date.from(Instant.now())
      val invalidRows = Vector(
        new Document("state", "Unknown").append("revision", Long.box(0L)),
        new Document("state", "Pending").append("revision", Long.box(-1L)),
        new Document("state", "Pending")
          .append("revision", Long.box(0L))
          .append("interviewTransactionalIds", java.util.List.of("hiring-publisher-invalid")),
        new Document("state", "AwaitingRetention")
          .append("revision", Long.box(3L))
          .append(
            "barriers",
            java.util.List.of(
              new Document("topic", "hiring.interview-commands")
                .append("partition", Int.box(0))
                .append("endOffset", Long.box(4L))
            )
          ),
        new Document("state", "Complete").append("revision", Long.box(4L)),
        new Document("_id", Int.box(1)).append("state", "Pending").append("revision", Long.box(0L))
      )
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        queue <- Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewSubjectCleanup)
        _ <- (1 to 129).toList.traverse_ { _ =>
          queue
            .insertOne(
              new Document("_id", UUID.randomUUID().toString)
                .append("requestedAt", at)
                .append("interviewTransactionalIds", java.util.List.of[String]())
                .append("state", "Pending")
                .append("revision", Long.box(0L))
            )
            .void
        }
        _ <- invalidRows.toList.traverse_ { malformed =>
          val subject = UUID.randomUUID().toString
          val row = new Document("_id", subject)
            .append("requestedAt", at)
            .append("interviewTransactionalIds", java.util.List.of[String]())
          row.putAll(malformed)
          for {
            _ <- queue.insertOne(row)
            first <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop).attempt
            repeated <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop).attempt
            _ <- IO {
              assert(first.isLeft)
              assert(repeated.isLeft)
            }
            _ <- queue.deleteMany(Filters.eq("_id", row.get("_id")))
          } yield ()
        }
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
      } yield ()
    }
  }

  test("startup rejects malformed cleanup migration ledger without rewriting it") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val malformedRows = Vector(
        new Document("version", Long.box(2L)).append("state", "Complete"),
        new Document("version", Int.box(1)).append("state", "Complete"),
        new Document("version", Long.box(1L)).append("state", "Unknown"),
        new Document("version", Long.box(1L)),
        new Document("state", "Running")
      )
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        ledger <- Mongo4catsCollections.documents(fixture.database, MongoCollections.HiringMigrationLedger)
        _ <- malformedRows.toList.traverse_ { malformed =>
          val row = new Document("_id", MongoInterviewCleanupMigrations.MigrationId)
          row.putAll(malformed)
          for {
            _ <- ledger.deleteMany(Filters.eq("_id", MongoInterviewCleanupMigrations.MigrationId))
            _ <- ledger.insertOne(row)
            result <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop).attempt
            stored <- ledger.find(Filters.eq("_id", MongoInterviewCleanupMigrations.MigrationId)).first
            _ <- IO {
              assert(result.isLeft)
              assertEquals(stored, Some(row))
            }
          } yield ()
        }
      } yield ()
    }
  }
}
