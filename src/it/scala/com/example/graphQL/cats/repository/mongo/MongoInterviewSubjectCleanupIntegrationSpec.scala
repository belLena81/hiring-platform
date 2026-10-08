package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref, Deferred}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.application.InterviewSubjectCleanupWorker
import com.example.graphQL.cats.service.port.{
  InterviewPublisherFencer,
  InterviewRetentionBarrier,
  RepositoryIO,
  InterviewCleanupUpdate,
  InterviewCleanupCursor,
  InterviewCleanupPage,
  RepositoryError
}
import com.example.graphQL.cats.domain.workflow.{
  InterviewCleanupObservation,
  InterviewCleanupState,
  InterviewSubjectCleanup
}
import org.bson.{Document, BsonBinary, BsonDocument, BsonDouble, BsonInt32, BsonInt64, BsonNull, BsonString}
import com.mongodb.client.model.Filters
import java.time.Instant
import java.util.{Date, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import io.circe.parser.parse

final class MongoInterviewSubjectCleanupIntegrationSpec extends MongoIntegrationSuite {
  override protected def dedicatedMongo: Boolean = true
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
    worker(cleanup, passed)
      .runOnce(None)
      .value
      .flatMap(result =>
        IO(
          assert(
            result.flatMap(progress => progress.firstFailure.toLeft(())).isRight
          )
        )
      )

  private def page(
      cleanup: MongoInterviewSubjectCleanup,
      cursor: Option[InterviewCleanupCursor],
      at: Instant
  ): IO[InterviewCleanupPage] =
    cleanup.pendingPage(cursor, at).value.flatMap {
      case Right(value) => IO.pure(value)
      case Left(error)  => IO.raiseError(new AssertionError(s"Cleanup selection failed: $error"))
    }

  private def pendingRow(id: AnyRef, at: Instant): Document =
    new Document("_id", id)
      .append("state", "Pending")
      .append("revision", Long.box(0L))
      .append("requestedAt", Date.from(at))
      .append("interviewTransactionalIds", java.util.List.of[String]())
      .append("producerRegistry", true)

  test("cleanup visits a later subject while thirty-two older subjects await retention") {
    mongoResource.use { fixture =>
      val at = Instant.now().minusSeconds(60)
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      val later = UserId(new UUID(0L, 33L))
      val cleaner = worker(cleanup, false)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- (1L to 32L).toList.traverse_ { ordinal =>
          val row = pendingRow(new UUID(0L, ordinal).toString, at)
            .append("state", "AwaitingRetention")
            .append("revision", Long.box(3L))
            .append(
              "barriers",
              java.util.List.of(
                new Document("topic", "hiring.interview-commands")
                  .append("partition", Int.box(0))
                  .append("endOffset", Long.box(4L)),
                new Document("topic", "hiring.interview-results")
                  .append("partition", Int.box(0))
                  .append("endOffset", Long.box(5L))
              )
            )
          MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.InterviewSubjectCleanup, row)
        }
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewSubjectCleanup,
          pendingRow(later.value.toString, at.plusSeconds(1))
        )
        _ <- List.fill(4)(()).foldLeftM(Option.empty[InterviewCleanupCursor]) { (cursor, _) =>
          cleaner.runOnce(cursor).value.flatMap {
            case Right(progress) => IO.pure(progress.next)
            case Left(error)     => IO.raiseError(new AssertionError(s"Cleanup page failed: $error"))
          }
        }
        reached <- cleanup.find(later).value
      } yield assertEquals(reached.toOption.flatten.map(_.state), Some(InterviewCleanupState.MongoPurged))
    }
  }

  /** Deliberate privileged corruption exercises defensive row handling; normal writes are strictly validated. */
  private def allowCorruptFixtures(database: mongo4cats.database.MongoDatabase[IO]): IO[Unit] =
    database
      .runCommand(
        new Document("collMod", MongoCollections.InterviewSubjectCleanup)
          .append("validationLevel", "off"),
        com.mongodb.ReadPreference.primary()
      )
      .void

  test("malformed selected cleanup records do not prevent a healthy subject from advancing") {
    mongoResource.use { fixture =>
      val at = Instant.now().minusSeconds(60)
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      val healthy = UserId(new UUID(0L, 2L))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- allowCorruptFixtures(fixture.database)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewSubjectCleanup,
          pendingRow(new UUID(0L, 1L).toString, at).append("revision", "invalid")
        )
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewSubjectCleanup,
          pendingRow(Int.box(7), at)
        )
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewSubjectCleanup,
          pendingRow(healthy.value.toString, at.plusSeconds(1))
        )
        _ <- worker(cleanup, false).runOnce(None).value
        reached <- cleanup.find(healthy).value
        malformedComplete <- cleanup.complete(UserId(new UUID(0L, 1L)))
      } yield {
        assertEquals(reached.toOption.flatten.map(_.state), Some(InterviewCleanupState.ProducersFenced))
        assert(!malformedComplete)
      }
    }
  }

  test("cleanup cursor retains numeric widths, binary values and ordered document identities") {
    val at = Instant.parse("2026-10-06T12:00:00Z")
    val keys = Vector(
      new BsonInt32(7),
      new BsonInt64(7L),
      new BsonDouble(7.0),
      new BsonBinary(Array[Byte](1, 2, 3)),
      BsonNull.VALUE,
      new BsonString("$revision"),
      new BsonDocument("second", new BsonInt64(2L)).append("first", new BsonInt32(1))
    )
    keys.foreach { key =>
      val state = MongoInterviewCleanupSweepCodec.Sweep(Some(key), key, at)
      val decoded = MongoInterviewCleanupSweepCodec.decode(MongoInterviewCleanupSweepCodec.encode(state, key))
      assertEquals(decoded, Right(state))
      assertEquals(decoded.toOption.map(_.throughId.getBsonType), Some(key.getBsonType))
      if (key.isDocument)
        assertEquals(
          decoded.toOption.map(_.throughId.asDocument().keySet().asScala.toVector),
          Some(key.asDocument().keySet().asScala.toVector)
        )
    }
  }

  test("bounded cleanup pages cross malformed identity types and timestamps without losing healthy work") {
    mongoResource.use { fixture =>
      val at = Instant.now().minusSeconds(60)
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      val healthy = UserId(new UUID(0L, 2L))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- allowCorruptFixtures(fixture.database)
        _ <- (1 to 31).toList.traverse_(ordinal =>
          MongoRepositoryTestSupport.insertOne(
            fixture.database,
            MongoCollections.InterviewSubjectCleanup,
            pendingRow(Int.box(ordinal), at).append("requestedAt", "malformed")
          )
        )
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewSubjectCleanup,
          pendingRow("$revision", at).append("requestedAt", java.util.List.of(Date.from(at)))
        )
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewSubjectCleanup,
          pendingRow(healthy.value.toString, at)
        )
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewSubjectCleanup,
          pendingRow(new Document("key", "invalid"), at)
        )
        first <- page(cleanup, None, at.plusSeconds(30))
        _ = assertEquals(first.entries, Vector.fill(32)(Left(RepositoryError.InvalidStoredData)))
        _ = assert(first.next.nonEmpty)
        second <- page(cleanup, first.next, at.plusSeconds(31))
        _ = assertEquals(second.entries.size, 2)
        _ = assertEquals(second.entries.flatMap(_.toOption).map(_.subjectId), Vector(healthy))
        _ = assertEquals(second.entries.count(_.isLeft), 1)
        _ = assertEquals(second.next, None)
        progress <- worker(cleanup, false).runOnce(first.next).value
        reached <- cleanup.find(healthy).value
        corrupt <- MongoRepositoryTestSupport.count(
          fixture.database,
          MongoCollections.InterviewSubjectCleanup,
          Filters.not(Filters.`type`("_id", org.bson.BsonType.STRING))
        )
      } yield {
        assertEquals(progress.toOption.flatMap(_.firstFailure), Some(RepositoryError.InvalidStoredData))
        assertEquals(reached.toOption.flatten.map(_.state), Some(InterviewCleanupState.ProducersFenced))
        assertEquals(corrupt, 32L)
      }
    }
  }

  test("a finite cleanup sweep defers later arrivals and future dates until the next sweep") {
    mongoResource.use { fixture =>
      val at = Instant.now().minusSeconds(60)
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      val behind = UserId(new UUID(0L, 0L))
      val ahead = UserId(new UUID(0L, 34L))
      val future = UserId(new UUID(0L, 35L))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- (1L to 33L).toList.traverse_(ordinal =>
          MongoRepositoryTestSupport.insertOne(
            fixture.database,
            MongoCollections.InterviewSubjectCleanup,
            pendingRow(new UUID(0L, ordinal).toString, at)
          )
        )
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewSubjectCleanup,
          pendingRow(future.value.toString, at.plusSeconds(100))
        )
        first <- page(cleanup, None, at.plusSeconds(1))
        _ <- List(behind, ahead).traverse_(id =>
          MongoRepositoryTestSupport.insertOne(
            fixture.database,
            MongoCollections.InterviewSubjectCleanup,
            pendingRow(id.value.toString, if (id == ahead) at else at.plusSeconds(2))
          )
        )
        last <- page(cleanup, first.next, at.plusSeconds(3))
        _ = assertEquals(last.entries.flatMap(_.toOption).map(_.subjectId), Vector(UserId(new UUID(0L, 33L))))
        _ = assertEquals(last.next, None)
        restarted <- page(cleanup, None, at.plusSeconds(4))
        rest <- page(cleanup, restarted.next, at.plusSeconds(4))
        ids = (restarted.entries ++ rest.entries).flatMap(_.toOption).map(_.subjectId)
        later <- page(cleanup, None, at.plusSeconds(101))
        laterRest <- page(cleanup, later.next, at.plusSeconds(101))
      } yield {
        assertEquals(first.entries.size, 32)
        assert(ids.contains(behind) && ids.contains(ahead))
        assert(!ids.contains(future))
        assert((later.entries ++ laterRest.entries).flatMap(_.toOption).exists(_.subjectId == future))
      }
    }
  }

  test("a later arrival inside the current identity range waits for the next sweep") {
    mongoResource.use { fixture =>
      val at = Instant.now().minusSeconds(60)
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      val inserted = UserId(new UUID(0L, 33L))
      val ceiling = UserId(new UUID(0L, 34L))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- ((1L to 32L).toList :+ 34L).traverse_(ordinal =>
          MongoRepositoryTestSupport.insertOne(
            fixture.database,
            MongoCollections.InterviewSubjectCleanup,
            pendingRow(new UUID(0L, ordinal).toString, at)
          )
        )
        first <- page(cleanup, None, at.plusSeconds(1))
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewSubjectCleanup,
          pendingRow(inserted.value.toString, at.plusSeconds(2))
        )
        continued <- page(cleanup, first.next, at.plusSeconds(3))
        restarted <- page(cleanup, None, at.plusSeconds(4))
        restartedRest <- page(cleanup, restarted.next, at.plusSeconds(4))
      } yield {
        assert(first.next.nonEmpty)
        assertEquals(continued.entries.flatMap(_.toOption).map(_.subjectId), Vector(ceiling))
        assertEquals(continued.next, None)
        assert((restarted.entries ++ restartedRest.entries).flatMap(_.toOption).exists(_.subjectId == inserted))
      }
    }
  }

  test("cleanup selection explains use the active identity index without a blocking sort") {
    mongoResource.use { fixture =>
      val at = Instant.now().minusSeconds(60)
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- (1L to 33L).toList.traverse_(ordinal =>
          MongoRepositoryTestSupport.insertOne(
            fixture.database,
            MongoCollections.InterviewSubjectCleanup,
            pendingRow(new UUID(0L, ordinal).toString, at)
          )
        )
        _ <- fixture.commands.clear
        first <- page(cleanup, None, at.plusSeconds(1))
        _ <- page(cleanup, first.next, at.plusSeconds(1))
        reads <- fixture.commands.snapshot.map(
          _.filter(command =>
            command.getFirstKey == "find" && command
              .getString("find")
              .getValue == MongoCollections.InterviewSubjectCleanup
          )
        )
        plans <- reads.traverse { observed =>
          val command = Document.parse(observed.toJson)
          List("$db", "lsid", "readConcern").foreach(command.remove)
          MongoAccessEvaluationSupport.command(
            fixture.database,
            new Document("explain", command).append("verbosity", "executionStats")
          )
        }
      } yield {
        assertEquals(reads.size, 3)
        plans.foreach { result =>
          val json = parse(result.toJson).toOption.getOrElse(fail("Invalid explain JSON"))
          assert(
            json.findAllByKey("indexName").flatMap(_.asString).contains(MongoInterviewCleanupSweepCodec.ActiveIndex)
          )
          assert(!json.findAllByKey("stage").flatMap(_.asString).contains("SORT"))
          val stats = json.hcursor.downField("executionStats")
          val returned = stats.get[Long]("nReturned").toOption.getOrElse(fail("Missing nReturned"))
          val examined = stats.get[Long]("totalDocsExamined").toOption.getOrElse(fail("Missing documents examined"))
          val keys = stats.get[Long]("totalKeysExamined").toOption.getOrElse(fail("Missing keys examined"))
          assert(returned <= 32L)
          assert(examined <= 33L && keys <= 33L)
          println(s"Cleanup identity index: returned=$returned documentsExamined=$examined keysExamined=$keys")
        }
      }
    }
  }

  test("an exactly full cleanup page and a removed sweep ceiling both wrap safely") {
    mongoResource.use { fixture =>
      val at = Instant.now().minusSeconds(60)
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- (1L to 32L).toList.traverse_(ordinal =>
          MongoRepositoryTestSupport.insertOne(
            fixture.database,
            MongoCollections.InterviewSubjectCleanup,
            pendingRow(new UUID(0L, ordinal).toString, at)
          )
        )
        full <- page(cleanup, None, at.plusSeconds(1))
        _ = assertEquals(full.entries.size, 32)
        _ = assertEquals(full.next, None)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewSubjectCleanup,
          pendingRow(new UUID(0L, 33L).toString, at)
        )
        first <- page(cleanup, None, at.plusSeconds(1))
        queue <- Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewSubjectCleanup)
        _ <- queue.deleteOne(Filters.eq("_id", new UUID(0L, 33L).toString))
        last <- page(cleanup, first.next, at.plusSeconds(2))
      } yield {
        assert(first.next.nonEmpty)
        assertEquals(last, InterviewCleanupPage(Vector.empty, None))
      }
    }
  }

  test("two cleaners racing the same snapshot advance once and a restarted cleaner completes the durable steps") {
    mongoResource.use { fixture =>
      val at = Instant.now().minusSeconds(60)
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      val subject = UserId(new UUID(0L, 1L))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewSubjectCleanup,
          pendingRow(subject.value.toString, at)
        )
        entered <- Ref.of[IO, Int](0)
        both <- Deferred[IO, Unit]
        synchronizedFencer = new InterviewPublisherFencer {
          override def fence(ids: Vector[String]) = RepositoryIO.lift(
            entered.updateAndGet(_ + 1).flatMap(count => if (count == 2) both.complete(()).void else both.get)
          )
        }
        first = new InterviewSubjectCleanupWorker(
          cleanup,
          synchronizedFencer,
          IO.pure(barriers),
          _ => IO.pure(false),
          Diagnostics.noop
        )
        second = new InterviewSubjectCleanupWorker(
          cleanup,
          synchronizedFencer,
          IO.pure(barriers),
          _ => IO.pure(false),
          Diagnostics.noop
        )
        raced <- (first.runOnce(None).value, second.runOnce(None).value).parTupled.timeout(20.seconds)
        afterRace <- cleanup.find(subject).value
        _ <- step(cleanup)
        _ <- step(cleanup)
        awaiting <- cleanup.find(subject).value
        _ <- step(cleanup, true)
        done <- cleanup.complete(subject)
      } yield {
        assert(raced._1.exists(_.firstFailure.isEmpty) && raced._2.exists(_.firstFailure.isEmpty))
        assertEquals(afterRace.toOption.flatten.map(_.revision), Some(1L))
        assertEquals(afterRace.toOption.flatten.map(_.state), Some(InterviewCleanupState.ProducersFenced))
        assertEquals(awaiting.toOption.flatten.map(_.revision), Some(3L))
        assert(done)
      }
    }
  }
  test("workflow deletion requires confirmed fencing and physical retention, purges both participants' effects") {
    mongoResource.use { fixture =>
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
        ).runOnce(None).value
        pendingAfterRestart <- new MongoInterviewSubjectCleanup(fixture.database).complete(subject)
        _ <- new InterviewSubjectCleanupWorker(
          cleanup,
          fencer,
          IO.raiseError(new AssertionError("barrier must be reused")),
          _ => IO.pure(true),
          Diagnostics.noop
        ).runOnce(None).value
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
    mongoResource.use { fixture =>
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
    mongoResource.use { fixture =>
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
    mongoResource.use { fixture =>
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
    mongoResource.use { fixture =>
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
        )
        _ <- MongoProducerRegistrations
          .register(fixture.database, None, subject.value.toString, generation, "Interview", Instant.now())
          .value
        _ <- MongoProducerRegistrations
          .register(fixture.database, None, subject.value.toString, unrelated, "Operational", Instant.now())
          .value
        _ <- cleanup.enqueue(subject, Instant.now(), None).value.flatMap(result => IO(assert(result.isRight)))
        ids <- cleanup.producerBatch(subject).value
        operational <- MongoProducerRegistrations.batch(fixture.database, subject.value.toString, "Operational").value
        current <- cleanup.find(subject).value
        complete <- cleanup.complete(subject)
        fence <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.OutboxSubjectFences,
          Filters.eq("_id", subject.value.toString)
        )
      } yield {
        assertEquals(ids, Right(Vector(generation)))
        assertEquals(current.toOption.flatten.map(_.transactionalIds), Some(Vector.empty))
        assertEquals(current.toOption.flatten.map(_.state), Some(InterviewCleanupState.Pending))
        assert(!complete)
        assertEquals(operational, Right(Vector(unrelated)))
        assert(fence.forall(!_.containsKey("transactionalIds")))
      }
    }
  }

  test("competing cleaners cannot overwrite a newer durable phase with a stale observation") {
    mongoResource.use { fixture =>
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
    mongoResource.use { fixture =>
      val subject = UserId(UUID.randomUUID())
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- allowCorruptFixtures(fixture.database)
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
    mongoResource.use { fixture =>
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
          new Document("_id", s"$workflowId:command")
            .append("workflowId", workflowId)
            .append("stepId", "command")
            .append("revision", Long.box(0L))
            .append("attempts", Int.box(0))
            .append("executionAttempts", Int.box(0))
            .append("commandState", "Pending")
            .append("availableAt", java.util.Date.from(Instant.now()))
            .append("occurredAt", java.util.Date.from(Instant.now()))
            .append("command", new Document("kind", "requireRepair").append("reason", "Orphan command cleanup"))
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

  private enum CleanupAttribution {
    case RequestReceipt, CalendarReservation, NotificationReceipt, Workflow
  }

  private val cleanupDataCollections = Vector(
    MongoCollections.InterviewWorkflows,
    MongoCollections.InterviewWorkflowCommands,
    MongoCollections.InterviewWorkflowInbox,
    MongoCollections.FakeInterviewCalendarReservations,
    MongoCollections.FakeInterviewNotificationReceipts
  )

  private def seedCleanupData(
      database: mongo4cats.database.MongoDatabase[IO],
      subject: UserId,
      ids: Vector[String],
      attribution: Set[CleanupAttribution]
  ): IO[Unit] = {
    val at = Date.from(Instant.now())
    ids.toList.traverse_ { id =>
      val sources = attribution.toList.map {
        case CleanupAttribution.RequestReceipt =>
          MongoCollections.InterviewWorkflows -> new Document("_id", s"request:${subject.value}:$id")
            .append("documentType", "requestReceipt")
            .append("requestWorkflowId", id)
        case CleanupAttribution.CalendarReservation =>
          MongoCollections.FakeInterviewCalendarReservations -> new Document("_id", id)
            .append("workflowId", id)
            .append("participants", java.util.List.of(subject.value.toString))
            .append("releaseKey", s"$id:release")
        case CleanupAttribution.NotificationReceipt =>
          MongoCollections.FakeInterviewNotificationReceipts -> new Document("_id", s"$id:notify:Candidate")
            .append("workflowId", id)
            .append("recipientId", subject.value.toString)
        case CleanupAttribution.Workflow =>
          MongoCollections.InterviewWorkflows -> new Document("_id", id)
            .append("candidateId", subject.value.toString)
      }
      val children = List(
        MongoCollections.InterviewWorkflowCommands -> new Document("_id", s"$id:command")
          .append("workflowId", id)
          .append("stepId", "command")
          .append("revision", Long.box(0L))
          .append("attempts", Int.box(0))
          .append("executionAttempts", Int.box(0))
          .append("commandState", "Pending")
          .append("availableAt", at)
          .append("occurredAt", at)
          .append("command", new Document("kind", "requireRepair").append("reason", "Cleanup verification")),
        MongoCollections.InterviewWorkflowInbox -> new Document("_id", s"$id:receipt")
          .append("workflowId", id)
      )
      (sources ++ children).traverse_ { case (name, row) =>
        MongoRepositoryTestSupport.insertOne(database, name, row)
      }
    }
  }

  private def cleanupCounts(database: mongo4cats.database.MongoDatabase[IO]): IO[Vector[Long]] =
    cleanupDataCollections.traverse(name => MongoRepositoryTestSupport.count(database, name))

  test("cleanup removes linked children across more than one request receipt batch and preserves another subject") {
    mongoResource.use { fixture =>
      val subject = UserId(UUID.randomUUID())
      val other = UserId(UUID.randomUUID())
      val ids = Vector.fill(137)(UUID.randomUUID().toString)
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedCleanupData(
          fixture.database,
          other,
          Vector(UUID.randomUUID().toString),
          Set(CleanupAttribution.RequestReceipt)
        )
        preserved <- cleanupCounts(fixture.database)
        _ <- seedCleanupData(fixture.database, subject, ids, Set(CleanupAttribution.RequestReceipt))
        _ <- cleanup.enqueue(subject, Instant.now(), None).value.flatMap(result => IO(assert(result.isRight)))
        _ <- List.fill(4)(()).traverse_(_ => step(cleanup, passed = true))
        complete <- cleanup.complete(subject)
        remaining <- cleanupCounts(fixture.database)
      } yield {
        assert(complete)
        assertEquals(remaining, preserved)
      }
    }
  }

  test("cleanup discovers provider-only workflows across batches and deduplicates overlapping attribution") {
    mongoResource.use { fixture =>
      val subject = UserId(UUID.randomUUID())
      val other = UserId(UUID.randomUUID())
      val firstCalendar = UUID.randomUUID().toString
      val calendarOnly = firstCalendar +: Vector.fill(136)(UUID.randomUUID().toString)
      val notificationOnly = Vector.fill(137)(UUID.randomUUID().toString)
      val shared = UUID.randomUUID().toString
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedCleanupData(
          fixture.database,
          other,
          Vector(UUID.randomUUID().toString),
          CleanupAttribution.values.toSet
        )
        preserved <- cleanupCounts(fixture.database)
        _ <- seedCleanupData(fixture.database, subject, calendarOnly, Set(CleanupAttribution.CalendarReservation))
        _ <- seedCleanupData(fixture.database, subject, notificationOnly, Set(CleanupAttribution.NotificationReceipt))
        _ <- seedCleanupData(fixture.database, subject, Vector(shared), CleanupAttribution.values.toSet)
        // The surviving participant's receipt is attributable through the reservation, not its own recipient.
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.FakeInterviewNotificationReceipts,
          new Document("_id", s"$firstCalendar:notify:Recruiter")
            .append("workflowId", firstCalendar)
            .append("recipientId", other.value.toString)
        )
        _ <- cleanup.enqueue(subject, Instant.now(), None).value.flatMap(result => IO(assert(result.isRight)))
        _ <- List.fill(4)(()).traverse_(_ => step(cleanup, passed = true))
        complete <- cleanup.complete(subject)
        remaining <- cleanupCounts(fixture.database)
      } yield {
        assert(complete)
        assertEquals(remaining, preserved)
      }
    }
  }

  test("interrupted cleanup preserves attribution after child removal and converges on retry") {
    mongoResource.use { fixture =>
      val subject = UserId(UUID.randomUUID())
      val ids = Vector.fill(137)(UUID.randomUUID().toString)
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- seedCleanupData(fixture.database, subject, ids, Set(CleanupAttribution.CalendarReservation))
        _ <- cleanup.enqueue(subject, Instant.now(), None).value.flatMap(result => IO(assert(result.isRight)))
        _ <- step(cleanup)
        admin <- fixture.client.getDatabase("admin")
        _ <- MongoAccessEvaluationSupport.command(
          admin,
          new Document("configureFailPoint", "failCommand")
            .append("mode", new Document("skip", 2))
            .append("data", new Document("failCommands", List("delete").asJava).append("errorCode", 11601))
        )
        interrupted <- worker(cleanup, false)
          .runOnce(None)
          .value
          .guarantee(
            MongoAccessEvaluationSupport
              .command(
                admin,
                new Document("configureFailPoint", "failCommand").append("mode", "off")
              )
              .void
          )
        retained <- cleanup.find(subject).value
        commands <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.InterviewWorkflowCommands)
        receipts <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.InterviewWorkflowInbox)
        providers <- MongoRepositoryTestSupport.count(
          fixture.database,
          MongoCollections.FakeInterviewCalendarReservations
        )
        absent <- cleanup.absent(subject).value
        _ = assert(interrupted.toOption.exists(_.firstFailure.nonEmpty))
        _ = assertEquals(retained.toOption.flatten.map(_.state), Some(InterviewCleanupState.ProducersFenced))
        _ = assertEquals(commands, 9L)
        _ = assertEquals(receipts, 9L)
        _ = assertEquals(providers, 137L)
        _ = assertEquals(absent, Right(false))
        _ <- List.fill(3)(()).traverse_(_ => step(cleanup, passed = true))
        complete <- cleanup.complete(subject)
        remaining <- cleanupCounts(fixture.database)
      } yield {
        assert(complete)
        assertEquals(remaining, Vector.fill(cleanupDataCollections.size)(0L))
      }
    }
  }

  test("legacy cleanup completion is reopened with a fresh snapshot and migration is restartable") {
    mongoResource.use { fixture =>
      val subject = UserId(UUID.randomUUID())
      val generation = "hiring-interview-orchestrator-" + UUID.randomUUID().toString
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        ledger <- Mongo4catsCollections.documents(fixture.database, MongoCollections.HiringMigrationLedger)
        _ <- ledger.deleteMany(
          Filters.in(
            "_id",
            MongoInterviewCleanupMigrations.MigrationId,
            MongoProducerRegistrationMigrations.MigrationId,
            MongoInterviewCleanupIntegrityMigrations.MigrationId
          )
        )
        _ <- allowCorruptFixtures(fixture.database)
        _ <- fixture.database
          .runCommand(
            new Document("collMod", MongoCollections.OutboxSubjectFences)
              .append("validationLevel", "off"),
            com.mongodb.ReadPreference.primary()
          )
          .void
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
        ids <- cleanup.producerBatch(subject).value
        _ <- step(cleanup)
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        resumed <- cleanup.find(subject).value
      } yield {
        assertEquals(reopened.toOption.flatten.map(_.state), Some(InterviewCleanupState.Pending))
        assertEquals(ids, Right(Vector(generation)))
        assertEquals(reopened.toOption.flatten.map(_.transactionalIds), Some(Vector.empty))
        assertEquals(resumed.toOption.flatten.map(_.state), Some(InterviewCleanupState.ProducersFenced))
        assertEquals(resumed.toOption.flatten.map(_.revision), Some(2L))
      }
    }
  }

  test("strict cleanup proof rejects malformed normal writes after migration completion") {
    mongoResource.use { fixture =>
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
                .append("producerRegistry", true)
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
            .append("producerRegistry", true)
          row.putAll(malformed)
          for {
            rejected <- queue.insertOne(row).attempt
            _ <- IO(assert(rejected.isLeft))
          } yield ()
        }
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
      } yield ()
    }
  }

  test("startup rejects malformed cleanup migration ledger without rewriting it") {
    mongoResource.use { fixture =>
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
