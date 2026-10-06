package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.Diagnostics
import org.bson.Document
import com.mongodb.client.model.Filters
import java.time.Instant
import java.util.{Date, UUID}
import munit.CatsEffectSuite
import scala.concurrent.duration.*

final class MongoInterviewSubjectCleanupIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes
  test("workflow deletion remains pending until drain and physical retention, purges both participants' effects") {
    MongoAccessEvaluationSupport.resource.use { fixture =>
      val subject = UserId(UUID.randomUUID())
      val other = UUID.randomUUID().toString
      val workflowId = UUID.randomUUID().toString
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      val at = Instant.now()
      val marker = new Document("topic", "hiring.interview-results")
        .append("partition", Int.box(0))
        .append("endOffset", Long.box(4))
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
        _ <- cleanup.runOnce(1.second, IO.pure(List(marker)), _ => IO.pure(false))
        purged <- MongoRepositoryTestSupport.count(fixture.database, MongoCollections.FakeInterviewCalendarReservations)
        notifications <- MongoRepositoryTestSupport.count(
          fixture.database,
          MongoCollections.FakeInterviewNotificationReceipts
        )
        _ <- MongoSessionOperations
          .updateOne(
            Mongo4catsCollections.documents(fixture.database, MongoCollections.InterviewSubjectCleanup),
            None,
            MongoFilter.eq("_id", subject.value.toString),
            MongoUpdate.set("purgedAt", Date.from(at.minusSeconds(3600)))
          )
          .void
        _ <- cleanup.runOnce(1.second, IO.pure(List(marker)), _ => IO.pure(false))
        pending <- cleanup.complete(subject)
        _ <- cleanup.runOnce(1.second, IO.raiseError(new AssertionError("barrier must be reused")), _ => IO.pure(false))
        pendingAfterRestart <- new MongoInterviewSubjectCleanup(fixture.database).complete(subject)
        _ <- cleanup.runOnce(1.second, IO.raiseError(new AssertionError("barrier must be reused")), _ => IO.pure(true))
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
        _ <- cleanup.runOnce(1.second, IO.pure(Nil), _ => IO.pure(false))
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
}
