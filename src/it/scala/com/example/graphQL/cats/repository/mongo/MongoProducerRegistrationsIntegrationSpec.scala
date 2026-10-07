package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.{RepositoryIO, RepositoryError}
import com.example.graphQL.cats.service.events.*
import org.bson.Document
import com.mongodb.client.model.Filters
import io.circe.Json
import java.time.Instant
import java.util.{Date, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class MongoProducerRegistrationsIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout = 5.minutes
  private val now = Instant.parse("2026-10-07T08:00:00Z")
  private def success[A](effect: RepositoryIO[A]): IO[A] =
    effect.value.flatMap(value => IO.fromEither(value.leftMap(error => new AssertionError(error.toString))))

  test("stopped generation retirement traverses 130 rows, retries broker failure and preserves unrelated generations") {
    mongoResource.use { fixture =>
      val selected = "hiring-interview-worker-" + UUID.randomUUID()
      val unrelated = "hiring-publisher-" + UUID.randomUUID()
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- (1 to 130).toList.traverse_(_ =>
          success(
            MongoProducerRegistrations.register(
              fixture.database,
              None,
              UUID.randomUUID().toString,
              selected,
              "Interview",
              now
            )
          )
        )
        _ <- success(
          MongoProducerRegistrations.register(
            fixture.database,
            None,
            UUID.randomUUID().toString,
            unrelated,
            "Operational",
            now
          )
        )
        fenceCalls <- Ref.of[IO, Int](0)
        failing = new MongoProducerGenerationMaintenance(
          fixture.database,
          _ =>
            RepositoryIO
              .lift(fenceCalls.update(_ + 1))
              .flatMap(_ => RepositoryIO.fromEither[Unit](Left(RepositoryError.Unavailable))),
          IO.pure(now)
        )
        inventory <- success(failing.inventory(None))
        next <- success(failing.inventory(inventory.nextCursor))
        failed <- failing.retire(selected).value
        rows <- Mongo4catsCollections.documents(fixture.database, MongoProducerRegistrations.Collection)
        stillActive <- rows.count(Filters.eq("state", "Active"), new com.mongodb.client.model.CountOptions())
        recovering = new MongoProducerGenerationMaintenance(
          fixture.database,
          _ => RepositoryIO.lift(fenceCalls.update(_ + 1)),
          IO.pure(now)
        )
        _ <- success(recovering.retire(selected))
        _ <- success(recovering.retire(selected))
        active <- rows.find(Filters.eq("state", "Active")).all
        fenced <- rows.find(Filters.eq("state", "Fenced")).all
        calls <- fenceCalls.get
      } yield {
        assert(inventory.nextCursor.nonEmpty)
        assert(next.nextCursor.nonEmpty)
        assertEquals(failed, Left(RepositoryError.Unavailable))
        assertEquals(stillActive, 131L)
        assertEquals(active.map(_.getString("transactionalId")).toList, List(unrelated))
        assertEquals(fenced.size, 130)
        assert(fenced.forall(_.getDate("expiresAt").toInstant == now.plusSeconds(8.days.toSeconds)))
        assertEquals(calls, 3)
      }
    }
  }

  test("restart after broker fencing but before checkpoint safely repeats retirement") {
    mongoResource.use { fixture =>
      val id = "hiring-interview-orchestrator-" + UUID.randomUUID()
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- success(
          MongoProducerRegistrations.register(fixture.database, None, UUID.randomUUID().toString, id, "Interview", now)
        )
        brokerConfirmed <- Ref.of[IO, Int](0)
        interrupted = new MongoProducerGenerationMaintenance(
          fixture.database,
          _ => RepositoryIO.lift(brokerConfirmed.update(_ + 1)),
          IO.raiseError(new IllegalStateException("process stopped after fencing"))
        )
        interruptedResult <- interrupted.retire(id).value.attempt
        restarted = new MongoProducerGenerationMaintenance(
          fixture.database,
          _ => RepositoryIO.lift(brokerConfirmed.update(_ + 1)),
          IO.pure(now)
        )
        before <- success(restarted.inventory(None))
        _ <- success(restarted.retire(id))
        after <- success(restarted.inventory(None))
        calls <- brokerConfirmed.get
      } yield {
        assert(interruptedResult.isLeft)
        assertEquals(before.transactionalIds, Vector(id))
        assertEquals(after.transactionalIds, Vector.empty)
        assertEquals(calls, 2)
      }
    }
  }

  test("bounded registration pages persist fencing checkpoints and active generations have no expiry") {
    mongoResource.use { fixture =>
      val subject = UserId(UUID.randomUUID())
      val ids = Vector.fill(130)("hiring-interview-worker-" + UUID.randomUUID())
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- ids.traverse_(id =>
          success(
            MongoProducerRegistrations.register(fixture.database, None, subject.value.toString, id, "Interview", now)
          )
        )
        _ <- success(cleanup.enqueue(subject, now, None))
        stored <- success(cleanup.find(subject))
        first <- success(cleanup.producerBatch(subject))
        _ = assertEquals(first.size, 64)
        _ <- success(cleanup.markProducersFenced(subject, first, now))
        restarted = new MongoInterviewSubjectCleanup(fixture.database)
        second <- success(restarted.producerBatch(subject))
        _ = assertEquals(second.size, 64)
        _ = assert(first.toSet.intersect(second.toSet).isEmpty)
        _ <- success(restarted.markProducersFenced(subject, second, now))
        finalBatch <- success(restarted.producerBatch(subject))
        _ = assertEquals(finalBatch.size, 2)
        rows <- Mongo4catsCollections.documents(fixture.database, MongoProducerRegistrations.Collection)
        active <- rows.find(Filters.eq("state", "Active")).all
        fenced <- rows.find(Filters.eq("state", "Fenced")).all
      } yield {
        assertEquals(stored.map(_.transactionalIds), Some(Vector.empty))
        assert(active.forall(!_.containsKey("expiresAt")))
        assert(fenced.forall(row => row.getDate("expiresAt").toInstant == now.plusSeconds(8.days.toSeconds)))
      }
    }
  }

  test("fair claim scan checkpoints past more than one page of busy subjects") {
    mongoResource.use { fixture =>
      val hot = UserId(UUID.randomUUID())
      val independent = UserId(UUID.randomUUID())
      val outbox =
        MongoOperationalEventOutboxRepository.transactional(fixture.database, fixture.client, Diagnostics.noop)
      def event(n: Long, actor: UserId): OperationalEventEnvelope = OperationalEventEnvelope(
        new UUID(0L, n),
        OperationalEventType.JOB_CREATED,
        now,
        OperationalAggregateType.Job,
        new UUID(1L, n).toString,
        actor,
        Json.obj(
          "job" -> Json.obj(
            "jobId" -> Json.fromString(new UUID(1L, n).toString),
            "skills" -> Json.arr(Json.fromString("Scala")),
            "status" -> Json.fromString("Open")
          )
        )
      )
      val desired = event(200L, independent)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.OutboxSubjectFences,
          new Document("_id", hot.value.toString)
            .append("deleted", false)
            .append("leaseUntil", Date.from(now.plusSeconds(60)))
        )
        _ <- ((1L to 128L).map(n => event(n, hot)).toList :+ desired).traverse_(value =>
          IO.fromEither(MongoHiringCodecs.outboxRecord(value, now).leftMap(new AssertionError(_)))
            .flatMap(row => MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.EventOutbox, row))
        )
        first <- success(outbox.claim("fair-worker", "hiring-publisher-test", now, now.plusSeconds(60), 1))
        second <- success(outbox.claim("fair-worker", "hiring-publisher-test", now, now.plusSeconds(60), 1))
        third <- success(outbox.claim("fair-worker", "hiring-publisher-test", now, now.plusSeconds(60), 1))
      } yield {
        assertEquals(first, Nil)
        assertEquals(second, Nil)
        assertEquals(third.map(_.event.eventId), List(desired.eventId))
      }
    }
  }

  test("maintenance migration replays copied registrations before contracting legacy arrays") {
    mongoResource.use { fixture =>
      val subject = UUID.randomUUID().toString
      val ids = Vector("hiring-interview-worker-" + UUID.randomUUID(), "hiring-interview-worker-" + UUID.randomUUID())
      for {
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.OutboxSubjectFences,
          new Document("_id", subject).append("deleted", true).append("interviewTransactionalIds", ids.asJava)
        )
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.InterviewSubjectCleanup,
          new Document("_id", subject)
            .append("state", "Pending")
            .append("revision", Long.box(0L))
            .append("requestedAt", Date.from(now))
            .append("interviewTransactionalIds", ids.asJava)
        )
        _ <- success(
          MongoProducerRegistrations.register(
            fixture.database,
            None,
            subject,
            ids.headOption.fold("")(identity),
            "Interview",
            now
          )
        )
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        first <- success(MongoProducerRegistrations.batch(fixture.database, subject, "Interview"))
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop)
        second <- success(MongoProducerRegistrations.batch(fixture.database, subject, "Interview"))
        fence <- MongoRepositoryTestSupport.findOne(
          fixture.database,
          MongoCollections.OutboxSubjectFences,
          Filters.eq("_id", subject)
        )
        staleWrite <- MongoRepositoryTestSupport
          .insertOne(
            fixture.database,
            MongoCollections.OutboxSubjectFences,
            new Document("_id", UUID.randomUUID().toString).append("interviewTransactionalIds", ids.asJava)
          )
          .attempt
      } yield {
        assertEquals(first.sorted, ids.sorted)
        assertEquals(second, first)
        assert(fence.forall(!_.containsKey("interviewTransactionalIds")))
        assert(staleWrite.isLeft)
      }
    }
  }
}
