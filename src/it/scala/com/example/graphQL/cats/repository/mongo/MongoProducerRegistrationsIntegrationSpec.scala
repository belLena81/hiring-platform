package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.domain.model.JobStatus
import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.port.{ProducerGeneration, ProducerGenerationId, RepositoryF}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.events.*
import org.bson.Document
import com.mongodb.client.model.Filters
import java.time.Instant
import java.util.{Date, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final class MongoProducerRegistrationsIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout = 5.minutes
  private val now = Instant.parse("2026-10-07T08:00:00Z")

  /** The instant is within seconds of the test host's own clock, which here shares the database's. */
  private def assertEqualsWithin(instant: Instant, started: Instant): Unit =
    assert(
      !instant.isBefore(started.minusSeconds(5)) && !instant.isAfter(Instant.now().plusSeconds(5)),
      clue(instant)
    )

  /** A Fenced row: registeredAt, fencedAt and expiresAt (fencedAt plus eight days) are database-stamped. */
  private def databaseStamped(row: Document, started: Instant): Unit = {
    assertEquals(row.getString("state"), "Fenced")
    val fencedAt = row.getDate("fencedAt").toInstant
    assertEqualsWithin(fencedAt, started)
    assertEqualsWithin(row.getDate("registeredAt").toInstant, started)
    assertEquals(row.getDate("expiresAt").toInstant, fencedAt.plusSeconds(8.days.toSeconds))
  }
  private def success[A](effect: RepositoryF[IO, A]): IO[A] =
    effect.value.flatMap(value => IO.fromEither(value.leftMap(error => new AssertionError(error.toString))))

  test("stopped generation retirement traverses 130 rows, retries broker failure and preserves unrelated generations") {
    mongoResource.use { fixture =>
      val selected = "hiring-interview-worker-" + UUID.randomUUID()
      val unrelated = "hiring-publisher-" + UUID.randomUUID()
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- (1 to 130).toList.traverse_(n =>
          success(
            MongoProducerRegistrations.register(
              fixture.database,
              None,
              UUID.randomUUID().toString,
              ProducerGenerationFixtures.lifetime(selected, n.toLong),
              "Interview"
            )
          )
        )
        _ <- success(
          MongoProducerRegistrations.register(
            fixture.database,
            None,
            UUID.randomUUID().toString,
            ProducerGenerationFixtures.of(unrelated),
            "Operational"
          )
        )
        _ <- IO.sleep(20.millis)
        started = Instant.now()
        fenceCalls <- Ref.of[IO, Int](0)
        failing = new MongoProducerGenerationMaintenance(
          fixture.database,
          _ =>
            RepositoryF
              .lift(fenceCalls.update(_ + 1))
              .flatMap(_ => RepositoryF.at[IO].fromEither[Unit](Left(RepositoryError.Unavailable)))
        )
        inventory <- success(failing.inventory(None))
        next <- success(failing.inventory(inventory.nextCursor))
        failed <- failing.retire(selected).value
        rows <- Mongo4catsCollections.documents(fixture.database, MongoProducerRegistrations.Collection)
        stillActive <- rows.count(Filters.eq("state", "Active"), new com.mongodb.client.model.CountOptions())
        recovering = new MongoProducerGenerationMaintenance(
          fixture.database,
          _ => RepositoryF.lift(fenceCalls.update(_ + 1))
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
        fenced.foreach(databaseStamped(_, started))
        assertEquals(calls, 3)
      }
    }
  }

  test("restart after broker fencing but before checkpoint safely repeats retirement") {
    mongoResource.use { fixture =>
      val id = "hiring-interview-orchestrator-" + UUID.randomUUID()
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- success(
          MongoProducerRegistrations.register(
            fixture.database,
            None,
            UUID.randomUUID().toString,
            ProducerGenerationFixtures.of(id),
            "Interview"
          )
        )
        _ <- IO.sleep(20.millis)
        brokerConfirmed <- Ref.of[IO, Int](0)
        // The process stops right after the broker confirmed fencing, before any row is marked.
        interrupted = new MongoProducerGenerationMaintenance(
          fixture.database,
          _ =>
            RepositoryF.lift(brokerConfirmed.update(_ + 1)) *>
              RepositoryF.lift(IO.raiseError[Unit](new IllegalStateException("process stopped after fencing")))
        )
        interruptedResult <- interrupted.retire(id).value.attempt
        restarted = new MongoProducerGenerationMaintenance(
          fixture.database,
          _ => RepositoryF.lift(brokerConfirmed.update(_ + 1))
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
      val ids = Vector.fill(130)(ProducerGenerationFixtures.of("hiring-interview-worker-" + UUID.randomUUID()))
      val cleanup = new MongoInterviewSubjectCleanup(fixture.database)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- ids.traverse_(id =>
          success(
            MongoProducerRegistrations.register(fixture.database, None, subject.value.toString, id, "Interview")
          )
        )
        startedAt = Instant.now()
        _ <- success(cleanup.enqueue(subject, now, None))
        stored <- success(cleanup.find(subject))
        first <- success(cleanup.producerBatch(subject))
        _ = assertEquals(first.size, 64)
        _ <- success(cleanup.markProducersFenced(subject, first))
        restarted = new MongoInterviewSubjectCleanup(fixture.database)
        second <- success(restarted.producerBatch(subject))
        _ = assertEquals(second.size, 64)
        _ = assert(first.toSet.intersect(second.toSet).isEmpty)
        _ <- success(restarted.markProducersFenced(subject, second))
        finalBatch <- success(restarted.producerBatch(subject))
        _ = assertEquals(finalBatch.size, 2)
        rows <- Mongo4catsCollections.documents(fixture.database, MongoProducerRegistrations.Collection)
        active <- rows.find(Filters.eq("state", "Active")).all
        fenced <- rows.find(Filters.eq("state", "Fenced")).all
      } yield {
        assertEquals(stored.map(_.transactionalIds), Some(Vector.empty))
        assert(active.forall(!_.containsKey("expiresAt")))
        fenced.foreach(databaseStamped(_, startedAt))
      }
    }
  }

  private def registrationRows(fixture: MongoAccessEvaluationSupport.Fixture) =
    Mongo4catsCollections
      .documents(fixture.database, MongoProducerRegistrations.Collection)
      .flatMap(_.find(Filters.empty()).all)
      .map(_.toList)

  test(
    "KFH-04.2 a generation registers for a subject whose earlier generation of the same transactional id is Fenced"
  ) {
    mongoResource.use { fixture =>
      val subject = UUID.randomUUID().toString
      val id = "hiring-publisher-publisher-a"
      val earlier = ProducerGenerationFixtures.lifetime(id, 1L)
      val later = ProducerGenerationFixtures.lifetime(id, 2L)
      def register(generation: ProducerGeneration) =
        success(MongoProducerRegistrations.register(fixture.database, None, subject, generation, "Operational"))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- register(earlier)
        _ <- success(
          MongoProducerRegistrations.markFenced(fixture.database, subject, "Operational", Vector(earlier))
        )
        _ <- register(later)
        // Registering the dead generation again neither revives nor duplicates it.
        _ <- register(earlier)
        stored <- registrationRows(fixture)
        active <- success(MongoProducerRegistrations.batch(fixture.database, subject, "Operational"))
      } yield {
        assertEquals(stored.size, 2)
        assertEquals(
          stored.map(row => row.getString("generationId") -> row.getString("state")).toMap,
          Map(earlier.generationId.value.toString -> "Fenced", later.generationId.value.toString -> "Active")
        )
        assertEquals(active, Vector(later))
      }
    }
  }

  test("KFH-04.3 marking fenced affects only the generations read, only while Active, and never extends expiry") {
    mongoResource.use { fixture =>
      val subject = UUID.randomUUID().toString
      val id = "hiring-interview-worker-worker-a"
      val read = ProducerGenerationFixtures.lifetime(id, 1L)
      val successor = ProducerGenerationFixtures.lifetime(id, 2L)
      def register(generation: ProducerGeneration) =
        success(MongoProducerRegistrations.register(fixture.database, None, subject, generation, "Interview"))
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- register(read)
        generations <- success(MongoProducerRegistrations.batch(fixture.database, subject, "Interview"))
        // The successor registers after the read and before the mark.
        _ <- register(successor)
        started = Instant.now()
        _ <- success(MongoProducerRegistrations.markFenced(fixture.database, subject, "Interview", generations))
        first <- registrationRows(fixture)
        _ <- IO.sleep(50.millis)
        _ <- success(MongoProducerRegistrations.markFenced(fixture.database, subject, "Interview", generations))
        stored <- registrationRows(fixture)
        remaining <- success(MongoProducerRegistrations.batch(fixture.database, subject, "Interview"))
      } yield {
        assertEquals(generations, Vector(read))
        assertEquals(remaining, Vector(successor))
        val fenced = stored.find(_.getString("state") == "Fenced").getOrElse(fail("no fenced generation"))
        assertEquals(fenced.getString("generationId"), read.generationId.value.toString)
        databaseStamped(fenced, started)
        // A repeated mark neither moves fencedAt nor extends the expiry.
        val firstFenced = first.find(_.getString("state") == "Fenced").getOrElse(fail("no fenced generation"))
        assertEquals(fenced.getDate("fencedAt"), firstFenced.getDate("fencedAt"))
        assertEquals(fenced.getDate("expiresAt"), firstFenced.getDate("expiresAt"))
        assert(stored.filter(_.getString("state") == "Active").forall(!_.containsKey("expiresAt")))
      }
    }
  }

  test(
    "KFH-04.6 retiring a running worker id finishes, marks pre-cut rows Fenced and leaves the successor's rows Active"
  ) {
    mongoResource.use { fixture =>
      val id = "hiring-publisher-publisher-a"
      val earlier = ProducerGenerationFixtures.lifetime(id, 1L)
      val successor = ProducerGenerationFixtures.lifetime(id, 2L)
      val subjects = List.fill(70)(UUID.randomUUID().toString)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- subjects.traverse_(subject =>
          success(MongoProducerRegistrations.register(fixture.database, None, subject, earlier, "Operational"))
        )
        _ <- IO.sleep(20.millis)
        fenceCalls <- Ref.of[IO, Int](0)
        // The replacement generation registers after the cut, while the broker fence is issued.
        maintenance = new MongoProducerGenerationMaintenance(
          fixture.database,
          _ =>
            RepositoryF.lift(fenceCalls.update(_ + 1)) *>
              subjects.traverse_(subject =>
                MongoProducerRegistrations.register(fixture.database, None, subject, successor, "Operational")
              )
        )
        _ <- success(maintenance.retire(id))
        stored <- registrationRows(fixture)
        calls <- fenceCalls.get
      } yield {
        val byGeneration = stored.groupBy(_.getString("generationId")).view.mapValues(_.map(_.getString("state")).toSet)
        assertEquals(stored.size, 140)
        assertEquals(byGeneration(earlier.generationId.value.toString), Set("Fenced"))
        assertEquals(byGeneration(successor.generationId.value.toString), Set("Active"))
        assertEquals(calls, 1)
      }
    }
  }

  test(
    "KFH-04.6 retirement uses database time for both the cut-off and registeredAt, so a skewed writer clock cannot fence a live successor"
  ) {
    mongoResource.use { fixture =>
      val id = "hiring-publisher-publisher-a"
      val earlier = ProducerGenerationFixtures.lifetime(id, 1L)
      val successor = ProducerGenerationFixtures.lifetime(id, 2L)
      val subject = UUID.randomUUID().toString
      // No application clock takes part: registration, the retirement cut-off, fencedAt and expiresAt are all stamped by
      // the database, so a host clock that is far off cannot change them (here the host clock is the test's own).
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        started = Instant.now()
        _ <- success(MongoProducerRegistrations.register(fixture.database, None, subject, earlier, "Operational"))
        _ <- IO.sleep(20.millis)
        maintenance = new MongoProducerGenerationMaintenance(
          fixture.database,
          _ => MongoProducerRegistrations.register(fixture.database, None, subject, successor, "Operational")
        )
        _ <- success(maintenance.retire(id))
        stored <- registrationRows(fixture)
      } yield {
        val byGeneration = stored.groupBy(_.getString("generationId"))
        val old = byGeneration(earlier.generationId.value.toString).head
        val live = byGeneration(successor.generationId.value.toString).head
        assertEquals(old.getString("state"), "Fenced")
        assertEquals(live.getString("state"), "Active")
        databaseStamped(old, started)
        assertEqualsWithin(live.getDate("registeredAt").toInstant, started)
        assert(!live.containsKey("expiresAt"))
      }
    }
  }

  test("KFH-04.6 retirement's Active-rows-before-the-cut query is served by the generation index with no sort stage") {
    mongoResource.use { fixture =>
      val id = "hiring-interview-worker-worker-a"
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        collection <- Mongo4catsCollections.documents(fixture.database, MongoProducerRegistrations.Collection)
        base = Instant.now().minusSeconds(3600)
        rows = (1 to 3000).toList.map { n =>
          val own = n % 2 == 0
          val generation =
            ProducerGenerationFixtures.lifetime(if (own) id else s"hiring-interview-worker-other-$n", n.toLong)
          new Document("_id", s"${UUID.randomUUID()}:${generation.transactionalId}:${generation.generationId.value}")
            .append("subjectId", UUID.randomUUID().toString)
            .append("transactionalId", generation.transactionalId)
            .append("generationId", generation.generationId.value.toString)
            .append("kind", "Interview")
            .append("state", if (n % 10 == 0) "Fenced" else "Active")
            .append("registeredAt", Date.from(base.plusSeconds(n.toLong)))
        }
        _ <- collection.insertMany(
          rows,
          new com.mongodb.client.model.InsertManyOptions().bypassDocumentValidation(true)
        )
        cut = base.plusSeconds(2900)
        find = new Document("find", MongoProducerRegistrations.Collection)
          .append("filter", MongoProducerGenerationMaintenance.retireFilter(id, cut).bson.toBsonDocument())
          .append("sort", MongoProducerGenerationMaintenance.RetireSort.toBsonDocument())
          .append("limit", Int.box(MongoProducerRegistrations.BatchSize))
        explained <- MongoAccessEvaluationSupport.command(
          fixture.database,
          new Document("explain", find).append("verbosity", "executionStats")
        )
        json = io.circe.parser.parse(explained.toJson).toOption.getOrElse(fail("Invalid explain JSON"))
        winning = json.hcursor.downField("queryPlanner").downField("winningPlan").focus.getOrElse(fail("no plan"))
        stages = winning.findAllByKey("stage").flatMap(_.asString).toSet
        names = winning.findAllByKey("indexName").flatMap(_.asString).toSet
        stats = json.hcursor.downField("executionStats")
        keys = stats.downField("totalKeysExamined").as[Long].getOrElse(-1L)
        docs = stats.downField("totalDocsExamined").as[Long].getOrElse(-1L)
        returned = stats.downField("nReturned").as[Long].getOrElse(-1L)
      } yield {
        assertEquals(names, Set(MongoIndexNames.ProducerRegistrationGenerationState), clue(winning))
        assert(!stages.contains("SORT") && !stages.contains("COLLSCAN"), clue(stages))
        // Bounded by the batch: only the returned rows are fetched.
        assertEquals(returned, 64L)
        assert(keys <= 65L && docs <= 64L, clue((keys, docs, returned)))
        println(
          s"retire explain: stages=$stages index=$names keysExamined=$keys docsExamined=$docs nReturned=$returned"
        )
      }
    }
  }

  test("fair claim scan checkpoints past more than one page of busy subjects across claims") {
    mongoResource.use { fixture =>
      val hot = UserId(UUID.randomUUID())
      val independent = UserId(UUID.randomUUID())
      val outbox =
        MongoOperationalEventOutboxRepository.transactional(
          fixture.database,
          fixture.client,
          Diagnostics.noop[IO],
          maxScanPages = 0
        )
      def event(n: Long, actor: UserId): OperationalEventEnvelope = OperationalEventEnvelope(
        new UUID(0L, n),
        OperationalEventType.JOB_CREATED,
        now,
        OperationalAggregateType.Job,
        new UUID(1L, n).toString,
        actor,
        OperationalEventPayload.JobFact(
          OperationalEventPayload.JobKind.Created,
          OperationalEventPayload.JobSnapshot(new UUID(1L, n), List("Scala"), JobStatus.Open)
        )
      )
      val desired = event(200L, independent)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.OutboxSubjectFences,
          new Document("_id", hot.value.toString)
            .append("deleted", false)
            .append("leaseUntil", Date.from(Instant.parse("2100-01-01T00:00:00Z")))
        )
        _ <- ((1L to 128L).map(n => event(n, hot)).toList :+ desired).traverse_(value =>
          IO.fromEither(MongoHiringCodecs.outboxRecord(value, now).leftMap(new AssertionError(_)))
            .flatMap(row => MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.EventOutbox, row))
        )
        first <- success(
          outbox.claim("fair-worker", ProducerGenerationFixtures.of("hiring-publisher-test"), now, 60.seconds, 1)
        )
        second <- success(
          outbox.claim("fair-worker", ProducerGenerationFixtures.of("hiring-publisher-test"), now, 60.seconds, 1)
        )
        third <- success(
          outbox.claim("fair-worker", ProducerGenerationFixtures.of("hiring-publisher-test"), now, 60.seconds, 1)
        )
      } yield {
        // With no extra pages per claim, each claim checkpoints one page of busy subjects and the next resumes there.
        assertEquals(first, Nil)
        assertEquals(second, Nil)
        assertEquals(third.map(_.event.eventId), List(desired.eventId))
      }
    }
  }

  test("fair claim scan continues past more than one page of busy subjects within one claim") {
    mongoResource.use { fixture =>
      val hot = UserId(UUID.randomUUID())
      val independent = UserId(UUID.randomUUID())
      val outbox =
        MongoOperationalEventOutboxRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
      def event(n: Long, actor: UserId): OperationalEventEnvelope = OperationalEventEnvelope(
        new UUID(0L, n),
        OperationalEventType.JOB_CREATED,
        now,
        OperationalAggregateType.Job,
        new UUID(1L, n).toString,
        actor,
        OperationalEventPayload.JobFact(
          OperationalEventPayload.JobKind.Created,
          OperationalEventPayload.JobSnapshot(new UUID(1L, n), List("Scala"), JobStatus.Open)
        )
      )
      val desired = event(200L, independent)
      for {
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        _ <- MongoRepositoryTestSupport.insertOne(
          fixture.database,
          MongoCollections.OutboxSubjectFences,
          new Document("_id", hot.value.toString)
            .append("deleted", false)
            .append("leaseUntil", Date.from(Instant.parse("2100-01-01T00:00:00Z")))
        )
        _ <- ((1L to 128L).map(n => event(n, hot)).toList :+ desired).traverse_(value =>
          IO.fromEither(MongoHiringCodecs.outboxRecord(value, now).leftMap(new AssertionError(_)))
            .flatMap(row => MongoRepositoryTestSupport.insertOne(fixture.database, MongoCollections.EventOutbox, row))
        )
        first <- success(
          outbox.claim("fair-worker", ProducerGenerationFixtures.of("hiring-publisher-test"), now, 60.seconds, 1)
        )
        second <- success(
          outbox.claim("fair-worker", ProducerGenerationFixtures.of("hiring-publisher-test"), now, 60.seconds, 1)
        )
        third <- success(
          outbox.claim("fair-worker", ProducerGenerationFixtures.of("hiring-publisher-test"), now, 60.seconds, 1)
        )
      } yield {
        // The scan used to checkpoint page by page over three claims; it now continues over the next pages (bounded by
        // MaxScanPages) so the independent event is claimed at once, and the later claims find nothing free.
        assertEquals(first.map(_.event.eventId), List(desired.eventId))
        assertEquals(second, Nil)
        assertEquals(third, Nil)
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
            ids.headOption.fold(ProducerGenerationFixtures.of(""))(id =>
              ProducerGeneration(id, ProducerGenerationId(UUID.fromString(id.takeRight(36))))
            ),
            "Interview"
          )
        )
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
        first <- success(MongoProducerRegistrations.batch(fixture.database, subject, "Interview"))
        _ <- MongoHiringSetup.initialize(fixture.database, Diagnostics.noop[IO])
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
        assertEquals(first.map(_.transactionalId).sorted, ids.sorted)
        assertEquals(second, first)
        assert(fence.forall(!_.containsKey("interviewTransactionalIds")))
        assert(staleWrite.isLeft)
      }
    }
  }
}
