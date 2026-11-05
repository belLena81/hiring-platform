package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.JobStatus
import com.example.graphQL.cats.infrastructure.events.OperationalEventWireCodec
import com.example.graphQL.cats.service.events.*
import com.example.graphQL.cats.service.events.OperationalEventPayload.SearchKind
import com.example.graphQL.cats.service.port.PendingSearchSessionWork
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.mongodb.MongoWriteException
import com.mongodb.client.model.{Filters, InsertManyOptions}
import mongo4cats.database.MongoDatabase
import org.bson.Document
import org.bson.conversions.Bson
import org.bson.types.Binary

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** The outbox keeps one copy of an event body (`envelopeBytes`): the writers, the readers and migration 026. */
final class MongoOutboxEnvelopeSourceIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes

  private val now = Instant.parse("2026-10-20T12:00:00Z")
  private val Source = MigrationIds.OutboxEnvelopeSource
  private val Outbox = MongoCollections.EventOutbox
  private val Work = MongoCollections.SearchSessionWork

  private def init(database: MongoDatabase[IO]): IO[Unit] = MongoHiringSetup.initialize(database, Diagnostics.noop[IO])

  private def collection(database: MongoDatabase[IO], name: String) = Mongo4catsCollections.documents(database, name)

  private def ledger(database: MongoDatabase[IO]): IO[List[Document]] =
    collection(database, MongoCollections.HiringMigrationLedger).flatMap(_.find(Filters.empty()).all).map(_.toList)

  private def sourceRow(database: MongoDatabase[IO]): IO[Option[Document]] =
    ledger(database).map(_.find(_.getString(MongoFields.Id) == Source.value))

  private def count(database: MongoDatabase[IO], name: String, filter: Bson = Filters.empty()): IO[Long] =
    MongoRepositoryTestSupport.count(database, name, filter)

  private def rejected(result: Either[Throwable, ?]): Boolean = result.left.exists {
    case error: MongoWriteException => error.getError.getCode == 121
    case _                          => false
  }

  private val outboxPayload = Filters.exists(MongoFields.Payload)
  private val workPayload = Filters.exists(s"${MongoFields.Event}.${MongoFields.Payload}")

  private def event(actor: UserId): OperationalEventEnvelope = {
    val id = UUID.randomUUID()
    OperationalEventEnvelope(
      UUID.randomUUID(),
      OperationalEventType.JOB_CREATED,
      now,
      OperationalAggregateType.Job,
      id.toString,
      actor,
      OperationalEventPayload.JobFact(
        OperationalEventPayload.JobKind.Created,
        OperationalEventPayload.JobSnapshot(id, List("Scala"), JobStatus.Open)
      )
    )
  }

  private def legacyOutbox(value: OperationalEventEnvelope): Document =
    MongoHiringCodecs
      .outboxRecord(value, now)
      .fold(message => fail(message), identity)
      .append(MongoFields.Payload, OperationalEventWireCodec.payloadJson(value.payload).noSpaces)

  private def legacyWork(actor: UserId): Document = {
    val session = SearchSession(
      UUID.randomUUID(),
      actor,
      SearchKind.Jobs,
      None,
      SearchFilterSnapshot.Unfiltered,
      None,
      Nil,
      now,
      now.plusSeconds(3600)
    )
    val value =
      OperationalEvents.searchPerformed(UUID.randomUUID(), session).fold(error => fail(error.toString), identity)
    // The shape of a database at 025: the work row still stores its event, with the `payload` copy (migration 031 later
    // removes the event itself).
    MongoSearchSessionWorkCodecs
      .work(PendingSearchSessionWork(session), now)
      .append(
        MongoFields.Event,
        new Document(MongoFields.Id, value.eventId.toString)
          .append(MongoFields.EnvelopeBytes, new Binary(OperationalEventWireCodec.bytes(value)))
          .append(MongoFields.Payload, OperationalEventWireCodec.payloadJson(value.payload).noSpaces)
      )
  }

  private def insertUnvalidated(database: MongoDatabase[IO], name: String, rows: List[Document]): IO[Unit] =
    collection(database, name).flatMap(_.insertMany(rows, new InsertManyOptions().bypassDocumentValidation(true))).void

  /** A database that completed 025 under the earlier binary: the earlier validators, no 026 proof. The single-copy step
    * of search session work (031) rebuilds the validator 026 installs, so it has not completed either.
    */
  private def rewindTo025(database: MongoDatabase[IO]): IO[Unit] =
    for {
      setup <- MongoHiringSetup.setupDatabase(database)
      _ <- MongoHiringValidators.install(setup, Outbox, MongoHiringValidators.outboxValidatorV1)
      _ <- MongoHiringValidators.install(setup, Work, MongoOperationalValidators.searchSessionWorkValidatorThrough025)
      rows <- collection(database, MongoCollections.HiringMigrationLedger)
      _ <- rows.deleteMany(
        Filters.in(MongoFields.Id, Source.value, MigrationIds.SearchSessionWorkSingleCopy.value)
      )
    } yield ()

  private def successorInstalled(database: MongoDatabase[IO]): IO[Boolean] =
    MongoHiringSetup.setupDatabase(database).flatMap { setup =>
      (MongoOperationalValidators.all :+ (Outbox -> MongoHiringValidators.outboxValidator)).forallM {
        case (name, expected) => MongoHiringValidators.validatorMatches(setup, name, expected)
      }
    }

  private def seed(database: MongoDatabase[IO], events: Int, works: Int): IO[Unit] = {
    val actor = UserId(UUID.randomUUID())
    insertUnvalidated(database, Outbox, List.fill(events)(legacyOutbox(event(actor)))) *>
      insertUnvalidated(database, Work, List.fill(works)(legacyWork(actor)))
  }

  private def assertSlim(database: MongoDatabase[IO], events: Long, works: Long): IO[Unit] =
    for {
      outboxPending <- count(database, Outbox, outboxPayload)
      workPending <- count(database, Work, workPayload)
      outbox <- count(database, Outbox)
      work <- count(database, Work)
      rows <- collection(database, Outbox).flatMap(_.find(Filters.empty()).limit(50).all)
      workRows <- collection(database, Work).flatMap(_.find(Filters.empty()).limit(50).all)
    } yield {
      assertEquals(outboxPending, 0L)
      assertEquals(workPending, 0L)
      assertEquals(outbox, events)
      assertEquals(work, works)
      assert(rows.forall(row => MongoHiringCodecs.readOperationalEvent(row).isValid), clue(rows))
      assert(workRows.forall(row => MongoSearchSessionWorkCodecs.readWork(row).isValid), clue(workRows))
    }

  test("MSC-06.1 a published event row has the envelope and the query fields but no payload") {
    mongoResource.use { fixture =>
      val actor = UserId(UUID.randomUUID())
      val value = event(actor)
      val changed = value.copy(payload =
        OperationalEventPayload.JobFact(
          OperationalEventPayload.JobKind.Created,
          OperationalEventPayload.JobSnapshot(UUID.fromString(value.aggregateId), List("Cats"), JobStatus.Open)
        )
      )
      val sessions = MongoSearchSessionRepository(fixture.database, Diagnostics.noop[IO])
      val outbox =
        MongoOperationalEventOutboxRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
      def row =
        MongoRepositoryTestSupport.findOne(fixture.database, Outbox, Filters.eq(MongoFields.Id, value.eventId.toString))
      for {
        _ <- init(fixture.database)
        first <- sessions.record(value).value
        again <- sessions.record(value).value
        different <- sessions.record(changed).value
        stored <- row
        claimed <- outbox
          .claim(
            "envelope-source",
            ProducerGenerationFixtures.of(s"hiring-publisher-${UUID.randomUUID()}"),
            now,
            30.seconds,
            10
          )
          .value
        token = claimed.toOption.flatMap(_.headOption).map(_.leaseToken).getOrElse("")
        published <- outbox.markPublished(value.eventId, token, now, now.plusSeconds(3600)).value
        after <- row
      } yield {
        assertEquals(first, Right(true))
        assertEquals(again, Right(false))
        assertEquals(different, Left(RepositoryError.Conflict))
        val document = stored.getOrElse(fail("outbox row is missing"))
        assert(!document.containsKey(MongoFields.Payload), clue(document))
        assert(document.get(MongoFields.EnvelopeBytes).isInstanceOf[Binary], clue(document))
        List(
          MongoFields.State,
          MongoFields.AvailableAt,
          MongoFields.EventType,
          MongoFields.AggregateType,
          MongoFields.AggregateId,
          MongoFields.ActorId,
          MongoFields.OccurredAt,
          MongoFields.Topic,
          MongoFields.PartitionKey,
          MongoFields.SubjectIds,
          MongoFields.SubjectRefsVersion
        ).foreach(field => assert(document.containsKey(field), clue(field)))
        assertEquals(claimed.map(_.map(_.event)), Right(List(value)))
        assertEquals(
          claimed.map(_.map(_.envelopeBytes.toVector)),
          Right(List(OperationalEventWireCodec.bytes(value).toVector))
        )
        assertEquals(published, Right(()))
        assertEquals(after.map(_.getString(MongoFields.State)), Some("Published"))
        assert(after.exists(!_.containsKey(MongoFields.Payload)))
      }
    }
  }

  test("MSC-06.1 the outbox and search-session-work validators reject a stored payload") {
    mongoResource.use { fixture =>
      val actor = UserId(UUID.randomUUID())
      for {
        _ <- init(fixture.database)
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        // The work definition 026 installed; migration 031 later replaces it with one that has no stored event.
        _ <- MongoHiringValidators.install(setup, Work, MongoOperationalValidators.searchSessionWorkValidatorThrough026)
        outbox <- MongoRepositoryTestSupport
          .insertOne(fixture.database, Outbox, legacyOutbox(event(actor)))
          .attempt
        work <- MongoRepositoryTestSupport.insertOne(fixture.database, Work, legacyWork(actor)).attempt
        slim <- MongoRepositoryTestSupport
          .insertOne(
            fixture.database,
            Outbox,
            MongoHiringCodecs.outboxRecord(event(actor), now).fold(message => fail(message), identity)
          )
          .attempt
        lease <- MongoRepositoryTestSupport
          .insertOne(
            fixture.database,
            Outbox,
            MongoHiringCodecs
              .outboxRecord(event(actor), now)
              .fold(message => fail(message), identity)
              .append(MongoFields.LeaseOwner, null)
          )
          .attempt
      } yield {
        assert(rejected(outbox), clue(outbox))
        assert(rejected(work), clue(work))
        assert(slim.isRight, clue(slim))
        assert(rejected(lease), clue(lease))
      }
    }
  }

  test("MSC-10.1 migration 026 removes payload from a database at 025 and writes nothing on restart") {
    mongoResource.use { fixture =>
      val events = 1_100
      val works = 3
      for {
        _ <- init(fixture.database)
        _ <- rewindTo025(fixture.database)
        _ <- seed(fixture.database, events, works)
        _ <- init(fixture.database)
        done <- sourceRow(fixture.database)
        installed <- successorInstalled(fixture.database)
        _ <- assertSlim(fixture.database, events.toLong, works.toLong)
        before <- ledger(fixture.database)
        _ <- init(fixture.database)
        after <- ledger(fixture.database)
        stillInstalled <- successorInstalled(fixture.database)
        _ <- assertSlim(fixture.database, events.toLong, works.toLong)
      } yield {
        assertEquals(done.map(_.getString(MongoFields.State)), Some("Complete"))
        assertEquals(done.map(_.containsKey(MongoFields.LastId)), Some(false))
        assert(installed)
        assert(stillInstalled)
        assertEquals(after.toSet, before.toSet)
      }
    }
  }

  test("MSC-10.1 two processes starting a database at 025 converge on one slim and validated shape") {
    mongoResource.use { fixture =>
      for {
        _ <- init(fixture.database)
        _ <- rewindTo025(fixture.database)
        _ <- seed(fixture.database, 600, 2)
        _ <- (init(fixture.database), init(fixture.database)).parTupled
        installed <- successorInstalled(fixture.database)
        rows <- ledger(fixture.database)
        _ <- assertSlim(fixture.database, 600L, 2L)
      } yield {
        assert(installed)
        assert(rows.forall(_.getString(MongoFields.State) == "Complete"), clue(rows))
        assert(rows.exists(_.getString(MongoFields.Id) == Source.value))
      }
    }
  }

  test("MSC-10.1 migration 026 fails closed with a checkpoint on an unreadable row and resumes once it is gone") {
    mongoResource.use { fixture =>
      val actor = UserId(UUID.randomUUID())
      val unreadable = legacyOutbox(event(actor))
      val _ = unreadable.remove(MongoFields.EnvelopeBytes)
      val unreadableId = unreadable.getString(MongoFields.Id)
      for {
        _ <- init(fixture.database)
        _ <- rewindTo025(fixture.database)
        _ <- seed(fixture.database, 600, 1)
        _ <- insertUnvalidated(fixture.database, Outbox, List(unreadable))
        failed <- init(fixture.database).attempt
        running <- sourceRow(fixture.database)
        installedWhileInvalid <- MongoHiringSetup
          .setupDatabase(fixture.database)
          .flatMap(
            MongoHiringValidators.validatorMatches(_, Outbox, MongoHiringValidators.outboxValidator)
          )
        pending <- count(fixture.database, Outbox, outboxPayload)
        _ <- collection(fixture.database, Outbox).flatMap(_.deleteOne(Filters.eq(MongoFields.Id, unreadableId)))
        _ <- init(fixture.database)
        done <- sourceRow(fixture.database)
        installed <- successorInstalled(fixture.database)
        _ <- assertSlim(fixture.database, 600L, 1L)
      } yield {
        assertEquals(failed, Left(MigrationError.StepFailed(Source, s"found invalid stored data in $Outbox")))
        assertEquals(running.map(_.getString(MongoFields.State)), Some("Running"))
        assert(running.exists(_.getString(MongoFields.LastId).startsWith("01:")), clue(running))
        assertEquals(pending, 0L)
        assert(!installedWhileInvalid)
        assertEquals(done.map(_.getString(MongoFields.State)), Some("Complete"))
        assert(installed)
      }
    }
  }

  test("MSC-10.1 a completed migration 026 reopens when an earlier binary stores a payload again") {
    mongoResource.use { fixture =>
      val actor = UserId(UUID.randomUUID())
      val late = legacyOutbox(event(actor))
      val lateWork = legacyWork(actor)
      for {
        _ <- init(fixture.database)
        // Both validators reject `payload`, so the earlier binary's write is simulated past validation.
        _ <- insertUnvalidated(fixture.database, Outbox, List(late))
        _ <- insertUnvalidated(fixture.database, Work, List(lateWork))
        _ <- init(fixture.database)
        outbox <- count(fixture.database, Outbox, outboxPayload)
        work <- count(fixture.database, Work, workPayload)
        state <- sourceRow(fixture.database)
        installed <- successorInstalled(fixture.database)
      } yield {
        assertEquals(outbox, 0L)
        assertEquals(work, 0L)
        assertEquals(state.map(_.getString(MongoFields.State)), Some("Complete"))
        assert(installed)
      }
    }
  }

  test("MSC-10.1 drift of the successor validators behind the completed migration 026 fails closed") {
    mongoResource.use { fixture =>
      for {
        _ <- init(fixture.database)
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        _ <- setup.runCommand(new Document("collMod", Work).append("validator", new Document()))
        drifted <- init(fixture.database).attempt
      } yield assertEquals(drifted, Left(MigrationError.ValidatorMismatch(Work)))
    }
  }

  test("MSC-06.1 the proof of migration 013 accepts the outbox validator V1 or V2 and any other definition is drift") {
    mongoResource.use { fixture =>
      for {
        _ <- init(fixture.database)
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        trustedV2 <- MongoWorkflowIntegrityMigrations.trusted(setup)
        _ <- MongoHiringValidators.install(setup, Outbox, MongoHiringValidators.outboxValidatorV1)
        trustedV1 <- MongoWorkflowIntegrityMigrations.trusted(setup)
        _ <- setup.runCommand(new Document("collMod", Outbox).append("validator", new Document()))
        drifted <- MongoWorkflowIntegrityMigrations.trusted(setup).attempt
      } yield {
        assert(trustedV2)
        assert(trustedV1)
        assertEquals(drifted, Left(MigrationError.ValidatorMismatch(Outbox)))
      }
    }
  }
}
