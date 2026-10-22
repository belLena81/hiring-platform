package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.InterviewLifecycleFixtures
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, UserId}
import com.example.graphQL.cats.domain.model.{ApplicationEvent, ApplicationStatus, UserRole}
import com.example.graphQL.cats.domain.workflow.InterviewWorkflowId
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.client.model.{Filters, IndexOptions, Indexes}
import mongo4cats.database.MongoDatabase
import org.bson.Document
import org.bson.conversions.Bson

import java.time.Instant
import java.util.{Date, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Ledger steps 029 to 034: each retires a stored shape (hiring receipts, request receipts, the event copy of a search
  * session work row, the kind and entity of an embedding work row, an application event or interview workflow without
  * its actor's role). Every step is exercised the way an operator meets it: upgrade from 025, restart, concurrent
  * start, fail closed with a checkpoint, resume and reopen.
  */
final class MongoStoredShapeRemovalIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 15.minutes

  private val now = Instant.parse("2026-10-20T10:00:00Z")
  private val Retired = List(
    MigrationIds.InterviewHiringReceiptRemoval,
    MigrationIds.InterviewRequestReceiptRemoval,
    MigrationIds.SearchSessionWorkSingleCopy,
    MigrationIds.EmbeddingWorkKeyFromIdentity,
    MigrationIds.ApplicationEventActorRole,
    MigrationIds.InterviewWorkflowInitiatorRole
  )

  private def init(database: MongoDatabase[IO]): IO[Unit] = MongoHiringSetup.initialize(database, Diagnostics.noop[IO])

  private def documents(database: MongoDatabase[IO], name: String) = Mongo4catsCollections.documents(database, name)

  private def count(database: MongoDatabase[IO], name: String, filter: Bson = Filters.empty()): IO[Long] =
    MongoRepositoryTestSupport.count(database, name, filter)

  private def ledgerRow(database: MongoDatabase[IO], id: MigrationId): IO[Option[Document]] =
    MongoRepositoryTestSupport.findOne(
      database,
      MongoCollections.HiringMigrationLedger,
      Filters.eq(MongoFields.Id, id.value)
    )

  private def indexNames(database: MongoDatabase[IO], name: String): IO[Set[String]] =
    documents(database, name).flatMap(_.listIndexes[Document]).map(_.map(_.getString("name")).toSet)

  private def installed(database: MongoDatabase[IO], name: String, expected: Document): IO[Boolean] =
    MongoHiringSetup.setupDatabase(database).flatMap(MongoHiringValidators.validatorMatches(_, name, expected))

  /** The database an upgrade from 025 finds: the generation of validators 025 installed (the users definition stays
    * current, it is installed at every start), the two receipt indexes, and no proof of the later steps that rebuild a
    * validator installed here (the outbox envelope step 026 and the six steps under test).
    */
  private def atTwentyFive(database: MongoDatabase[IO]): IO[Unit] =
    for {
      _ <- init(database)
      setup <- MongoHiringSetup.setupDatabase(database)
      _ <- MongoOperationalValidators.installedBy025.filterNot(_._1 == MongoCollections.Users).traverse_ {
        case (name, validator) => MongoHiringValidators.install(setup, name, validator)
      }
      inbox <- documents(database, MongoCollections.InterviewWorkflowInbox)
      _ <- inbox.createIndex(
        Indexes.ascending(MongoFields.SubjectIds),
        new IndexOptions()
          .name(MongoIndexNames.InterviewWorkflowInboxHiringSubjects)
          .partialFilterExpression(Filters.eq(MongoFields.DocumentType, "hiringReceipt"))
      )
      workflows <- documents(database, MongoCollections.InterviewWorkflows)
      _ <- workflows.createIndex(
        Indexes.ascending("requestWorkflowId"),
        new IndexOptions().name(MongoIndexNames.InterviewWorkflowRequestReceipt).sparse(true)
      )
      ledger <- documents(database, MongoCollections.HiringMigrationLedger)
      _ <- ledger.deleteMany(
        Filters.in(MongoFields.Id, (MigrationIds.OutboxEnvelopeSource :: Retired).map(_.value)*)
      )
    } yield ()

  private def dataWrites(names: Set[String])(fixture: MongoAccessEvaluationSupport.Fixture): IO[List[String]] =
    fixture.commands.snapshot.map(
      _.filter(command =>
        Set("update", "insert", "delete").contains(command.getFirstKey) &&
          names.contains(command.getString(command.getFirstKey).getValue)
      ).map(_.getFirstKey)
    )

  // ---------------------------------------------------------------------------------------------------------------
  // 029 and 030: a document type disappears
  // ---------------------------------------------------------------------------------------------------------------

  private final case class DocumentTypeRemoval(
      label: String,
      id: MigrationId,
      collection: String,
      retiredType: String,
      index: String,
      successor: Document,
      retired: Int => Document,
      unrecognised: Document,
      survivor: Document
  ) {
    def retiredFilter: Bson = Filters.eq(MongoFields.DocumentType, retiredType)
  }

  private val hiringReceipts = DocumentTypeRemoval(
    "hiring receipts",
    MigrationIds.InterviewHiringReceiptRemoval,
    MongoCollections.InterviewWorkflowInbox,
    "hiringReceipt",
    MongoIndexNames.InterviewWorkflowInboxHiringSubjects,
    MongoOperationalValidators.interviewWorkflowInboxValidator,
    index => {
      val workflow = UUID.nameUUIDFromBytes(s"workflow-$index".getBytes).toString
      new Document(MongoFields.Id, s"$workflow:hiring")
        .append(MongoFields.DocumentType, "hiringReceipt")
        .append("workflowId", workflow)
        .append(
          MongoFields.SubjectIds,
          List(UUID.randomUUID().toString, UUID.randomUUID().toString).asJava
        )
        .append(MongoFields.OccurredAt, Date.from(now))
    },
    new Document(MongoFields.Id, "~unrecognised")
      .append(MongoFields.DocumentType, "hiringReceipt")
      .append("workflowId", UUID.randomUUID().toString)
      .append(MongoFields.SubjectIds, List(UUID.randomUUID().toString).asJava)
      .append(MongoFields.OccurredAt, Date.from(now)),
    MongoInterviewWorkflowCodec.inboxDocument(
      "survivor:message",
      InterviewWorkflowId(UUID.randomUUID()),
      "message",
      1L,
      now
    )
  )

  private val requestReceipts = DocumentTypeRemoval(
    "request receipts",
    MigrationIds.InterviewRequestReceiptRemoval,
    MongoCollections.InterviewWorkflows,
    "requestReceipt",
    MongoIndexNames.InterviewWorkflowRequestReceipt,
    MongoOperationalValidators.interviewWorkflowsValidator,
    index =>
      new Document(
        MongoFields.Id,
        s"request:${UUID.nameUUIDFromBytes(s"actor-$index".getBytes)}:${UUID.nameUUIDFromBytes(s"key-$index".getBytes)}"
      )
        .append(MongoFields.DocumentType, "requestReceipt")
        .append("requestWorkflowId", UUID.randomUUID().toString)
        .append("requestFingerprint", s"fingerprint-$index")
        .append(MongoFields.CreatedAt, Date.from(now)),
    new Document(MongoFields.Id, "~unrecognised")
      .append(MongoFields.DocumentType, "requestReceipt")
      .append("requestWorkflowId", UUID.randomUUID().toString)
      .append("requestFingerprint", "f")
      .append(MongoFields.CreatedAt, Date.from(now)),
    MongoInterviewWorkflowCodec.workflowDocument(InterviewLifecycleFixtures.completed)
  )

  List(hiringReceipts, requestReceipts).foreach { shape =>
    test(s"AC-04.2 migration ${shape.id.value} removes every ${shape.label} in batches and nothing else") {
      mongoResource.use { fixture =>
        val database = fixture.database
        val receipts = (0 until 1100).toList.map(shape.retired)
        for {
          _ <- atTwentyFive(database)
          before <- indexNames(database, shape.collection)
          _ <- documents(database, shape.collection).flatMap(_.insertMany(shape.survivor :: receipts))
          _ <- init(database)
          remaining <- count(database, shape.collection, shape.retiredFilter)
          survivor <- MongoRepositoryTestSupport.findOne(
            database,
            shape.collection,
            Filters.eq(MongoFields.Id, shape.survivor.get(MongoFields.Id))
          )
          after <- indexNames(database, shape.collection)
          current <- installed(database, shape.collection, shape.successor)
          row <- ledgerRow(database, shape.id)
        } yield {
          assert(before.contains(shape.index), clue(before))
          assertEquals(remaining, 0L)
          assertEquals(survivor, Some(shape.survivor))
          assert(!after.contains(shape.index), clue(after))
          assert(current, "the successor validator is installed")
          assertEquals(row.map(_.getString(MongoFields.State)), Some("Complete"))
          assertEquals(row.map(_.containsKey(MongoFields.LastId)), Some(false))
        }
      }
    }

    test(s"AC-04.2 migration ${shape.id.value} writes nothing on restart and converges under a concurrent start") {
      mongoResource.use { fixture =>
        val database = fixture.database
        for {
          _ <- atTwentyFive(database)
          _ <- documents(database, shape.collection).flatMap(_.insertMany((0 until 700).toList.map(shape.retired)))
          _ <- (init(database), init(database)).parTupled
          remaining <- count(database, shape.collection, shape.retiredFilter)
          rows <- documents(database, MongoCollections.HiringMigrationLedger).flatMap(_.find(Filters.empty()).all)
          before <- indexNames(database, shape.collection)
          _ <- fixture.commands.clear
          _ <- init(database)
          writes <- dataWrites(Set(MongoCollections.HiringMigrationLedger, shape.collection))(fixture)
          after <- indexNames(database, shape.collection)
          current <- installed(database, shape.collection, shape.successor)
        } yield {
          assertEquals(remaining, 0L)
          assert(rows.forall(_.getString(MongoFields.State) == "Complete"), clue(rows))
          assertEquals(writes, Nil)
          assertEquals(after, before)
          assert(current)
        }
      }
    }

    test(s"AC-04.2 migration ${shape.id.value} fails closed on an unrecognised row, resumes from its checkpoint") {
      mongoResource.use { fixture =>
        val database = fixture.database
        val receipts = (0 until 600).toList.map(shape.retired)
        for {
          _ <- atTwentyFive(database)
          _ <- documents(database, shape.collection).flatMap(_.insertMany(shape.unrecognised :: receipts))
          first <- init(database).attempt
          running <- ledgerRow(database, shape.id)
          second <- init(database).attempt
          _ <- documents(database, shape.collection).flatMap(
            _.deleteOne(Filters.eq(MongoFields.Id, shape.unrecognised.get(MongoFields.Id)))
          )
          repaired <- init(database).attempt
          done <- ledgerRow(database, shape.id)
          remaining <- count(database, shape.collection, shape.retiredFilter)
        } yield {
          val expected =
            MigrationError.StepFailed(shape.id, s"found an unrecognised row in ${shape.collection}")
          assertEquals(first, Left(expected))
          assertEquals(second, Left(expected))
          assertEquals(running.map(_.getString(MongoFields.State)), Some("Running"))
          // The first full page was removed and checkpointed before the page holding the unrecognised row failed.
          assert(running.exists(_.containsKey(MongoFields.LastId)), clue(running))
          assertEquals(repaired, Right(()))
          assertEquals(done.map(_.getString(MongoFields.State)), Some("Complete"))
          assertEquals(remaining, 0L)
        }
      }
    }

    test(s"AC-04.2 migration ${shape.id.value} reopens when an earlier binary stores the retired shape again") {
      mongoResource.use { fixture =>
        val database = fixture.database
        for {
          _ <- atTwentyFive(database)
          _ <- init(database)
          // A writer that still runs the earlier binary: the validator is bypassed to represent its stored shape.
          _ <- MongoRepositoryTestSupport.insertUnvalidated(database, shape.collection, shape.retired(1))
          rejected <- MongoRepositoryTestSupport
            .insertOne(database, shape.collection, shape.retired(2))
            .attempt
          _ <- init(database)
          remaining <- count(database, shape.collection, shape.retiredFilter)
          state <- ledgerRow(database, shape.id)
        } yield {
          assert(rejected.isLeft, "the successor validator refuses the retired shape")
          assertEquals(remaining, 0L)
          assertEquals(state.map(_.getString(MongoFields.State)), Some("Complete"))
        }
      }
    }
  }

  test("AC-04.2 migration 030 leaves mutation receipts, workflows and the other inbox types alone") {
    mongoResource.use { fixture =>
      val database = fixture.database
      val receipt = new Document(MongoFields.Id, UUID.randomUUID().toString)
        .append(MongoFields.Operation, "interviewWorkflow")
        .append(MongoFields.ActorScope, UUID.randomUUID().toString)
        .append(MongoFields.IdempotencyKey, UUID.randomUUID().toString)
        .append(MongoFields.Fingerprint, "f")
        .append(MongoFields.State, "Completed")
        .append(MongoFields.CreatedAt, Date.from(now))
        .append(MongoFields.ExpiresAt, Date.from(now.plusSeconds(3600)))
      for {
        _ <- atTwentyFive(database)
        _ <- MongoRepositoryTestSupport.insertOne(database, MongoCollections.MutationReceipts, receipt)
        _ <- documents(database, MongoCollections.InterviewWorkflows)
          .flatMap(_.insertMany(List(requestReceipts.survivor, requestReceipts.retired(1))))
        _ <- init(database)
        receipts <- count(database, MongoCollections.MutationReceipts)
        workflows <- count(database, MongoCollections.InterviewWorkflows)
      } yield {
        assertEquals(receipts, 1L)
        assertEquals(workflows, 1L)
      }
    }
  }

  test("AC-04.2 migration 019 still creates its index on a database before it, and 030 retires it in the same start") {
    mongoResource.use { fixture =>
      val database = fixture.database
      for {
        _ <- atTwentyFive(database)
        workflows <- documents(database, MongoCollections.InterviewWorkflows)
        _ <- workflows.dropIndex(MongoIndexNames.InterviewWorkflowRequestReceipt)
        ledger <- documents(database, MongoCollections.HiringMigrationLedger)
        _ <- ledger.deleteMany(Filters.eq(MongoFields.Id, MigrationIds.InterviewRequestReceiptIndex.value))
        _ <- init(database)
        after <- indexNames(database, MongoCollections.InterviewWorkflows)
        row019 <- ledgerRow(database, MigrationIds.InterviewRequestReceiptIndex)
        row030 <- ledgerRow(database, MigrationIds.InterviewRequestReceiptRemoval)
      } yield {
        assert(!after.contains(MongoIndexNames.InterviewWorkflowRequestReceipt), clue(after))
        assertEquals(row019.map(_.getString(MongoFields.State)), Some("Complete"))
        assertEquals(row030.map(_.getString(MongoFields.State)), Some("Complete"))
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // 031 and 032: redundant fields of a work row disappear
  // ---------------------------------------------------------------------------------------------------------------

  private final case class FieldRemoval(
      label: String,
      id: MigrationId,
      collection: String,
      fields: List[String],
      successor: Document,
      row: Int => Document
  ) {
    def leftover: Bson = Filters.or(fields.map(Filters.exists(_, true))*)
  }

  private def searchWorkRow(index: Int): Document = {
    val session = com.example.graphQL.cats.service.events.SearchSession(
      UUID.nameUUIDFromBytes(s"search-$index".getBytes),
      com.example.graphQL.cats.domain.model.Identifiers.UserId(UUID.nameUUIDFromBytes(s"actor-$index".getBytes)),
      com.example.graphQL.cats.service.events.OperationalEventPayload.SearchKind.Jobs,
      None,
      com.example.graphQL.cats.service.events.SearchFilterSnapshot.Unfiltered,
      None,
      Nil,
      now,
      now.plusSeconds(3600)
    )
    val work = MongoSearchSessionWorkCodecs.work(
      com.example.graphQL.cats.service.port.PendingSearchSessionWork(session),
      now
    )
    work.append(
      MongoFields.Event,
      new Document(MongoFields.Id, UUID.randomUUID().toString)
        .append(MongoFields.Payload, "{}")
        .append(MongoFields.EnvelopeBytes, new org.bson.types.Binary(Array[Byte](1, 2, 3)))
    )
  }

  private def embeddingWorkRow(index: Int): Document = {
    val entity = UUID.nameUUIDFromBytes(s"entity-$index".getBytes).toString
    new Document(MongoFields.Id, s"Job:$entity")
      .append(MongoFields.Kind, "Job")
      .append(MongoFields.WorkEntityId, entity)
      .append(MongoFields.State, "Ready")
      .append(MongoFields.Generation, Long.box(1L))
      .append(MongoFields.Attempts, Int.box(0))
      .append(MongoFields.AvailableAt, Date.from(now))
      .append(MongoFields.CreatedAt, Date.from(now))
      .append(MongoFields.UpdatedAt, Date.from(now))
  }

  private val searchWork = FieldRemoval(
    "the event copy of search session work",
    MigrationIds.SearchSessionWorkSingleCopy,
    MongoCollections.SearchSessionWork,
    List(MongoFields.Event),
    MongoOperationalValidators.searchSessionWorkValidator,
    searchWorkRow
  )

  private val embeddingWork = FieldRemoval(
    "the kind and entity of embedding work",
    MigrationIds.EmbeddingWorkKeyFromIdentity,
    MongoCollections.EmbeddingWork,
    List(MongoFields.Kind, MongoFields.WorkEntityId),
    MongoOperationalValidators.embeddingWorkValidator,
    embeddingWorkRow
  )

  List(searchWork, embeddingWork).foreach { shape =>
    test(s"AC-05.1 migration ${shape.id.value} removes ${shape.label} in batches and keeps every other field") {
      mongoResource.use { fixture =>
        val database = fixture.database
        val rows = (0 until 1100).toList.map(shape.row)
        for {
          _ <- atTwentyFive(database)
          _ <- documents(database, shape.collection).flatMap(_.insertMany(rows))
          _ <- init(database)
          leftover <- count(database, shape.collection, shape.leftover)
          total <- count(database, shape.collection)
          sample <- MongoRepositoryTestSupport.findOne(
            database,
            shape.collection,
            Filters.eq(MongoFields.Id, shape.row(7).get(MongoFields.Id))
          )
          current <- installed(database, shape.collection, shape.successor)
          row <- ledgerRow(database, shape.id)
        } yield {
          assertEquals(leftover, 0L)
          assertEquals(total, 1100L)
          val expected = shape.row(7)
          shape.fields.foreach(expected.remove)
          assertEquals(sample, Some(expected))
          assert(current)
          assertEquals(row.map(_.getString(MongoFields.State)), Some("Complete"))
        }
      }
    }

    test(s"AC-05.1 migration ${shape.id.value} writes nothing on restart and converges under a concurrent start") {
      mongoResource.use { fixture =>
        val database = fixture.database
        for {
          _ <- atTwentyFive(database)
          _ <- documents(database, shape.collection).flatMap(_.insertMany((0 until 700).toList.map(shape.row)))
          _ <- (init(database), init(database)).parTupled
          leftover <- count(database, shape.collection, shape.leftover)
          _ <- fixture.commands.clear
          _ <- init(database)
          writes <- dataWrites(Set(MongoCollections.HiringMigrationLedger, shape.collection))(fixture)
        } yield {
          assertEquals(leftover, 0L)
          assertEquals(writes, Nil)
        }
      }
    }

    test(s"AC-05.1 migration ${shape.id.value} reopens when an earlier binary writes the redundant fields again") {
      mongoResource.use { fixture =>
        val database = fixture.database
        for {
          _ <- atTwentyFive(database)
          _ <- init(database)
          _ <- MongoRepositoryTestSupport.insertUnvalidated(database, shape.collection, shape.row(1))
          _ <- init(database)
          leftover <- count(database, shape.collection, shape.leftover)
        } yield assertEquals(leftover, 0L)
      }
    }
  }

  test("AC-05.1 a database that never held the retired shapes finishes the six steps without any data write") {
    mongoResource.use { fixture =>
      val database = fixture.database
      for {
        _ <- atTwentyFive(database)
        _ <- fixture.commands.clear
        _ <- init(database)
        writes <- dataWrites(
          Set(
            MongoCollections.InterviewWorkflowInbox,
            MongoCollections.InterviewWorkflows,
            MongoCollections.SearchSessionWork,
            MongoCollections.EmbeddingWork,
            MongoCollections.ApplicationEvents
          )
        )(fixture)
        rows <- Retired.traverse(ledgerRow(database, _))
      } yield {
        assertEquals(writes, Nil)
        assert(rows.forall(_.exists(_.getString(MongoFields.State) == "Complete")), clue(rows))
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // 033: an application event always records the role its actor acted in
  // ---------------------------------------------------------------------------------------------------------------

  private def eventDocument(index: Int, withRole: Boolean): Document = {
    val document = MongoHiringCodecs.event(
      ApplicationEvent(
        ApplicationEventId(UUID.nameUUIDFromBytes(s"event-$index".getBytes)),
        ApplicationId(UUID.nameUUIDFromBytes(s"application-$index".getBytes)),
        None,
        ApplicationStatus.Created,
        UserId(UUID.nameUUIDFromBytes(s"actor-$index".getBytes)),
        now,
        None,
        None,
        UserRole.Candidate
      )
    )
    if (!withRole) { val _ = document.remove(MongoFields.ActorRole) }
    document
  }

  private val withoutRole: Bson = Filters.exists(MongoFields.ActorRole, false)

  test("AC-09.4 migration 033 installs the successor validator over events that all record a role, writing nothing") {
    mongoResource.use { fixture =>
      val database = fixture.database
      for {
        _ <- atTwentyFive(database)
        _ <- documents(database, MongoCollections.ApplicationEvents)
          .flatMap(_.insertMany((0 until 1100).toList.map(eventDocument(_, withRole = true))))
        before <- installed(
          database,
          MongoCollections.ApplicationEvents,
          MongoOperationalValidators.applicationEventsValidatorThrough025
        )
        _ <- fixture.commands.clear
        _ <- init(database)
        writes <- dataWrites(Set(MongoCollections.ApplicationEvents))(fixture)
        current <- installed(
          database,
          MongoCollections.ApplicationEvents,
          MongoOperationalValidators.applicationEventsValidator
        )
        total <- count(database, MongoCollections.ApplicationEvents)
        row <- ledgerRow(database, MigrationIds.ApplicationEventActorRole)
        rejected <- MongoRepositoryTestSupport
          .insertOne(database, MongoCollections.ApplicationEvents, eventDocument(5000, withRole = false))
          .attempt
        _ <- fixture.commands.clear
        _ <- init(database)
        restartWrites <- dataWrites(Set(MongoCollections.ApplicationEvents, MongoCollections.HiringMigrationLedger))(
          fixture
        )
      } yield {
        assert(before, "the database starts with the generation 025 installed")
        assertEquals(writes, Nil)
        assert(current)
        assertEquals(total, 1100L)
        assertEquals(row.map(_.getString(MongoFields.State)), Some("Complete"))
        assert(rejected.isLeft, "the successor validator refuses an event without its role")
        assertEquals(restartWrites, Nil)
      }
    }
  }

  test("AC-09.4 migration 033 fails closed on an event without a role, rewrites nothing and completes once repaired") {
    mongoResource.use { fixture =>
      val database = fixture.database
      val rows = (0 until 600).toList.map(eventDocument(_, withRole = true))
      val orphan = eventDocument(9000, withRole = false)
      for {
        _ <- atTwentyFive(database)
        _ <- documents(database, MongoCollections.ApplicationEvents).flatMap(_.insertMany(orphan :: rows))
        first <- init(database).attempt
        second <- init(database).attempt
        stillThere <- count(database, MongoCollections.ApplicationEvents, withoutRole)
        earlier <- installed(
          database,
          MongoCollections.ApplicationEvents,
          MongoOperationalValidators.applicationEventsValidatorThrough025
        )
        _ <- documents(database, MongoCollections.ApplicationEvents)
          .flatMap(_.deleteOne(Filters.eq(MongoFields.Id, orphan.get(MongoFields.Id))))
        repaired <- init(database).attempt
        row <- ledgerRow(database, MigrationIds.ApplicationEventActorRole)
        current <- installed(
          database,
          MongoCollections.ApplicationEvents,
          MongoOperationalValidators.applicationEventsValidator
        )
      } yield {
        val expected = MigrationError.StepFailed(
          MigrationIds.ApplicationEventActorRole,
          s"found an unrecognised row in ${MongoCollections.ApplicationEvents}"
        )
        assertEquals(first, Left(expected))
        assertEquals(second, Left(expected))
        assertEquals(stillThere, 1L, "the row is neither rewritten nor removed")
        assert(earlier, "the validator is not replaced while a row would violate it")
        assertEquals(repaired, Right(()))
        assertEquals(row.map(_.getString(MongoFields.State)), Some("Complete"))
        assert(current)
      }
    }
  }

  test("AC-09.4 migration 033 reopens when an earlier binary stores an event without a role again") {
    mongoResource.use { fixture =>
      val database = fixture.database
      for {
        _ <- atTwentyFive(database)
        _ <- init(database)
        _ <- MongoRepositoryTestSupport.insertUnvalidated(
          database,
          MongoCollections.ApplicationEvents,
          eventDocument(1, withRole = false)
        )
        reopened <- init(database).attempt
        _ <- documents(database, MongoCollections.ApplicationEvents).flatMap(_.deleteMany(withoutRole))
        repaired <- init(database).attempt
        row <- ledgerRow(database, MigrationIds.ApplicationEventActorRole)
      } yield {
        assertEquals(
          reopened,
          Left(
            MigrationError.StepFailed(
              MigrationIds.ApplicationEventActorRole,
              s"found an unrecognised row in ${MongoCollections.ApplicationEvents}"
            )
          )
        )
        assertEquals(repaired, Right(()))
        assertEquals(row.map(_.getString(MongoFields.State)), Some("Complete"))
      }
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // 034: an interview workflow always records the role of its initiator
  // ---------------------------------------------------------------------------------------------------------------

  private def workflowDocument(index: Int, withRole: Boolean): Document = {
    val base = InterviewLifecycleFixtures.workflowIn(
      com.example.graphQL.cats.domain.workflow.InterviewWorkflowPhase.ReservationPending
    )
    val document = MongoInterviewWorkflowCodec.workflowDocument(
      base.copy(
        id = InterviewWorkflowId(UUID.nameUUIDFromBytes(s"workflow-role-$index".getBytes)),
        applicationId = com.example.graphQL.cats.domain.model.Identifiers.ApplicationId(
          UUID.nameUUIDFromBytes(s"application-role-$index".getBytes)
        )
      )
    )
    if (!withRole) { val _ = document.remove("initiatedByRole") }
    document
  }

  private val workflowWithoutRole: Bson =
    Filters.and(Filters.eq(MongoFields.DocumentType, "workflow"), Filters.exists("initiatedByRole", false))

  test(
    "AC-09.4 migration 034 installs the successor validator over workflows that all record a role, writing nothing"
  ) {
    mongoResource.use { fixture =>
      val database = fixture.database
      for {
        _ <- atTwentyFive(database)
        _ <- documents(database, MongoCollections.InterviewWorkflows)
          .flatMap(_.insertMany((0 until 700).toList.map(workflowDocument(_, withRole = true))))
        _ <- fixture.commands.clear
        _ <- init(database)
        writes <- dataWrites(Set(MongoCollections.InterviewWorkflows))(fixture)
        current <- installed(
          database,
          MongoCollections.InterviewWorkflows,
          MongoOperationalValidators.interviewWorkflowsValidator
        )
        row28 <- ledgerRow(database, MigrationIds.InterviewRequestReceiptRemoval)
        row32 <- ledgerRow(database, MigrationIds.InterviewWorkflowInitiatorRole)
        rejected <- MongoRepositoryTestSupport
          .insertOne(database, MongoCollections.InterviewWorkflows, workflowDocument(9000, withRole = false))
          .attempt
        _ <- fixture.commands.clear
        _ <- init(database)
        restartWrites <- dataWrites(Set(MongoCollections.InterviewWorkflows, MongoCollections.HiringMigrationLedger))(
          fixture
        )
      } yield {
        assertEquals(writes, Nil)
        assert(current)
        assertEquals(row28.map(_.getString(MongoFields.State)), Some("Complete"))
        assertEquals(row32.map(_.getString(MongoFields.State)), Some("Complete"))
        assert(rejected.isLeft, "the successor validator refuses a workflow without its initiator's role")
        assertEquals(restartWrites, Nil)
      }
    }
  }

  test(
    "AC-09.4 migration 034 fails closed on a workflow without a role, rewrites nothing and completes once repaired"
  ) {
    mongoResource.use { fixture =>
      val database = fixture.database
      val orphan = workflowDocument(9001, withRole = false)
      for {
        _ <- atTwentyFive(database)
        _ <- documents(database, MongoCollections.InterviewWorkflows)
          .flatMap(_.insertMany(orphan :: (0 until 50).toList.map(workflowDocument(_, withRole = true))))
        first <- init(database).attempt
        stillThere <- count(database, MongoCollections.InterviewWorkflows, workflowWithoutRole)
        _ <- documents(database, MongoCollections.InterviewWorkflows)
          .flatMap(_.deleteOne(Filters.eq(MongoFields.Id, orphan.get(MongoFields.Id))))
        repaired <- init(database).attempt
        row <- ledgerRow(database, MigrationIds.InterviewWorkflowInitiatorRole)
      } yield {
        assertEquals(
          first,
          Left(
            MigrationError.StepFailed(
              MigrationIds.InterviewWorkflowInitiatorRole,
              s"found an unrecognised row in ${MongoCollections.InterviewWorkflows}"
            )
          )
        )
        assertEquals(stillThere, 1L)
        assertEquals(repaired, Right(()))
        assertEquals(row.map(_.getString(MongoFields.State)), Some("Complete"))
      }
    }
  }
}
