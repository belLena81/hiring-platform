package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.JobStatus
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.InterviewWorkflowId
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.events.{
  OperationalAggregateType,
  OperationalEventEnvelope,
  OperationalEventPayload,
  OperationalEventType
}
import com.mongodb.client.model.{Filters, IndexOptions, Indexes, InsertManyOptions}
import io.circe.Json
import io.circe.parser.parse
import java.time.Instant
import java.util.{Date, UUID}
import org.bson.{BsonDocument, Document}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Interview erasure and consumer-receipt identity: every purge query is index-served, migration 022 still attributes
  * the hiring receipts of an older database (its successor 027 then removes them), and the redundant receipt index is
  * retired (migration 023).
  */
final class MongoInterviewErasureIndexIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes

  private val Collections = List(
    MongoCollections.InterviewWorkflows,
    MongoCollections.InterviewWorkflowCommands,
    MongoCollections.InterviewWorkflowInbox,
    MongoCollections.InterviewCalendarReservations,
    MongoCollections.InterviewNotificationReceipts
  )

  private def setup(database: mongo4cats.database.MongoDatabase[IO]): IO[Unit] =
    MongoHiringSetup.initialize(database, Diagnostics.noop[IO])

  /** Migration 022 alone: a full start would also run its successor 027, which deletes the receipts 022 attributes. */
  private def attribution(database: mongo4cats.database.MongoDatabase[IO]): IO[Unit] =
    MongoHiringSetup
      .setupDatabase(database)
      .flatMap(handle =>
        // The inbox validator of the database 022 ran on: the one migration 025 installed, which still knows the shape.
        MongoHiringValidators.install(
          handle,
          MongoCollections.InterviewWorkflowInbox,
          MongoOperationalValidators.interviewWorkflowInboxValidatorThrough025
        ) *> MongoMigrationRunner.run(handle, MongoInterviewSubjectAttributionMigrations.step)
      )

  private def insertMany(
      database: mongo4cats.database.MongoDatabase[IO],
      name: String,
      rows: List[Document]
  ): IO[Unit] =
    // Partial fixture rows: the purge is under test, not the stored shape the validators enforce.
    IO.whenA(rows.nonEmpty)(
      Mongo4catsCollections
        .documents(database, name)
        .flatMap(_.insertMany(rows, new InsertManyOptions().bypassDocumentValidation(true)))
        .void
    )

  private def count(database: mongo4cats.database.MongoDatabase[IO], name: String, filter: org.bson.conversions.Bson) =
    MongoRepositoryTestSupport.count(database, name, filter)

  private val now = Instant.now()

  /** Every document shape below is written through the production codecs, so the fixtures cannot drift from them. */
  private def rowsFor(subject: UserId, workflows: List[UUID]): Map[String, List[Document]] = {
    val recruiter = (id: UUID) => UserId(new UUID(id.getMostSignificantBits, id.getLeastSignificantBits ^ 1L))
    def per(f: UUID => Document): List[Document] = workflows.map(f)
    Map(
      MongoCollections.InterviewWorkflows -> (per(id =>
        MongoInterviewWorkflowCodec.workflowDocument(
          com.example.graphQL.cats.domain.InterviewLifecycleFixtures.completed.copy(
            id = InterviewWorkflowId(id),
            applicationId = com.example.graphQL.cats.domain.model.Identifiers.ApplicationId(UUID.randomUUID()),
            idempotencyKey = UUID.randomUUID(),
            candidateId = subject,
            recruiterId = recruiter(id),
            initiatedBy = recruiter(id)
          )
        )
      )),
      MongoCollections.InterviewWorkflowCommands -> per(id =>
        new Document(MongoFields.Id, s"$id:command")
          .append("workflowId", id.toString)
          .append("documentType", "command")
          .append("stepId", "command")
          .append("revision", Long.box(0L))
          .append("attempts", Int.box(0))
          .append("executionAttempts", Int.box(0))
          .append("commandState", "Pending")
          .append("availableAt", Date.from(now))
          .append("occurredAt", Date.from(now))
          .append("command", new Document("kind", "requireRepair").append("reason", "retry exhausted: provider"))
      ),
      MongoCollections.InterviewWorkflowInbox -> per(id =>
        MongoInterviewWorkflowCodec.inboxDocument(s"$id:message", InterviewWorkflowId(id), "message", 1L, now)
      ),
      MongoCollections.InterviewCalendarReservations -> per(id =>
        new Document(MongoFields.Id, id.toString)
          .append("workflowId", id.toString)
          .append("reserveKey", s"$id:reserve")
          .append("releaseKey", s"$id:release")
          .append("candidateId", subject.value.toString)
          .append("recruiterId", recruiter(id).value.toString)
          .append("participants", List(subject.value.toString, recruiter(id).value.toString).asJava)
          .append("startsAt", Date.from(now.plusSeconds(3600)))
          .append("endsAt", Date.from(now.plusSeconds(7200)))
          .append("reservedAt", Date.from(now))
      ),
      MongoCollections.InterviewNotificationReceipts -> per(id =>
        new Document(MongoFields.Id, s"$id:notify:Candidate")
          .append("workflowId", id.toString)
          .append("recipientId", subject.value.toString)
          .append("participant", "Candidate")
          .append("kind", "Scheduled")
          .append("deliveredAt", Date.from(now))
      )
    )
  }

  private def seed(
      database: mongo4cats.database.MongoDatabase[IO],
      subject: UserId,
      workflows: List[UUID]
  ): IO[Unit] =
    rowsFor(subject, workflows).toList.traverse_ { case (name, rows) => insertMany(database, name, rows) }

  private def totals(database: mongo4cats.database.MongoDatabase[IO]): IO[List[Long]] =
    Collections.traverse(count(database, _, new BsonDocument()))

  private def plan(
      fixture: MongoAccessEvaluationSupport.Fixture,
      observed: BsonDocument
  ): IO[(String, Json)] = IO.defer {
    val command = Document.parse(observed.toJson)
    List("$db", "lsid", "readConcern", "txnNumber").foreach(command.remove)
    val collection = observed.getString(observed.getFirstKey).getValue
    MongoAccessEvaluationSupport
      .command(fixture.database, new Document("explain", command).append("verbosity", "executionStats"))
      .map(result => collection -> parse(result.toJson).toOption.getOrElse(fail("Invalid explain JSON")))
  }

  private def winningPlan(explained: Json): Json =
    explained.hcursor.downField("queryPlanner").downField("winningPlan").focus.getOrElse(fail("Missing winning plan"))

  private def stages(plan: Json): Set[String] = plan.findAllByKey("stage").flatMap(_.asString).toSet
  private def indexes(plan: Json): Set[String] = plan.findAllByKey("indexName").flatMap(_.asString).toSet

  private def purgeCommands(fixture: MongoAccessEvaluationSupport.Fixture): IO[List[BsonDocument]] =
    fixture.commands.snapshot.map(
      _.filter(command =>
        Set("find", "delete").contains(command.getFirstKey) &&
          Collections.contains(command.getString(command.getFirstKey).getValue)
      )
    )

  /** Every index a purge query of the collection may legitimately use. */
  private def allowed(collection: String): Set[String] = collection match {
    case MongoCollections.InterviewWorkflows =>
      Set(
        MongoIndexNames.InterviewWorkflowCandidate,
        MongoIndexNames.InterviewWorkflowRecruiter,
        "_id_"
      )
    case MongoCollections.InterviewCalendarReservations =>
      Set(MongoIndexNames.InterviewCalendarParticipants, MongoIndexNames.InterviewCalendarReservationIdentity)
    case MongoCollections.InterviewNotificationReceipts =>
      Set(MongoIndexNames.InterviewNotificationRecipient, MongoIndexNames.workflowIdentity(collection))
    case other => Set(MongoIndexNames.workflowIdentity(other))
  }

  test(
    "MSC-02.1 every find and delete of the purge and the absence check is index-served among 5000 other workflows of 10 other users"
  ) {
    mongoResource.use { fixture =>
      val database = fixture.database
      val ana = UserId(UUID.randomUUID())
      val anaWorkflows = List.fill(3)(UUID.randomUUID())
      val cleanup = new MongoInterviewSubjectCleanup(database)
      def anaRows: IO[List[Long]] =
        rowsFor(ana, anaWorkflows).toList.traverse { case (name, _) =>
          count(
            database,
            name,
            Filters.in(MongoFields.Id, rowsFor(ana, anaWorkflows)(name).map(_.get(MongoFields.Id))*)
          )
        }
      for {
        _ <- setup(database)
        _ <- (0 until 10).toList.traverse_(_ =>
          seed(database, UserId(UUID.randomUUID()), List.fill(500)(UUID.randomUUID()))
        )
        baseline <- totals(database)
        _ <- seed(database, ana, anaWorkflows)
        before <- anaRows
        _ <- fixture.commands.clear
        absentBefore <- cleanup.absent(ana).value
        absenceReads <- purgeCommands(fixture)
        absencePlans <- absenceReads.traverse(plan(fixture, _))
        _ <- fixture.commands.clear
        _ <- cleanup.purge(ana).value.flatMap(result => IO(assert(result.isRight, clue(result))))
        purgeReads <- purgeCommands(fixture)
        purgePlans <- purgeReads.traverse(plan(fixture, _))
        absentAfter <- cleanup.absent(ana).value
        after <- anaRows
        remaining <- totals(database)
        inboxStats <- Mongo4catsCollections
          .documents(database, MongoCollections.InterviewWorkflowInbox)
          .flatMap(_ =>
            MongoAccessEvaluationSupport
              .command(database, new Document("collStats", MongoCollections.InterviewWorkflowInbox))
          )
      } yield {
        // MR-07: the hiring receipt and its erasure index are gone; the inbox is reached through its workflow id only.
        val sizes = inboxStats.get("indexSizes", classOf[Document])
        println(s"Inbox index sizes (bytes) after purge of the seeded dataset: ${sizes.toJson}")
        assert(!sizes.containsKey(MongoIndexNames.InterviewWorkflowInboxHiringSubjects), clue(sizes))
        // Ana owns commands, an inbox row, reservations and notifications for 3 workflows.
        assert(before.forall(_ > 0L), clue(before))
        assertEquals(after.sum, 0L)
        assertEquals(remaining, baseline)
        assertEquals(absentBefore, Right(false))
        assertEquals(absentAfter, Right(true))
        assertEquals(absenceReads.size, 3)
        (absencePlans ++ purgePlans).foreach { case (collection, explained) =>
          val winning = winningPlan(explained)
          assert(!stages(winning).contains("COLLSCAN"), clue((collection, winning)))
          assert(
            indexes(winning).nonEmpty && indexes(winning).subsetOf(allowed(collection)),
            clue((collection, winning))
          )
        }
        // The subject lookups name their own index: the workflow OR uses the candidate and recruiter indexes and the primary key.
        val workflowFinds = absencePlans.collect { case (MongoCollections.InterviewWorkflows, explained) => explained }
        workflowFinds.foreach(explained =>
          assertEquals(
            indexes(winningPlan(explained)),
            Set(MongoIndexNames.InterviewWorkflowCandidate, MongoIndexNames.InterviewWorkflowRecruiter)
          )
        )
        absencePlans.foreach { case (collection, explained) =>
          val stats = explained.hcursor.downField("executionStats")
          val examined = stats.get[Long]("totalDocsExamined").getOrElse(fail("Missing documents examined"))
          val keys = stats.get[Long]("totalKeysExamined").getOrElse(fail("Missing keys examined"))
          // A findOne of an existing subject reads one document, never the 5000 other users' rows.
          assert(examined <= 1L && keys <= 2L, clue((collection, examined, keys)))
          println(s"Erasure absence check $collection: documentsExamined=$examined keysExamined=$keys")
        }
      }
    }
  }

  private def inboxIds(database: mongo4cats.database.MongoDatabase[IO]): IO[List[String]] =
    Mongo4catsCollections
      .documents(database, MongoCollections.InterviewWorkflowInbox)
      .flatMap(_.find(Filters.empty()).all)
      .map(_.toList.map(_.getString(MongoFields.Id)).sorted)

  private def workflowIds(database: mongo4cats.database.MongoDatabase[IO]): IO[List[String]] =
    Mongo4catsCollections
      .documents(database, MongoCollections.InterviewWorkflows)
      .flatMap(_.find(Filters.empty()).all)
      .map(_.toList.map(_.getString(MongoFields.Id)).sorted)

  test("MSC-02.1 an Admin-initiated workflow is purged through its candidate and the Admin is not attributable") {
    mongoResource.use { fixture =>
      val database = fixture.database
      val admin = UserId(UUID.randomUUID())
      val ana = UserId(UUID.randomUUID())
      val recruiter = UserId(UUID.randomUUID())
      val id = UUID.randomUUID()
      val cleanup = new MongoInterviewSubjectCleanup(database)
      for {
        _ <- setup(database)
        _ <- seed(database, ana, List(id))
        _ <- Mongo4catsCollections
          .documents(database, MongoCollections.InterviewWorkflows)
          .flatMap(
            _.updateOne(
              Filters.eq(MongoFields.Id, id.toString),
              com.mongodb.client.model.Updates.combine(
                com.mongodb.client.model.Updates.set("initiatedBy", admin.value.toString),
                com.mongodb.client.model.Updates.set("recruiterId", recruiter.value.toString)
              )
            )
          )
        adminAbsent <- cleanup.absent(admin).value
        anaAbsent <- cleanup.absent(ana).value
        _ <- cleanup.purge(ana).value
        left <- totals(database)
      } yield {
        // Only the request receipt of ana (as initiator) and the workflow rows linked to it existed; all are gone.
        assertEquals(adminAbsent, Right(true))
        assertEquals(anaAbsent, Right(false))
        assertEquals(left, List.fill(Collections.size)(0L))
      }
    }
  }

  test("MSC-02.1 the second party of a shared workflow is purged with it and other workflows are untouched") {
    mongoResource.use { fixture =>
      val database = fixture.database
      val ana = UserId(UUID.randomUUID())
      val shared = UUID.randomUUID()
      val unrelated = UUID.randomUUID()
      val stranger = UserId(UUID.randomUUID())
      val cleanup = new MongoInterviewSubjectCleanup(database)
      for {
        _ <- setup(database)
        _ <- seed(database, ana, List(shared))
        _ <- seed(database, stranger, List(unrelated))
        // The recruiter of the shared workflow is the second party.
        recruiter = UserId(new UUID(shared.getMostSignificantBits, shared.getLeastSignificantBits ^ 1L))
        before <- cleanup.absent(recruiter).value
        _ <- cleanup.purge(recruiter).value
        left <- workflowIds(database)
        receipts <- inboxIds(database)
      } yield {
        assertEquals(before, Right(false))
        assertEquals(left.filter(_ == shared.toString), Nil)
        assert(left.contains(unrelated.toString))
        assert(receipts.forall(_.startsWith(unrelated.toString)), clue(receipts))
      }
    }
  }

  test("MSC-02.2 migration 022 rescans once when an earlier binary wrote a receipt below the saved checkpoint") {
    mongoResource.use { fixture =>
      val database = fixture.database
      val ana = UUID.randomUUID().toString
      val bob = UUID.randomUUID().toString
      for {
        _ <- setup(database)
        _ <- insertMany(
          database,
          MongoCollections.InterviewWorkflowInbox,
          List(legacyReceipt("00000000-0000-0000-0000-00000000000a", ana, bob))
        )
        _ <- dropLedger(database, MigrationIds.InterviewSubjectAttribution)
        handle <- MongoHiringSetup.setupDatabase(database)
        _ <- MongoMigrationLedger.markRunning(handle, MigrationIds.InterviewSubjectAttribution)
        _ <- MongoMigrationLedger.checkpoint(handle, MigrationIds.InterviewSubjectAttribution, "z")
        result <- attribution(database).attempt
        missing <- unattributed(database)
        row <- ledgerRow(database, MigrationIds.InterviewSubjectAttribution)
      } yield {
        assertEquals(result, Right(()))
        assertEquals(missing, 0L)
        assertEquals(row.map(_.getString(MongoFields.State)), Some("Complete"))
      }
    }
  }

  /** Receipts written by an earlier binary keep a UUID workflow identity; `id` only has to sort as intended. */
  private def legacyReceipt(id: String, candidate: String, recruiter: String): Document =
    new Document(MongoFields.Id, s"$id:hiring")
      .append("workflowId", id)
      .append("candidateId", candidate)
      .append("recruiterId", recruiter)
      .append(MongoFields.OccurredAt, Date.from(now))

  private def dropLedger(database: mongo4cats.database.MongoDatabase[IO], id: MigrationId): IO[Unit] =
    Mongo4catsCollections
      .documents(database, MongoCollections.HiringMigrationLedger)
      .flatMap(_.deleteOne(Filters.eq(MongoFields.Id, id.value)))
      .void

  private def ledgerRow(database: mongo4cats.database.MongoDatabase[IO], id: MigrationId): IO[Option[Document]] =
    MongoRepositoryTestSupport.findOne(
      database,
      MongoCollections.HiringMigrationLedger,
      Filters.eq(MongoFields.Id, id.value)
    )

  private def unattributed(database: mongo4cats.database.MongoDatabase[IO]): IO[Long] =
    count(database, MongoCollections.InterviewWorkflowInbox, Filters.exists(MongoFields.DocumentType, false))

  test("MSC-02.1 migration 022 attributes legacy hiring receipts in batches and a restart changes nothing") {
    mongoResource.use { fixture =>
      val database = fixture.database
      val ana = UUID.randomUUID().toString
      val bob = UUID.randomUUID().toString
      val legacy = List.fill(1100)(UUID.randomUUID().toString).map(legacyReceipt(_, ana, bob))
      val orphan = legacyReceipt(UUID.randomUUID().toString, ana, bob)
      val typed = MongoInterviewWorkflowCodec
        .inboxDocument("typed:message", InterviewWorkflowId(UUID.randomUUID()), "message", 1L, now)
      for {
        _ <- setup(database)
        _ <- insertMany(database, MongoCollections.InterviewWorkflowInbox, orphan :: typed :: legacy)
        _ <- dropLedger(database, MigrationIds.InterviewSubjectAttribution)
        _ <- attribution(database)
        row <- ledgerRow(database, MigrationIds.InterviewSubjectAttribution)
        stored <- MongoRepositoryTestSupport.findOne(
          database,
          MongoCollections.InterviewWorkflowInbox,
          Filters.eq(MongoFields.Id, orphan.getString(MongoFields.Id))
        )
        attributed <- count(
          database,
          MongoCollections.InterviewWorkflowInbox,
          Filters.and(
            Filters.eq(MongoFields.DocumentType, "hiringReceipt"),
            Filters.eq(MongoFields.SubjectIds, ana),
            Filters.exists(MongoFields.CandidateId, false),
            Filters.exists(MongoFields.RecruiterId, false)
          )
        )
        missing <- unattributed(database)
        typedAfter <- MongoRepositoryTestSupport.findOne(
          database,
          MongoCollections.InterviewWorkflowInbox,
          Filters.eq(MongoFields.Id, "typed:message")
        )
        _ <- fixture.commands.clear
        _ <- attribution(database)
        restart <- fixture.commands.snapshot
      } yield {
        assertEquals(row.map(_.getString(MongoFields.State)), Some("Complete"))
        assertEquals(attributed, 1101L)
        assertEquals(missing, 0L)
        assertEquals(stored.map(_.get(MongoFields.SubjectIds)), Some(List(ana, bob).asJava))
        assertEquals(typedAfter, Some(typed))
        assertEquals(
          restart.filter(command =>
            Set("update", "insert", "delete").contains(command.getFirstKey) &&
              Set(MongoCollections.HiringMigrationLedger, MongoCollections.InterviewWorkflowInbox)
                .contains(command.getString(command.getFirstKey).getValue)
          ),
          Nil
        )
      }
    }
  }

  test("MSC-02.2 migration 022 fails closed on an unattributable inbox row, resumes from its checkpoint and reopens") {
    mongoResource.use { fixture =>
      val database = fixture.database
      val ana = UUID.randomUUID().toString
      val bob = UUID.randomUUID().toString
      val legacy = List.fill(600)(UUID.randomUUID().toString).map(legacyReceipt(_, ana, bob))
      val bad = new Document(MongoFields.Id, "~unattributable")
      for {
        _ <- setup(database)
        _ <- insertMany(database, MongoCollections.InterviewWorkflowInbox, bad :: legacy)
        _ <- dropLedger(database, MigrationIds.InterviewSubjectAttribution)
        first <- attribution(database).attempt
        running <- ledgerRow(database, MigrationIds.InterviewSubjectAttribution)
        second <- attribution(database).attempt
        _ <- Mongo4catsCollections
          .documents(database, MongoCollections.InterviewWorkflowInbox)
          .flatMap(_.deleteOne(Filters.eq(MongoFields.Id, "~unattributable")))
        repaired <- attribution(database).attempt
        done <- ledgerRow(database, MigrationIds.InterviewSubjectAttribution)
        // An earlier binary writes one more legacy receipt after completion; the next start repairs it.
        _ <- insertMany(
          database,
          MongoCollections.InterviewWorkflowInbox,
          List(legacyReceipt("ffffffff-ffff-ffff-ffff-ffffffffffff", ana, bob))
        )
        _ <- attribution(database)
        late <- unattributed(database)
      } yield {
        val expected = MigrationError.StepFailed(
          MigrationIds.InterviewSubjectAttribution,
          "found an unattributable inbox row at ~unattributable"
        )
        assertEquals(first, Left(expected))
        assertEquals(second, Left(expected))
        assertEquals(running.map(_.getString(MongoFields.State)), Some("Running"))
        // The first full page advanced the checkpoint, so the failing run kept its progress.
        assert(running.exists(_.containsKey(MongoFields.LastId)), clue(running))
        assertEquals(repaired, Right(()))
        assertEquals(done.map(_.getString(MongoFields.State)), Some("Complete"))
        assertEquals(late, 0L)
      }
    }
  }

  test("MSC-02.1 two processes starting on legacy hiring receipts converge on one attributed shape") {
    mongoResource.use { fixture =>
      val database = fixture.database
      val ana = UUID.randomUUID().toString
      val bob = UUID.randomUUID().toString
      val legacy = List.fill(700)(UUID.randomUUID().toString).map(legacyReceipt(_, ana, bob))
      for {
        _ <- setup(database)
        _ <- insertMany(database, MongoCollections.InterviewWorkflowInbox, legacy)
        _ <- dropLedger(database, MigrationIds.InterviewSubjectAttribution)
        _ <- (attribution(database), attribution(database)).parTupled
        missing <- unattributed(database)
        attributed <- count(
          database,
          MongoCollections.InterviewWorkflowInbox,
          Filters.eq(MongoFields.SubjectIds, ana)
        )
        row <- ledgerRow(database, MigrationIds.InterviewSubjectAttribution)
      } yield {
        assertEquals(missing, 0L)
        assertEquals(attributed, 700L)
        assertEquals(row.map(_.getString(MongoFields.State)), Some("Complete"))
      }
    }
  }

  private def consumerIndexNames(database: mongo4cats.database.MongoDatabase[IO]): IO[Set[String]] =
    Mongo4catsCollections
      .documents(database, MongoCollections.ConsumerReceipts)
      .flatMap(_.listIndexes[Document])
      .map(_.map(_.getString("name")).toSet)

  private def legacyConsumerIndex(database: mongo4cats.database.MongoDatabase[IO]): IO[Unit] =
    Mongo4catsCollections
      .documents(database, MongoCollections.ConsumerReceipts)
      .flatMap(
        _.createIndex(
          Indexes.ascending(MongoFields.ConsumerGroup, MongoFields.EventId),
          new IndexOptions().name(MongoConsumerReceiptIdentityMigrations.LegacyIndexName).unique(true)
        )
      )
      .void

  private def consumerReceipt(group: String, event: String, id: Option[String] = None): Document =
    new Document(MongoFields.Id, id.getOrElse(s"$group:$event"))
      .append(MongoFields.ConsumerGroup, group)
      .append(MongoFields.EventId, event)
      .append(MongoFields.CreatedAt, Date.from(now))
      .append(MongoFields.ExpiresAt, Date.from(now.plusSeconds(3600)))

  test("MSC-03.1 a duplicate receipt reports seen and the redundant consumer receipt index is gone") {
    mongoResource.use { fixture =>
      val database = fixture.database
      val jobId = UUID.randomUUID()
      val event = OperationalEventEnvelope(
        UUID.randomUUID(),
        OperationalEventType.JOB_CREATED,
        now,
        OperationalAggregateType.Job,
        jobId.toString,
        UserId(UUID.randomUUID()),
        OperationalEventPayload.JobFact(
          OperationalEventPayload.JobKind.Created,
          OperationalEventPayload.JobSnapshot(jobId, List("Scala"), JobStatus.Open)
        )
      )
      val receipts = new MongoConsumerReceiptRepository(database, Diagnostics.noop[IO])
      for {
        _ <- setup(database)
        first <- receipts.record("g", event, now, now.plusSeconds(60)).value
        second <- receipts.record("g", event, now, now.plusSeconds(60)).value
        seen <- receipts.exists("g", event.eventId).value
        names <- consumerIndexNames(database)
      } yield {
        assertEquals(first, Right(true))
        assertEquals(second, Right(false))
        assertEquals(seen, Right(true))
        assertEquals(names, Set("_id_", MongoIndexNames.ConsumerReceiptsExpiry))
      }
    }
  }

  test("MSC-03.1 migration 023 drops the legacy index, keeps every receipt and a restart changes nothing") {
    mongoResource.use { fixture =>
      val database = fixture.database
      val event = UUID.randomUUID().toString
      for {
        _ <- setup(database)
        _ <- legacyConsumerIndex(database)
        _ <- insertMany(
          database,
          MongoCollections.ConsumerReceipts,
          List(consumerReceipt("g", event), consumerReceipt("h", event))
        )
        withIndex <- consumerIndexNames(database)
        _ <- dropLedger(database, MigrationIds.ConsumerReceiptIdentity)
        _ <- setup(database)
        migrated <- consumerIndexNames(database)
        kept <- count(database, MongoCollections.ConsumerReceipts, new BsonDocument())
        ledgerBefore <- ledgerRow(database, MigrationIds.ConsumerReceiptIdentity)
        _ <- fixture.commands.clear
        _ <- setup(database)
        restart <- fixture.commands.snapshot
        ledgerAfter <- ledgerRow(database, MigrationIds.ConsumerReceiptIdentity)
        // A regression (the index reappears) reopens the step, which removes it again.
        _ <- legacyConsumerIndex(database)
        _ <- setup(database)
        reopened <- consumerIndexNames(database)
      } yield {
        assert(withIndex.contains(MongoConsumerReceiptIdentityMigrations.LegacyIndexName))
        assert(!migrated.contains(MongoConsumerReceiptIdentityMigrations.LegacyIndexName))
        assertEquals(kept, 2L)
        assertEquals(ledgerBefore.map(_.getString(MongoFields.State)), Some("Complete"))
        assertEquals(ledgerAfter, ledgerBefore)
        assertEquals(
          restart.filter(command =>
            Set("update", "insert", "delete").contains(command.getFirstKey) &&
              Set(MongoCollections.HiringMigrationLedger, MongoCollections.ConsumerReceipts)
                .contains(command.getString(command.getFirstKey).getValue)
          ),
          Nil
        )
        assert(!reopened.contains(MongoConsumerReceiptIdentityMigrations.LegacyIndexName))
      }
    }
  }

  test("MSC-03.1 migration 023 keeps the index and fails closed on a receipt whose identity differs") {
    mongoResource.use { fixture =>
      val database = fixture.database
      val event = UUID.randomUUID().toString
      for {
        _ <- setup(database)
        _ <- legacyConsumerIndex(database)
        _ <- insertMany(
          database,
          MongoCollections.ConsumerReceipts,
          List(consumerReceipt("g", event), consumerReceipt("h", event, Some("opaque")))
        )
        _ <- dropLedger(database, MigrationIds.ConsumerReceiptIdentity)
        first <- setup(database).attempt
        names <- consumerIndexNames(database)
        _ <- Mongo4catsCollections
          .documents(database, MongoCollections.ConsumerReceipts)
          .flatMap(_.deleteOne(Filters.eq(MongoFields.Id, "opaque")))
        repaired <- setup(database).attempt
        after <- consumerIndexNames(database)
      } yield {
        assertEquals(
          first,
          Left(
            MigrationError.StepFailed(
              MigrationIds.ConsumerReceiptIdentity,
              "found a receipt whose identity is not its consumer group and event"
            )
          )
        )
        assert(names.contains(MongoConsumerReceiptIdentityMigrations.LegacyIndexName))
        assertEquals(repaired, Right(()))
        assert(!after.contains(MongoConsumerReceiptIdentityMigrations.LegacyIndexName))
      }
    }
  }

  test("MSC-03.1 two processes starting on a database with the legacy index converge") {
    mongoResource.use { fixture =>
      val database = fixture.database
      for {
        _ <- setup(database)
        _ <- legacyConsumerIndex(database)
        _ <- dropLedger(database, MigrationIds.ConsumerReceiptIdentity)
        _ <- (setup(database), setup(database)).parTupled
        names <- consumerIndexNames(database)
        row <- ledgerRow(database, MigrationIds.ConsumerReceiptIdentity)
      } yield {
        assert(!names.contains(MongoConsumerReceiptIdentityMigrations.LegacyIndexName))
        assertEquals(row.map(_.getString(MongoFields.State)), Some("Complete"))
      }
    }
  }
}
