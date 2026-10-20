package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.InterviewLifecycleFixtures
import com.example.graphQL.cats.domain.workflow.InterviewWorkflowId
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, CandidateProfile, JobStatus, User, UserProfile, UserRole}
import com.example.graphQL.cats.service.{Diagnostics, RepositoryError}
import com.example.graphQL.cats.service.events.OperationalEventPayload.SearchKind
import com.example.graphQL.cats.service.events.*
import com.example.graphQL.cats.service.port.PendingSearchSessionWork
import com.example.graphQL.cats.AccountValueFixtures.email
import com.mongodb.MongoWriteException
import com.mongodb.client.model.{Filters, Updates}
import mongo4cats.database.MongoDatabase
import org.bson.Document
import org.bson.conversions.Bson
import java.time.Instant
import java.util.{Date, UUID}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

/** Optional-field absence (migration 024) and the operational collection validators (migration 025): the stored shape
  * writers produce, the repairs of stored rows, the rejections the validators make, and restart behavior.
  */
final class MongoOperationalCollectionValidatorsIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 10.minutes

  private val now = Instant.parse("2026-10-07T12:00:00Z")
  private val Absence = MigrationIds.OptionalFieldAbsence
  private val Validators = MigrationIds.OperationalCollectionValidators

  private def init(database: MongoDatabase[IO]): IO[Unit] = MongoHiringSetup.initialize(database, Diagnostics.noop[IO])

  private def collection(database: MongoDatabase[IO], name: String) = Mongo4catsCollections.documents(database, name)

  private def ledger(database: MongoDatabase[IO]): IO[List[Document]] =
    collection(database, MongoCollections.HiringMigrationLedger).flatMap(_.find(Filters.empty()).all).map(_.toList)

  private def ledgerRow(database: MongoDatabase[IO], id: MigrationId): IO[Option[Document]] =
    ledger(database).map(_.find(_.getString(MongoFields.Id) == id.value))

  private def count(database: MongoDatabase[IO], name: String, filter: Bson = Filters.empty()): IO[Long] =
    MongoRepositoryTestSupport.count(database, name, filter)

  private def find(database: MongoDatabase[IO], name: String, id: String): IO[Option[Document]] =
    MongoRepositoryTestSupport.findOne(database, name, Filters.eq(MongoFields.Id, id))

  private def insert(database: MongoDatabase[IO], name: String, document: Document): IO[Unit] =
    MongoRepositoryTestSupport.insertOne(database, name, document)

  /** The MongoDB document-validation failure code. */
  private def rejected(result: Either[Throwable, ?]): Boolean = result.left.exists {
    case error: MongoWriteException => error.getError.getCode == 121
    case _                          => false
  }

  private def validatorsInstalled(database: MongoDatabase[IO]): IO[Boolean] =
    MongoHiringSetup.setupDatabase(database).flatMap { setup =>
      MongoOperationalValidators.all.forallM { case (name, expected) =>
        MongoHiringValidators.validatorMatches(setup, name, expected)
      }
    }

  /** A database that completed 023 under the earlier binary: no 024/025 proof, the earlier users validator, none of the
    * new validators.
    */
  private def rewind(database: MongoDatabase[IO]): IO[Unit] =
    for {
      setup <- MongoHiringSetup.setupDatabase(database)
      _ <- MongoOperationalValidators.all.map(_._1).filterNot(_ == MongoCollections.Users).traverse_ { name =>
        setup.runCommand(new Document("collMod", name).append("validator", new Document()))
      }
      _ <- MongoHiringValidators.install(setup, MongoCollections.Users, MongoHiringValidators.userValidatorV1)
      rows <- collection(database, MongoCollections.HiringMigrationLedger)
      _ <- rows.deleteMany(Filters.in(MongoFields.Id, Absence.value, Validators.value))
    } yield ()

  private def candidate(name: String, id: UUID = UUID.randomUUID()): User =
    User(
      UserId(id),
      Some(email(s"$name@example.com")),
      name,
      UserRole.Candidate,
      Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))),
      now
    )

  private def searchSession(actor: UserId): SearchSession =
    SearchSession(
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

  private def result[A](value: Either[RepositoryError, A]): IO[A] =
    value.fold(error => IO.raiseError(new AssertionError(s"Unexpected repository result: $error")), IO.pure)

  private def without(document: Document, field: String): Document = {
    val _ = document.remove(field)
    document
  }

  private def hasNoNulls(document: Document): Boolean = !document.values().asScala.exists(_ == null)

  test("MSC-04.1 an outbox row released from a lease and a session without query or model omit those fields") {
    mongoResource.use { fixture =>
      val actor = UserId(UUID.randomUUID())
      val value = event(actor)
      val session = searchSession(actor)
      val searchEvent =
        OperationalEvents.searchPerformed(UUID.randomUUID(), session).fold(error => fail(error.toString), identity)
      val outbox =
        MongoOperationalEventOutboxRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
      val work = MongoSearchSessionWorkRepository.transactional(fixture.database, fixture.client, Diagnostics.noop[IO])
      for {
        _ <- init(fixture.database)
        inserted <- IO.fromEither(MongoHiringCodecs.outboxRecord(value, now).leftMap(new AssertionError(_)))
        _ <- insert(fixture.database, MongoCollections.EventOutbox, inserted)
        fresh <- find(fixture.database, MongoCollections.EventOutbox, value.eventId.toString)
        claimed <- outbox
          .claim("validator-test", s"hiring-publisher-${UUID.randomUUID()}", now.plusSeconds(1), now.plusSeconds(30), 1)
          .value
          .flatMap(result)
        leased <- find(fixture.database, MongoCollections.EventOutbox, value.eventId.toString)
        _ <- outbox
          .releaseForRetry(value.eventId, claimed.head.leaseToken, now.plusSeconds(2), now.plusSeconds(10))
          .value
          .flatMap(result)
        released <- find(fixture.database, MongoCollections.EventOutbox, value.eventId.toString)
        _ <- work.enqueue(PendingSearchSessionWork(session, searchEvent), now).value.flatMap(result)
        pending <- find(fixture.database, MongoCollections.SearchSessionWork, session.id.toString)
        lease <- work.claim("validator-test", now.plusSeconds(1), now.plusSeconds(30)).value.flatMap(result)
        _ <- work.complete(lease.get, now.plusSeconds(2)).value.flatMap(result)
        stored <- find(fixture.database, MongoCollections.SearchSessions, session.id.toString)
      } yield {
        List(fresh, released).flatten.foreach { row =>
          assert(!row.containsKey(MongoFields.LeaseOwner), clue(row))
          assert(!row.containsKey(MongoFields.LeaseToken), clue(row))
          assert(hasNoNulls(row), clue(row))
        }
        assertEquals(fresh.isDefined && released.isDefined, true)
        assert(leased.exists(_.containsKey(MongoFields.LeaseToken)), clue(leased))
        val embedded = pending.map(_.get(MongoFields.Session, classOf[Document]))
        assert(embedded.exists(row => !row.containsKey(MongoFields.Query) && !row.containsKey(MongoFields.Model)))
        assert(stored.exists(row => !row.containsKey(MongoFields.Query) && !row.containsKey(MongoFields.Model)))
        assert(stored.exists(hasNoNulls))
      }
    }
  }

  test("MSC-04.1 a new user stores updatedAt equal to createdAt and passes the users validator") {
    mongoResource.use { fixture =>
      val user = candidate("new-user")
      for {
        _ <- init(fixture.database)
        _ <- insert(fixture.database, MongoCollections.Users, MongoHiringCodecs.user(user))
        stored <- find(fixture.database, MongoCollections.Users, user.id.value.toString)
      } yield {
        assertEquals(stored.map(_.get(MongoFields.UpdatedAt)), stored.map(_.get(MongoFields.CreatedAt)))
        assert(stored.exists(_.get(MongoFields.UpdatedAt).isInstanceOf[Date]), clue(stored))
      }
    }
  }

  test("MSC-05.1 an application with a status that is not a status is rejected, a valid one is stored") {
    mongoResource.use { fixture =>
      val application = Application.create(
        ApplicationId(UUID.randomUUID()),
        UserId(UUID.randomUUID()),
        JobId(UUID.randomUUID()),
        now
      )
      val valid = MongoHiringCodecs.application(application)
      for {
        _ <- init(fixture.database)
        pending <- insert(
          fixture.database,
          MongoCollections.Applications,
          MongoHiringCodecs
            .application(application.copy(id = ApplicationId(UUID.randomUUID())))
            .append(MongoFields.Status, "Pending")
        ).attempt
        extra <- insert(
          fixture.database,
          MongoCollections.Applications,
          MongoHiringCodecs
            .application(application.copy(id = ApplicationId(UUID.randomUUID())))
            .append("unexpected", "field")
        ).attempt
        accepted <- insert(fixture.database, MongoCollections.Applications, valid).attempt
        invalidUpdate <- collection(fixture.database, MongoCollections.Applications)
          .flatMap(
            _.updateOne(Filters.eq(MongoFields.Id, valid.getString(MongoFields.Id)), Updates.set("status", "Pending"))
          )
          .attempt
      } yield {
        assert(rejected(pending), clue(pending))
        assert(rejected(extra), clue(extra))
        assert(accepted.isRight, clue(accepted))
        assert(rejected(invalidUpdate), clue(invalidUpdate))
      }
    }
  }

  test("MSC-05.2 a Deleted user keeps no email, credentials, profile or embedding") {
    mongoResource.use { fixture =>
      def deleted(id: UUID): Document =
        new Document(MongoFields.Id, id.toString)
          .append(MongoFields.Version, Long.box(1L))
          .append(MongoFields.Role, "Candidate")
          .append(MongoFields.AccountStatus, "Deleted")
          .append(MongoFields.Name, s"deleted-${id}")
          .append(MongoFields.NameCanonical, s"deleted-${id}")
          .append(MongoFields.CreatedAt, Date.from(now))
          .append(MongoFields.UpdatedAt, Date.from(now))
          .append(MongoFields.DeletedAt, Date.from(now))
      val personal: List[(String, AnyRef)] = List(
        MongoFields.Email -> "ana@example.com",
        MongoFields.EmailCanonical -> "ana@example.com",
        MongoFields.PasswordHash -> "hash",
        MongoFields.Profile -> new Document(MongoFields.Kind, "Candidate"),
        MongoFields.Embedding -> List(Double.box(0.1d)).asJava,
        MongoFields.EmbeddingMeta -> new Document(MongoFields.Model, "m")
      )
      for {
        _ <- init(fixture.database)
        clean <- insert(fixture.database, MongoCollections.Users, deleted(UUID.randomUUID())).attempt
        inserts <- personal.traverse { case (field, value) =>
          insert(
            fixture.database,
            MongoCollections.Users,
            deleted(UUID.randomUUID()).append(field, value)
          ).attempt.map(field -> _)
        }
        target = deleted(UUID.randomUUID())
        _ <- insert(fixture.database, MongoCollections.Users, target)
        updated <- collection(fixture.database, MongoCollections.Users)
          .flatMap(_.updateOne(Filters.eq(MongoFields.Id, target.getString(MongoFields.Id)), Updates.set("email", "x")))
          .attempt
        mistyped <- insert(
          fixture.database,
          MongoCollections.Users,
          deleted(UUID.randomUUID()).append(MongoFields.CreatedAt, "yesterday")
        ).attempt
        undated <- insert(
          fixture.database,
          MongoCollections.Users,
          without(deleted(UUID.randomUUID()), MongoFields.UpdatedAt)
        ).attempt
      } yield {
        assert(clean.isRight, clue(clean))
        inserts.foreach { case (field, result) => assert(rejected(result), clue((field, result))) }
        assert(rejected(updated), clue(updated))
        assert(rejected(mistyped), clue(mistyped))
        assert(rejected(undated), clue(undated))
      }
    }
  }

  test("MSC-05.3 an inbox document without a known documentType is rejected, every stored type is accepted") {
    mongoResource.use { fixture =>
      val workflow = InterviewLifecycleFixtures.workflowId
      def untyped(id: String): Document =
        without(MongoInterviewWorkflowCodec.inboxDocument(id, workflow, "message", 1L, now), "documentType")
      for {
        _ <- init(fixture.database)
        missing <- insert(fixture.database, MongoCollections.InterviewWorkflowInbox, untyped("untyped")).attempt
        unknown <- insert(
          fixture.database,
          MongoCollections.InterviewWorkflowInbox,
          untyped("unknown").append("documentType", "bogus")
        ).attempt
        receipt <- insert(
          fixture.database,
          MongoCollections.InterviewWorkflowInbox,
          MongoInterviewWorkflowCodec.inboxDocument("receipt", workflow, "message", 1L, now)
        ).attempt
        hiring <- insert(
          fixture.database,
          MongoCollections.InterviewWorkflowInbox,
          MongoInterviewWorkflowCodec.hiringReceiptDocument(
            s"${workflow.value}:hiring",
            workflow,
            InterviewLifecycleFixtures.candidateId,
            InterviewLifecycleFixtures.recruiterId,
            now
          )
        ).attempt
        quarantine <- insert(
          fixture.database,
          MongoCollections.InterviewWorkflowInbox,
          new Document(MongoFields.Id, "quarantine:message")
            .append("documentType", "quarantine")
            .append("failureCode", "InvalidMessage")
            .append(MongoFields.OccurredAt, Date.from(now))
            .append(MongoFields.RetentionExpiresAt, Date.from(now.plusSeconds(60)))
        ).attempt
        audited <- insert(
          fixture.database,
          MongoCollections.InterviewWorkflowInbox,
          MongoInterviewWorkflowCodec
            .inboxDocument("audited", workflow, "repair", 1L, now)
            .append("repaired", Int.box(2))
            .append("auditActorId", UUID.randomUUID().toString)
        ).attempt
        nullActor <- insert(
          fixture.database,
          MongoCollections.InterviewWorkflowInbox,
          new Document(MongoInterviewWorkflowCodec.inboxDocument("nulled", workflow, "message", 1L, now))
            .append("auditActorId", null)
        ).attempt
      } yield {
        assert(rejected(missing), clue(missing))
        assert(rejected(unknown), clue(unknown))
        assert(rejected(nullActor), clue(nullActor))
        List(receipt, hiring, quarantine, audited).foreach(result => assert(result.isRight, clue(result)))
      }
    }
  }

  test("MSC-05 every validated collection rejects a bare row and the closed receipts reject unknown fields") {
    mongoResource.use { fixture =>
      for {
        _ <- init(fixture.database)
        installed <- validatorsInstalled(fixture.database)
        bare <- MongoOperationalValidators.all.traverse { case (name, _) =>
          insert(fixture.database, name, new Document(MongoFields.Id, "bare-row")).attempt.map(name -> _)
        }
        notification <- insert(
          fixture.database,
          MongoCollections.InterviewNotificationReceipts,
          new Document(MongoFields.Id, s"${UUID.randomUUID()}:notify:Candidate")
            .append("workflowId", UUID.randomUUID().toString)
            .append("recipientId", UUID.randomUUID().toString)
            .append("participant", "Candidate")
            .append("kind", "Scheduled")
            .append("deliveredAt", Date.from(now))
        ).attempt
        withSubjects <- insert(
          fixture.database,
          MongoCollections.InterviewNotificationReceipts,
          new Document(MongoFields.Id, s"${UUID.randomUUID()}:notify:Recruiter")
            .append("workflowId", UUID.randomUUID().toString)
            .append("recipientId", UUID.randomUUID().toString)
            .append(MongoFields.SubjectIds, List(UUID.randomUUID().toString).asJava)
            .append("participant", "Recruiter")
            .append("kind", "Scheduled")
            .append("deliveredAt", Date.from(now))
        ).attempt
        workflow <- insert(
          fixture.database,
          MongoCollections.InterviewWorkflows,
          MongoInterviewWorkflowCodec.workflowDocument(InterviewLifecycleFixtures.completed)
        ).attempt
        wrongPhase <- insert(
          fixture.database,
          MongoCollections.InterviewWorkflows,
          MongoInterviewWorkflowCodec
            .workflowDocument(InterviewLifecycleFixtures.completed.copy(id = InterviewWorkflowId(UUID.randomUUID())))
            .append("phase", Int.box(3))
        ).attempt
      } yield {
        assert(installed)
        bare.foreach { case (name, result) => assert(rejected(result), clue((name, result))) }
        assert(notification.isRight, clue(notification))
        assert(rejected(withSubjects), clue(withSubjects))
        assert(workflow.isRight, clue(workflow))
        assert(rejected(wrongPhase), clue(wrongPhase))
      }
    }
  }

  /** Seeds one row of every shape migration 024 repairs, plus a hiring receipt that must keep its subjects. */
  private def seedLegacyShapes(database: MongoDatabase[IO], users: Int): IO[Unit] = {
    val actor = UserId(UUID.randomUUID())
    val session = searchSession(actor)
    val searchEvent =
      OperationalEvents.searchPerformed(UUID.randomUUID(), session).fold(error => fail(error.toString), identity)
    val work = MongoSearchSessionWorkCodecs.work(PendingSearchSessionWork(session, searchEvent), now)
    work.get(MongoFields.Session, classOf[Document]).append(MongoFields.Query, null).append(MongoFields.Model, null)
    val workflow = InterviewLifecycleFixtures.workflowId
    val outboxRow =
      MongoHiringCodecs.outboxRecord(event(actor), now).fold(message => fail(message), identity)
    outboxRow.append(MongoFields.LeaseOwner, null).append(MongoFields.LeaseToken, null)
    val legacyUsers = (0 until users).toList.map { index =>
      val name = f"user-$index%05d"
      new Document(MongoFields.Id, name)
        .append(MongoFields.Version, Long.box(0L))
        .append(MongoFields.Role, "Candidate")
        .append(MongoFields.AccountStatus, "Deleted")
        .append(MongoFields.Name, name)
        .append(MongoFields.NameCanonical, name)
        .append(MongoFields.CreatedAt, Date.from(now.plusSeconds(index.toLong)))
    }
    for {
      _ <- insert(database, MongoCollections.EventOutbox, outboxRow)
      _ <- insert(
        database,
        MongoCollections.SearchSessions,
        MongoHiringCodecs
          .searchSession(session.copy(id = UUID.randomUUID()))
          .append(MongoFields.Query, null)
          .append(MongoFields.Model, null)
      )
      _ <- insert(database, MongoCollections.SearchSessionWork, work)
      _ <- insert(
        database,
        MongoCollections.InterviewWorkflowInbox,
        MongoInterviewWorkflowCodec
          .inboxDocument("legacy-inbox", workflow, "message", 1L, now)
          .append("auditActorId", null)
      )
      _ <- insert(
        database,
        MongoCollections.InterviewWorkflowInbox,
        MongoInterviewWorkflowCodec.hiringReceiptDocument(
          s"${workflow.value}:hiring",
          workflow,
          InterviewLifecycleFixtures.candidateId,
          InterviewLifecycleFixtures.recruiterId,
          now
        )
      )
      _ <- insert(
        database,
        MongoCollections.InterviewWorkflows,
        MongoInterviewWorkflowCodec
          .workflowDocument(InterviewLifecycleFixtures.completed)
          .append(
            MongoFields.SubjectIds,
            List(
              InterviewLifecycleFixtures.candidateId.value.toString,
              InterviewLifecycleFixtures.recruiterId.value.toString
            ).asJava
          )
      )
      _ <- insert(
        database,
        MongoCollections.InterviewNotificationReceipts,
        new Document(MongoFields.Id, s"${workflow.value}:notify:Candidate")
          .append("workflowId", workflow.value.toString)
          .append("recipientId", InterviewLifecycleFixtures.candidateId.value.toString)
          .append(MongoFields.SubjectIds, List(InterviewLifecycleFixtures.candidateId.value.toString).asJava)
          .append("participant", "Candidate")
          .append("kind", "Scheduled")
          .append("deliveredAt", Date.from(now))
      )
      _ <- IO.whenA(legacyUsers.nonEmpty)(
        collection(database, MongoCollections.Users).flatMap(_.insertMany(legacyUsers)).void
      )
    } yield ()
  }

  private def assertRepaired(database: MongoDatabase[IO], users: Long): IO[Unit] =
    for {
      nulls <- List(
        MongoCollections.EventOutbox,
        MongoCollections.SearchSessions,
        MongoCollections.SearchSessionWork,
        MongoCollections.InterviewWorkflowInbox
      ).traverse(name =>
        count(
          database,
          name,
          Filters.or(
            List("leaseOwner", "leaseToken", "query", "model", "session.query", "session.model", "auditActorId")
              .map(path => new Document(path, new Document("$type", "null")))*
          )
        )
      )
      workflowSubjects <- count(database, MongoCollections.InterviewWorkflows, Filters.exists(MongoFields.SubjectIds))
      notificationSubjects <- count(
        database,
        MongoCollections.InterviewNotificationReceipts,
        Filters.exists(MongoFields.SubjectIds)
      )
      hiring <- count(
        database,
        MongoCollections.InterviewWorkflowInbox,
        Filters.and(Filters.eq("documentType", "hiringReceipt"), Filters.exists(MongoFields.SubjectIds))
      )
      withoutUpdatedAt <- count(database, MongoCollections.Users, Filters.exists(MongoFields.UpdatedAt, false))
      equalDates <- count(
        database,
        MongoCollections.Users,
        new Document("$expr", new Document("$eq", List("$updatedAt", "$createdAt").asJava))
      )
      workflow <- count(database, MongoCollections.InterviewWorkflows)
      inbox <- count(database, MongoCollections.InterviewWorkflowInbox)
    } yield {
      assertEquals(nulls, List(0L, 0L, 0L, 0L))
      assertEquals(workflowSubjects, 0L)
      assertEquals(notificationSubjects, 0L)
      assertEquals(hiring, 1L)
      assertEquals(withoutUpdatedAt, 0L)
      assertEquals(equalDates, users)
      assertEquals(workflow, 1L)
      assertEquals(inbox, 2L)
    }

  test(
    "MSC-10.1 migrations 024 and 025 repair a database at 023, install every validator and write nothing on restart"
  ) {
    mongoResource.use { fixture =>
      val users = 1_100
      for {
        _ <- init(fixture.database)
        _ <- rewind(fixture.database)
        _ <- seedLegacyShapes(fixture.database, users)
        _ <- init(fixture.database)
        absence <- ledgerRow(fixture.database, Absence)
        validators <- ledgerRow(fixture.database, Validators)
        installed <- validatorsInstalled(fixture.database)
        _ <- assertRepaired(fixture.database, users.toLong)
        before <- ledger(fixture.database)
        _ <- init(fixture.database)
        after <- ledger(fixture.database)
        stillInstalled <- validatorsInstalled(fixture.database)
        _ <- assertRepaired(fixture.database, users.toLong)
      } yield {
        List(absence, validators).foreach { row =>
          assertEquals(row.map(_.getString(MongoFields.State)), Some("Complete"))
          assertEquals(row.map(_.containsKey(MongoFields.LastId)), Some(false))
        }
        assert(installed)
        assert(stillInstalled)
        assertEquals(after.toSet, before.toSet)
      }
    }
  }

  test("MSC-10.1 migration 024 resumes from its checkpoint after a failure and completes once the row is repaired") {
    mongoResource.use { fixture =>
      val users = 1_100
      val unrepairable = new Document(MongoFields.Id, "zzz-no-created-at")
        .append(MongoFields.Version, Long.box(0L))
        .append(MongoFields.Role, "Candidate")
        .append(MongoFields.AccountStatus, "Deleted")
        .append(MongoFields.Name, "zzz")
        .append(MongoFields.NameCanonical, "zzz")
      for {
        _ <- init(fixture.database)
        _ <- rewind(fixture.database)
        _ <- seedLegacyShapes(fixture.database, users)
        _ <- insert(fixture.database, MongoCollections.Users, unrepairable)
        failed <- init(fixture.database).attempt
        running <- ledgerRow(fixture.database, Absence)
        repairedSoFar <- count(fixture.database, MongoCollections.Users, Filters.exists(MongoFields.UpdatedAt))
        noValidators <- ledgerRow(fixture.database, Validators)
        _ <- collection(fixture.database, MongoCollections.Users).flatMap(
          _.updateOne(
            Filters.eq(MongoFields.Id, "zzz-no-created-at"),
            Updates.set(MongoFields.CreatedAt, Date.from(now))
          )
        )
        _ <- init(fixture.database)
        complete <- ledgerRow(fixture.database, Absence)
        installed <- validatorsInstalled(fixture.database)
        unset <- count(fixture.database, MongoCollections.Users, Filters.exists(MongoFields.UpdatedAt, false))
      } yield {
        assertEquals(
          failed,
          Left(MigrationError.StepFailed(Absence, s"found a document without createdAt in ${MongoCollections.Users}"))
        )
        assertEquals(running.map(_.getString(MongoFields.State)), Some("Running"))
        assertEquals(running.map(_.getString(MongoFields.LastId)), Some("09:user-00999"))
        assertEquals(repairedSoFar, users.toLong)
        assertEquals(noValidators, None)
        assertEquals(complete.map(_.getString(MongoFields.State)), Some("Complete"))
        assertEquals(complete.map(_.containsKey(MongoFields.LastId)), Some(false))
        assert(installed)
        assertEquals(unset, 0L)
      }
    }
  }

  test("MSC-10.1 a completed migration 024 reopens when an earlier binary stores a null again") {
    mongoResource.use { fixture =>
      val actor = UserId(UUID.randomUUID())
      val late = MongoHiringCodecs.outboxRecord(event(actor), now).fold(message => fail(message), identity)
      for {
        _ <- init(fixture.database)
        _ <- insert(fixture.database, MongoCollections.EventOutbox, late.append(MongoFields.LeaseOwner, null))
        _ <- init(fixture.database)
        row <- find(fixture.database, MongoCollections.EventOutbox, late.getString(MongoFields.Id))
        state <- ledgerRow(fixture.database, Absence)
      } yield {
        assert(row.exists(!_.containsKey(MongoFields.LeaseOwner)), clue(row))
        assertEquals(state.map(_.getString(MongoFields.State)), Some("Complete"))
      }
    }
  }

  test("MSC-10.1 two processes starting a database at 023 converge on one repaired and validated shape") {
    mongoResource.use { fixture =>
      val users = 600
      for {
        _ <- init(fixture.database)
        _ <- rewind(fixture.database)
        _ <- seedLegacyShapes(fixture.database, users)
        _ <- (init(fixture.database), init(fixture.database)).parTupled
        installed <- validatorsInstalled(fixture.database)
        rows <- ledger(fixture.database)
        _ <- assertRepaired(fixture.database, users.toLong)
      } yield {
        assert(installed)
        assert(rows.forall(_.getString(MongoFields.State) == "Complete"), clue(rows))
        assert(rows.exists(_.getString(MongoFields.Id) == Validators.value))
      }
    }
  }

  test("MSC-05 the users validator of migration 016 is succeeded by V2 and any other definition is drift") {
    mongoResource.use { fixture =>
      for {
        _ <- init(fixture.database)
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        _ <- rewind(fixture.database)
        v1Installed <- MongoHiringValidators.validatorMatches(
          setup,
          MongoCollections.Users,
          MongoHiringValidators.userValidatorV1
        )
        _ <- init(fixture.database)
        v2Installed <- MongoHiringValidators.validatorMatches(
          setup,
          MongoCollections.Users,
          MongoHiringValidators.userValidator
        )
        _ <- setup.runCommand(
          new Document("collMod", MongoCollections.Users).append("validator", new Document())
        )
        drifted <- init(fixture.database).attempt
      } yield {
        assert(v1Installed)
        assert(v2Installed)
        assertEquals(drifted, Left(MigrationError.ValidatorMismatch(MongoCollections.Users)))
      }
    }
  }

  test("MSC-05 drift of a validated collection behind the completed migration fails closed naming that collection") {
    mongoResource.use { fixture =>
      for {
        _ <- init(fixture.database)
        setup <- MongoHiringSetup.setupDatabase(fixture.database)
        _ <- setup.runCommand(
          new Document("collMod", MongoCollections.Applications).append("validator", new Document())
        )
        drifted <- init(fixture.database).attempt
        state <- ledgerRow(fixture.database, Validators)
      } yield {
        assertEquals(drifted, Left(MigrationError.ValidatorMismatch(MongoCollections.Applications)))
        assertEquals(state.map(_.getString(MongoFields.State)), Some("Complete"))
      }
    }
  }

  MongoOperationalValidators.all.foreach { case (name, _) =>
    test(s"MSC-05 migration 025 fails closed naming only $name when a stored row violates its validator") {
      mongoResource.use { fixture =>
        val invalid = new Document(MongoFields.Id, "invalid-row")
          .append(MongoFields.CreatedAt, Date.from(now))
          .append(MongoFields.UpdatedAt, Date.from(now))
          .append(MongoFields.DocumentType, "bogus")
        // The earlier users validator is still installed, so the row must satisfy it and fail only the successor.
        val stored =
          if (name == MongoCollections.Users)
            invalid
              .append(MongoFields.Version, Long.box(0L))
              .append(MongoFields.Role, "Candidate")
              .append(MongoFields.AccountStatus, "Deleted")
          else invalid
        for {
          _ <- init(fixture.database)
          setup <- MongoHiringSetup.setupDatabase(fixture.database)
          _ <- rewind(fixture.database)
          _ <- insert(fixture.database, name, stored)
          failed <- init(fixture.database).attempt
          state <- ledgerRow(fixture.database, Validators)
          installedWhileInvalid <- MongoOperationalValidators.all.existsM { case (other, validator) =>
            MongoHiringValidators.validatorMatches(setup, other, validator)
          }
          _ <- collection(fixture.database, name).flatMap(_.deleteOne(Filters.eq(MongoFields.Id, "invalid-row")))
          _ <- init(fixture.database)
          repaired <- validatorsInstalled(fixture.database)
          done <- ledgerRow(fixture.database, Validators)
        } yield {
          assertEquals(
            failed,
            Left(MigrationError.StepFailed(Validators, s"found invalid stored data in $name"))
          )
          assertEquals(state.map(_.getString(MongoFields.State)), Some("Running"))
          assert(!installedWhileInvalid, "nothing is installed while a stored row is invalid")
          assert(repaired)
          assertEquals(done.map(_.getString(MongoFields.State)), Some("Complete"))
        }
      }
    }
  }
}
