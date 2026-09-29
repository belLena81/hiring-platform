package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.AccountValueFixtures.email
import cats.effect.{Deferred, IO, Resource}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.{
  AccountDeletionStatus,
  AccountStatus,
  PasswordHash,
  RecruiterProfile,
  User,
  UserProfile,
  UserRole
}
import com.example.graphQL.cats.repository.protocol.{
  AnalyticsFunnelDay,
  AnalyticsReportSnapshot,
  AnalyticsSkillPostingDay,
  MutationWriteContext,
  RepositoryError
}
import com.example.graphQL.cats.shared.events.{
  OperationalAggregateType,
  OperationalEventEnvelope,
  OperationalEventType,
  OperationalEvents,
  SearchSession,
  SearchSessionResult
}
import com.example.graphQL.cats.service.ActorContext
import com.example.graphQL.cats.service.auth.{
  AccessTokenIssuer,
  AccessTokenIssuanceError,
  PasswordHasher,
  UserAccountService
}
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.IdempotencyRequest
import com.example.graphQL.cats.domain.model.AccountToken
import io.circe.Json
import munit.CatsEffectSuite
import org.bson.Document
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.{Duration, Instant}
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class MongoOutboxSubjectReferencesIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val image = "mongo:8.0.32-noble@sha256:01354084d2ae665d2e79b79b0cdc50c2c0c98873618912d9a2c8c9cb5c3d24e6"
  private val now = Instant.parse("2026-09-22T12:00:00Z")
  private final class ReplicaSet extends GenericContainer[ReplicaSet](DockerImageName.parse(image))

  private def replicaSet: Resource[IO, ReplicaSet] =
    Resource.make(IO.blocking {
      val instance = new ReplicaSet
      val _ = instance
        .withExposedPorts(27017)
        .withCommand("mongod", "--bind_ip_all", "--replSet", "rs0")
        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90)))
      try {
        instance.start()
        val initiated = instance.execInContainer(
          "mongosh",
          "--quiet",
          "--eval",
          "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})"
        )
        if (initiated.getExitCode != 0)
          throw new AssertionError(s"Replica-set initiation failed: ${initiated.getStderr}")
        instance
      } catch {
        case error: Throwable =>
          instance.stop()
          throw error
      }
    })(instance => IO.blocking(instance.stop()))

  private def awaitPrimary(instance: ReplicaSet, remaining: Int = 60): IO[Unit] =
    IO.blocking(instance.execInContainer("mongosh", "--quiet", "--eval", "db.hello().isWritablePrimary")).flatMap {
      result =>
        if (result.getExitCode == 0 && result.getStdout.trim == "true") IO.unit
        else if (remaining > 0) IO.sleep(250.millis) *> awaitPrimary(instance, remaining - 1)
        else IO.raiseError(new AssertionError(s"Mongo replica set did not elect a primary: ${result.getStderr}"))
    }

  private def uri(instance: ReplicaSet): String =
    s"mongodb://${instance.getHost}:${instance.getMappedPort(27017)}/?replicaSet=rs0&directConnection=true"

  private def uuid(value: Long): UUID = new UUID(0L, value)

  private def event(
      id: Long,
      eventType: OperationalEventType,
      aggregateType: OperationalAggregateType,
      aggregateId: UUID,
      actorId: UserId,
      payload: Json
  ): OperationalEventEnvelope =
    OperationalEventEnvelope(uuid(id), eventType, now, aggregateType, aggregateId.toString, actorId, payload)

  private def outboxRecord(value: OperationalEventEnvelope): Document =
    MongoHiringCodecs.outboxRecord(value, now).fold(message => throw new AssertionError(message), identity)

  private def report(asOf: Instant, created: Long): AnalyticsReportSnapshot =
    AnalyticsReportSnapshot(
      asOf,
      List(AnalyticsFunnelDay(asOf, created, 0L, 0L, 0L, 0L, 0L)),
      None,
      List(AnalyticsSkillPostingDay(asOf, "scala", created))
    )

  private def right[A](value: Either[RepositoryError, A]): IO[A] =
    value.fold(error => IO.raiseError(new AssertionError(s"Unexpected repository error: $error")), IO.pure)

  test("outbox subject migration resumes after unresolved search results and indexes candidate subjects") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        val database = client.getDatabase(s"outbox_subjects_${UUID.randomUUID()}")
        val actor = UserId(uuid(100L))
        val candidate = UserId(uuid(101L))
        val searchId = uuid(200L)
        val session = SearchSession(
          searchId,
          actor,
          "candidateMatches",
          None,
          Json.obj(),
          None,
          List(SearchSessionResult(candidate.value.toString, 1, 0.9d)),
          now,
          now.plusSeconds(7.days.toSeconds)
        )
        val click = OperationalEvents.searchResultClicked(
          uuid(2L),
          searchId,
          candidate.value.toString,
          "candidateMatches",
          actor,
          1,
          now
        )
        val legacyClick = outboxRecord(click)
        legacyClick.remove("subjectRefsVersion")
        val performed = OperationalEvents.searchPerformed(uuid(3L), session)
        val application = event(
          4L,
          OperationalEventType.APPLICATION_CREATED,
          OperationalAggregateType.Application,
          uuid(400L),
          actor,
          Json.obj("candidateId" -> Json.fromString(candidate.value.toString))
        )
        val incompleteLegacyRecord = outboxRecord(application)
          .append("subjectIds", List(actor.value.toString).asJava)
        incompleteLegacyRecord.remove("subjectRefsVersion")
        val outbox = database.getCollection("event_outbox")

        for {
          _ <- PublisherBridge
            .first(
              outbox.insertOne(
                outboxRecord(
                  event(
                    1L,
                    OperationalEventType.JOB_CREATED,
                    OperationalAggregateType.Job,
                    uuid(300L),
                    actor,
                    Json.obj("job" -> Json.obj("jobId" -> Json.fromString(uuid(300L).toString)))
                  )
                )
              )
            )
            .void
          _ <- PublisherBridge.first(outbox.insertOne(legacyClick)).void
          _ <- PublisherBridge.first(outbox.insertOne(outboxRecord(performed))).void
          _ <- PublisherBridge.first(outbox.insertOne(incompleteLegacyRecord)).void
          failed <- MongoHiringSetup.initialize(database).attempt
          _ = assert(failed.isLeft, "migration must fail while a search click cannot be attributed")
          first <- PublisherBridge.first(outbox.find(org.bson.Document("_id", uuid(1L).toString)))
          _ = assertEquals(
            first.map(_.getList("subjectIds", classOf[String]).asScala.toList),
            Some(List(actor.value.toString))
          )
          ledger <- PublisherBridge.first(
            database
              .getCollection("hiring_migration_ledger")
              .find(
                org.bson.Document("_id", "003_event_outbox_subject_references")
              )
          )
          _ = assertEquals(ledger.map(_.getString("state")), Some("Running"))
          _ <- PublisherBridge
            .first(
              database
                .getCollection("search_sessions")
                .insertOne(
                  MongoHiringCodecs.searchSession(session)
                )
            )
            .void
          _ <- MongoHiringSetup.initialize(database)
          clickDocument <- PublisherBridge.first(outbox.find(org.bson.Document("_id", uuid(2L).toString)))
          performedDocument <- PublisherBridge.first(outbox.find(org.bson.Document("_id", uuid(3L).toString)))
          migratedApplication <- PublisherBridge.first(outbox.find(org.bson.Document("_id", uuid(4L).toString)))
          _ = assertEquals(
            clickDocument.map(_.getList("subjectIds", classOf[String]).asScala.toList),
            Some(List(actor.value.toString, candidate.value.toString).sorted)
          )
          _ = assertEquals(
            performedDocument.map(_.getList("subjectIds", classOf[String]).asScala.toList),
            Some(List(actor.value.toString, candidate.value.toString).sorted)
          )
          _ = assertEquals(
            migratedApplication.map(_.getList("subjectIds", classOf[String]).asScala.toList),
            Some(List(actor.value.toString, candidate.value.toString).sorted)
          )
          _ = assertEquals(migratedApplication.map(_.getInteger("subjectRefsVersion")).map(_.intValue), Some(1))
          indexes <- PublisherBridge.collectWithin(outbox.listIndexes(), 100)
          _ = assert(indexes.exists(_.getString("name") == MongoHiringSetup.EventOutboxSubjectIdsIndex))
          complete <- PublisherBridge.first(
            database
              .getCollection("hiring_migration_ledger")
              .find(
                org.bson.Document("_id", "003_event_outbox_subject_references")
              )
          )
          _ = assertEquals(complete.map(_.getString("state")), Some("Complete"))
          invalidInsert <- PublisherBridge
            .first(
              outbox.insertOne(
                new Document("_id", "invalid-subject-reference")
                  .append("subjectIds", List.empty[String].asJava)
                  .append("subjectRefsVersion", 1)
              )
            )
            .attempt
          _ = assert(invalidInsert.isLeft, "Mongo validator must reject empty subject references")
        } yield ()
      }
    }
  }

  test("report generations hide snapshots and reject stale or conflicting batch publications") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        val database = client.getDatabase(s"analytics_publication_${UUID.randomUUID()}")
        val reports = MongoAnalyticsReportRepository.transactional(database, client)
        val erasures = MongoAnalyticsErasureRequestRepository.transactional(database, client)
        val runner = MongoTransactionRunner.sessions(client, RepositoryError.Conflict)
        val deletedUser = UserId(UUID.randomUUID())
        val reservationExpiry = now.plusSeconds(90L * 24L * 60L * 60L)
        val reportExpiry = now.plusSeconds(120L * 24L * 60L * 60L)

        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- PublisherBridge.first(
            database
              .getCollection("users")
              .insertOne(
                new Document("_id", deletedUser.value.toString)
                  .append("role", "Candidate")
                  .append("accountStatus", "Deleted")
                  .append("version", 0L)
              )
          )
          workerMissing <- erasures.workerReady(now)
          _ = assertEquals(workerMissing, Left(RepositoryError.Unavailable))
          _ <- PublisherBridge.first(
            database
              .getCollection("analytics_worker_heartbeats")
              .insertOne(
                new Document("_id", "analytics-erasure")
                  .append("state", "Ready")
                  .append("leaseUntil", java.util.Date.from(now.plusSeconds(60L)))
              )
          )
          workerReady <- erasures.workerReady(now)
          _ = assertEquals(workerReady, Right(()))
          initial <- PublisherBridge.first(
            database.getCollection("analytics_report_control").find(new Document("_id", "analytics-report"))
          )
          _ = assertEquals(initial.map(_.getLong("generation").longValue()), Option(0L))
          _ = assertEquals(initial.map(_.getString("state")), Some("Unpublished"))
          rolledBack <- runner.run { session =>
            erasures.enqueue(deletedUser, now, MongoMutationWriteContext(session)).flatMap {
              case Right(_)    => IO.pure(Left(RepositoryError.Unavailable))
              case Left(error) => IO.pure(Left(error))
            }
          }
          _ = assertEquals(rolledBack, Left(RepositoryError.Unavailable))
          afterRollback <- PublisherBridge.first(
            database.getCollection("analytics_report_control").find(new Document("_id", "analytics-report"))
          )
          requestAfterRollback <- PublisherBridge.first(
            database.getCollection("analytics_erasure_requests").find(new Document("_id", deletedUser.value.toString))
          )
          _ = assertEquals(afterRollback.map(_.getLong("generation").longValue()), Option(0L))
          _ = assertEquals(requestAfterRollback, None)
          oldRun <- reports.reserve("batch-old", "range-old", now, reservationExpiry)
          sameOldRun <- reports.reserve("batch-old", "range-old", now, reservationExpiry)
          conflictingOldRun <- reports.reserve("batch-old", "different-range", now, reservationExpiry)
          newerRun <- reports.reserve("batch-new", "range-new", now, reservationExpiry)
          _ = assertEquals(oldRun, sameOldRun)
          _ = assertEquals(conflictingOldRun, Left(RepositoryError.Conflict))
          oldReservation <- right(oldRun)
          newerReservation <- right(newerRun)
          newerPublished <- reports.publish(newerReservation, report(now.plusSeconds(2), 2L), reportExpiry)
          stalePublished <- reports.publish(oldReservation, report(now.plusSeconds(1), 1L), reportExpiry)
          _ = assertEquals(newerPublished, Right(()))
          _ = assertEquals(stalePublished, Left(RepositoryError.Conflict))
          retryAfterNewer <- reports.reserve("batch-old", "range-old", now.plusSeconds(2L), reservationExpiry)
          retryAfterNewerReservation <- right(retryAfterNewer)
          _ = assertEquals(retryAfterNewerReservation.generation, oldReservation.generation)
          _ = assert(retryAfterNewerReservation.revision > newerReservation.revision)
          visible <- reports.latest
          _ = assertEquals(visible.map(_.map(_.funnel.head.created)), Right(Some(2L)))
          _ <- PublisherBridge.first(
            database
              .getCollection("analytics_report_snapshots")
              .updateOne(
                new Document("_id", "current"),
                com.mongodb.client.model.Updates.set("expiresAt", java.util.Date.from(now.minusSeconds(1L)))
              )
          )
          expiredSnapshot <- reports.latest
          _ = assertEquals(expiredSnapshot, Right(None))
          restoredRetry <- reports.publish(
            newerReservation,
            report(now.plusSeconds(2), 2L),
            reportExpiry
          )
          _ = assertEquals(restoredRetry, Right(()))
          visibleAfterRetry <- reports.latest
          _ = assertEquals(visibleAfterRetry.map(_.map(_.funnel.head.created)), Right(Some(2L)))
          _ <- MongoHiringSetup.initialize(database)
          receiptIdResult <- erasures.enqueue(deletedUser, now.plusSeconds(3L), MutationWriteContext.noop)
          receiptId <- receiptIdResult.fold(error => IO.raiseError[String](new AssertionError(error.toString)), IO.pure)
          pendingStatus <- erasures.statusForSubject(deletedUser, receiptId)
          _ = assertEquals(pendingStatus, Right(com.example.graphQL.cats.domain.model.AccountDeletionStatus.Pending))
          foreignPendingStatus <- erasures.statusForSubject(UserId(UUID.randomUUID()), receiptId)
          _ = assertEquals(
            foreignPendingStatus,
            Right(com.example.graphQL.cats.domain.model.AccountDeletionStatus.NotFound)
          )
          hiddenBeforeCompletion <- reports.latest
          _ = assertEquals(hiddenBeforeCompletion, Right(None))
          generationAfterDelete <- PublisherBridge.first(
            database.getCollection("analytics_report_control").find(new Document("_id", "analytics-report"))
          )
          _ = assertEquals(generationAfterDelete.map(_.getLong("generation").longValue()), Option(1L))
          refreshedRetry <- reports.reserve("batch-old", "range-old", now.plusSeconds(3L), reservationExpiry)
          refreshedReservation <- right(refreshedRetry)
          _ = assertEquals(refreshedReservation.generation, 1L)
          _ = assert(refreshedReservation.revision > oldReservation.revision)
          oldGenerationPublish <- reports.publish(newerReservation, report(now.plusSeconds(4), 3L), reportExpiry)
          _ = assertEquals(oldGenerationPublish, Left(RepositoryError.Conflict))
          _ <- PublisherBridge.first(
            database
              .getCollection("event_outbox")
              .insertOne(
                new Document("_id", "delete-subject-event")
                  .append("subjectIds", List(deletedUser.value.toString).asJava)
                  .append("subjectRefsVersion", Integer.valueOf(1))
              )
          )
          purged <- erasures.purgeSubjectOutbox(deletedUser)
          _ = assertEquals(purged, Right(()))
          remainingOutbox <- PublisherBridge.first(
            database.getCollection("event_outbox").find(new Document("_id", "delete-subject-event"))
          )
          _ = assertEquals(remainingOutbox, None)
          _ <- erasures.markComplete(deletedUser, now.plusSeconds(4L)).flatMap {
            case Right(())   => IO.unit
            case Left(error) => IO.raiseError(new AssertionError(s"Could not complete erasure request: $error"))
          }
          completed <- erasures.statusForSubject(deletedUser, receiptId)
          _ = assertEquals(completed, Right(com.example.graphQL.cats.domain.model.AccountDeletionStatus.Complete))
          foreignCompletedStatus <- erasures.statusForSubject(UserId(UUID.randomUUID()), receiptId)
          _ = assertEquals(
            foreignCompletedStatus,
            Right(com.example.graphQL.cats.domain.model.AccountDeletionStatus.NotFound)
          )
          completedRequest <- PublisherBridge.first(
            database.getCollection("analytics_erasure_requests").find(new Document("_id", deletedUser.value.toString))
          )
          _ = assertEquals(completedRequest.map(_.getString("state")), Some("Complete"))
          _ = assert(
            completedRequest
              .flatMap(document => Option(document.getDate("expiresAt")))
              .exists(_.after(java.util.Date.from(now)))
          )
          duplicateEnqueue <- erasures.enqueue(deletedUser, now.plusSeconds(5L), MutationWriteContext.noop)
          _ = assertEquals(duplicateEnqueue, Right(receiptId))
          generationAfterDuplicateEnqueue <- PublisherBridge.first(
            database.getCollection("analytics_report_control").find(new Document("_id", "analytics-report"))
          )
          _ = assertEquals(generationAfterDuplicateEnqueue.map(_.getLong("generation").longValue()), Option(1L))
          _ <- PublisherBridge.first(
            database
              .getCollection("analytics_erasure_requests")
              .deleteOne(new Document("_id", deletedUser.value.toString))
          )
          completionSurvivesMarkerTtl <- erasures.statusForSubject(deletedUser, receiptId)
          _ = assertEquals(
            completionSurvivesMarkerTtl,
            Right(com.example.graphQL.cats.domain.model.AccountDeletionStatus.Complete)
          )
          enqueueAfterMarkerTtl <- erasures.enqueue(deletedUser, now.plusSeconds(6L), MutationWriteContext.noop)
          _ = assertEquals(enqueueAfterMarkerTtl, Right(receiptId))
          noRecreatedMarker <- PublisherBridge.first(
            database.getCollection("analytics_erasure_requests").find(new Document("_id", deletedUser.value.toString))
          )
          _ = assertEquals(noRecreatedMarker, None)
          generationAfterTtlReplay <- PublisherBridge.first(
            database.getCollection("analytics_report_control").find(new Document("_id", "analytics-report"))
          )
          _ = assertEquals(generationAfterTtlReplay.map(_.getLong("generation").longValue()), Option(1L))
          postDeleteRun <- reports.reserve("batch-after-delete", "range-after-delete", now, reservationExpiry)
          postDeleteReservation <- right(postDeleteRun)
          hiddenPublish <- reports.publish(postDeleteReservation, report(now.plusSeconds(5), 3L), reportExpiry)
          _ = assertEquals(hiddenPublish, Left(RepositoryError.Conflict))
          visibleAfterCompletion <- reports.latest
          _ = assertEquals(visibleAfterCompletion, Right(None))
        } yield ()
      }
    }
  }

  test("account deletion returns and replays a durable pending receipt with status independent of the account") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        val database = client.getDatabase(s"account_deletion_receipts_${UUID.randomUUID()}")
        val users = MongoUserRepository.transactional(database, client)
        val mutationReceipts = MongoMutationReceiptRepository.transactional(database, client)
        val erasures = MongoAnalyticsErasureRequestRepository.transactional(database, client)
        val userId = UserId(UUID.randomUUID())
        val actor = ActorContext(userId, UserRole.Recruiter)
        val serviceNow = Instant.now()
        val request = IdempotencyRequest.fromCanonicalInput(
          UUID.fromString("00000000-0000-0000-0000-000000000456"),
          "{\"idempotencyKey\":\"00000000-0000-0000-0000-000000000456\"}"
        )
        val service = UserAccountService(
          users,
          users,
          NoopPasswordHasher,
          NoopAccessTokenIssuer,
          erasures,
          idempotent = Idempotent(mutationReceipts),
          currentTime = IO.pure(serviceNow)
        )

        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- users.insert(recruiterUser(userId)).flatMap {
            case Right(())   => IO.unit
            case Left(error) => IO.raiseError(new AssertionError(s"Could not insert deletion-test user: $error"))
          }
          _ <- PublisherBridge.first(
            database
              .getCollection("analytics_worker_heartbeats")
              .insertOne(
                new Document("_id", "analytics-erasure")
                  .append("state", "Ready")
                  .append("leaseUntil", java.util.Date.from(serviceNow.plusSeconds(60L)))
              )
          )
          initial <- service.deleteMyAccount(request, actor).value
          receiptId <- initial.fold(
            error => IO.raiseError[String](new AssertionError(s"Account deletion failed: $error")),
            IO.pure
          )
          pending <- service.accountDeletionStatus(actor, receiptId).value
          _ = assertEquals(pending, Right(AccountDeletionStatus.Pending))
          foreignPending <- service
            .accountDeletionStatus(ActorContext(UserId(UUID.randomUUID()), UserRole.Candidate), receiptId)
            .value
          _ = assertEquals(foreignPending, Right(AccountDeletionStatus.NotFound))
          deleted <- users.find(userId)
          _ = assertEquals(deleted.map(_.map(_.accountStatus)), Right(Some(AccountStatus.Deleted)))
          mutationReceipt <- PublisherBridge.first(
            database
              .getCollection("mutation_receipts")
              .find(new Document("operation", "deleteMyAccount"))
          )
          _ = assert(mutationReceipt.nonEmpty, "the deletion receipt must be committed with the account tombstone")
          _ = assertEquals(mutationReceipt.map(_.getString("state")), Some("Completed"))
          storedEntity = mutationReceipt.flatMap(value => Option(value.get("entity", classOf[Document])))
          _ = assertEquals(storedEntity.map(_.getString("type")), Some("analytics-erasure-receipt"))
          _ = assertEquals(storedEntity.map(_.getString("id")), Some(receiptId))
          erasureRequest <- PublisherBridge.first(
            database
              .getCollection("analytics_erasure_requests")
              .find(new Document("_id", userId.value.toString))
          )
          _ = assertEquals(erasureRequest.map(_.getString("receiptId")), Some(receiptId))
          replay <- service.deleteMyAccount(request, actor).value
          _ = assertEquals(replay, Right(receiptId))
          pendingAfterReplay <- service.accountDeletionStatus(actor, receiptId).value
          _ = assertEquals(pendingAfterReplay, Right(AccountDeletionStatus.Pending))
          completion <- erasures.markComplete(userId, serviceNow.plusSeconds(1L))
          _ = assertEquals(completion, Right(()))
          complete <- service.accountDeletionStatus(actor, receiptId).value
          _ = assertEquals(complete, Right(AccountDeletionStatus.Complete))
          foreignComplete <- service
            .accountDeletionStatus(ActorContext(UserId(UUID.randomUUID()), UserRole.Candidate), receiptId)
            .value
          _ = assertEquals(foreignComplete, Right(AccountDeletionStatus.NotFound))
        } yield ()
      }
    }
  }

  private object NoopPasswordHasher extends PasswordHasher {
    override def hash(password: String): IO[PasswordHash] = IO.pure(PasswordHash.fromEncoded(password))
    override def verify(encoded: PasswordHash, password: String): IO[Boolean] = IO.pure(encoded.encoded == password)
    override def verifyUnknown(password: String): IO[Unit] = IO.unit
  }

  private object NoopAccessTokenIssuer extends AccessTokenIssuer {
    override def issue(user: User, at: Instant): IO[Either[AccessTokenIssuanceError, AccountToken]] =
      IO.raiseError(new AssertionError("Deletion receipt replay must not issue an account token"))
  }

  test("deletion request captures every publisher transactional ID from its subject fence") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        val database = client.getDatabase(s"analytics_fencing_${UUID.randomUUID()}")
        val erasures = MongoAnalyticsErasureRequestRepository.transactional(database, client)
        val runner = MongoTransactionRunner.sessions(client, RepositoryError.Conflict)
        val userId = UserId(UUID.randomUUID())
        val transactionalIds = List("hiring-publisher-generation-a", "hiring-publisher-generation-b")
        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- PublisherBridge.first(
            database
              .getCollection("outbox_subject_fences")
              .insertOne(
                new Document("_id", userId.value.toString)
                  .append("deleted", false)
                  .append("transactionalIds", transactionalIds.asJava)
              )
          )
          _ <- PublisherBridge.first(
            database
              .getCollection("analytics_worker_heartbeats")
              .insertOne(
                new Document("_id", "analytics-erasure")
                  .append("state", "Ready")
                  .append("leaseUntil", java.util.Date.from(now.plusSeconds(60L)))
              )
          )
          receipt <- runner.run(session => erasures.enqueue(userId, now, MongoMutationWriteContext(session)))
          _ = assert(receipt.isRight)
          request <- PublisherBridge.first(
            database
              .getCollection("analytics_erasure_requests")
              .find(new Document("_id", userId.value.toString))
          )
          storedIds = request.toList
            .flatMap(document => document.getList("transactionalIds", classOf[String]).asScala.toList)
          _ = assertEquals(storedIds, transactionalIds)
          fencingVersion = request.flatMap(value => Option(value.getInteger("fencingVersion"))).map(_.intValue())
          _ = assertEquals(fencingVersion, Option(1))
        } yield ()
      }
    }
  }

  test("outbox claim records its transactional publisher ID on every subject fence") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        val database = client.getDatabase(s"analytics_claim_fencing_${UUID.randomUUID()}")
        val outbox = MongoOperationalEventOutboxRepository.transactional(database, client)
        val actor = UserId(UUID.randomUUID())
        val value = event(
          700L,
          OperationalEventType.JOB_CREATED,
          OperationalAggregateType.Job,
          uuid(701L),
          actor,
          Json.obj("job" -> Json.obj("jobId" -> Json.fromString(uuid(701L).toString)))
        )
        val secondValue = value.copy(eventId = uuid(702L))
        val transactionalId = "hiring-publisher-test-generation"
        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- PublisherBridge.first(
            database
              .getCollection("event_outbox")
              .insertMany(List(outboxRecord(value), outboxRecord(secondValue)).asJava)
          )
          claimed <- outbox.claim("fence-test", transactionalId, now, now.plusSeconds(60L), 50)
          _ = assert(claimed.exists(_.size == 1), clues(claimed))
          fence <- PublisherBridge.first(
            database
              .getCollection("outbox_subject_fences")
              .find(new Document("_id", actor.value.toString))
          )
          storedIds = fence.toList.flatMap(_.getList("transactionalIds", classOf[String]).asScala.toList)
          _ = assertEquals(storedIds, List(transactionalId))
        } yield ()
      }
    }
  }

  test("expired-lease reclaim and account deletion serialize the publisher subject fence") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        val database = client.getDatabase(s"analytics_fence_race_${UUID.randomUUID()}")
        val userId = UserId(UUID.randomUUID())
        val firstPublisher = "hiring-publisher-generation-a"
        val secondPublisher = "hiring-publisher-generation-b"
        val racingPublisher = "hiring-publisher-generation-c"
        val eventValue = event(
          800L,
          OperationalEventType.JOB_CREATED,
          OperationalAggregateType.Job,
          uuid(801L),
          userId,
          Json.obj("job" -> Json.obj("jobId" -> Json.fromString(uuid(801L).toString)))
        )
        val erasures = MongoAnalyticsErasureRequestRepository.transactional(database, client)
        val outbox = MongoOperationalEventOutboxRepository.transactional(database, client)
        val users = MongoUserRepository.transactional(database, client)
        val runner = MongoTransactionRunner.sessions(client, RepositoryError.Conflict)
        val deletionAt = now.plusSeconds(122L)

        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- users.insert(recruiterUser(userId)).flatMap {
            case Right(())   => IO.unit
            case Left(error) => IO.raiseError(new AssertionError(s"Could not insert race-test user: $error"))
          }
          _ <- PublisherBridge.first(database.getCollection("event_outbox").insertOne(outboxRecord(eventValue)))
          _ <- PublisherBridge.first(
            database
              .getCollection("analytics_worker_heartbeats")
              .insertOne(
                new Document("_id", "analytics-erasure")
                  .append("state", "Ready")
                  .append("leaseUntil", java.util.Date.from(deletionAt.plusSeconds(300L)))
              )
          )
          firstClaim <- outbox.claim(
            "publisher-a",
            firstPublisher,
            now,
            now.plusSeconds(60L),
            1
          )
          _ = assertEquals(firstClaim.map(_.map(_.event.eventId)), Right(List(eventValue.eventId)))
          expiredLeaseClaim <- outbox.claim(
            "publisher-b",
            secondPublisher,
            now.plusSeconds(61L),
            now.plusSeconds(121L),
            1
          )
          _ = assertEquals(expiredLeaseClaim.map(_.map(_.event.eventId)), Right(List(eventValue.eventId)))
          fenceBeforeRace <- PublisherBridge.first(
            database.getCollection("outbox_subject_fences").find(new Document("_id", userId.value.toString))
          )
          generationsBeforeRace = fenceBeforeRace.toList
            .flatMap(_.getList("transactionalIds", classOf[String]).asScala.toList)
          _ = assertEquals(generationsBeforeRace, List(firstPublisher, secondPublisher))
          requestWritten <- Deferred[IO, Unit]
          allowDeletionToContinue <- Deferred[IO, Unit]
          deletionFiber <- runner.run { session =>
            val context = MongoMutationWriteContext(session)
            erasures.enqueue(userId, deletionAt, context).flatMap {
              case Left(error)      => IO.pure(Left(error))
              case Right(receiptId) =>
                requestWritten.complete(()).void *>
                  allowDeletionToContinue.get *>
                  users.deleteAccount(userId, deletionAt, "deleted-race-account", context).map(_.map(_ => receiptId))
            }
          }.start
          _ <- requestWritten.get
          racingClaim <- outbox.claim(
            "publisher-c",
            racingPublisher,
            deletionAt,
            deletionAt.plusSeconds(60L),
            1
          )
          _ = assertEquals(racingClaim.map(_.map(_.event.eventId)), Right(List(eventValue.eventId)))
          _ <- allowDeletionToContinue.complete(()).void
          firstDeletion <- deletionFiber.joinWithNever
          firstRequest <- PublisherBridge.first(
            database.getCollection("analytics_erasure_requests").find(new Document("_id", userId.value.toString))
          )
          _ = firstDeletion.foreach { _ =>
            val captured = firstRequest.toList.flatMap(
              _.getList("transactionalIds", classOf[String]).asScala.toList
            )
            assertEquals(captured, List(firstPublisher, secondPublisher, racingPublisher))
          }
          _ = assert(firstDeletion.isRight || firstRequest.isEmpty, clues(firstDeletion, firstRequest))
          finalDeletion <- firstDeletion match {
            case right @ Right(_) => IO.pure(right)
            case Left(_)          =>
              runner.run { session =>
                val context = MongoMutationWriteContext(session)
                erasures.enqueue(userId, deletionAt.plusSeconds(1L), context).flatMap {
                  case Left(error)      => IO.pure(Left(error))
                  case Right(receiptId) =>
                    users
                      .deleteAccount(userId, deletionAt.plusSeconds(1L), "deleted-race-account", context)
                      .map(_.map(_ => receiptId))
                }
              }
          }
          receiptId <- finalDeletion.fold(
            error => IO.raiseError[String](new AssertionError(s"Deletion retry failed after fence race: $error")),
            IO.pure
          )
          requestAfterDeletion <- PublisherBridge.first(
            database.getCollection("analytics_erasure_requests").find(new Document("_id", userId.value.toString))
          )
          capturedAfterDeletion = requestAfterDeletion.toList.flatMap(
            _.getList("transactionalIds", classOf[String]).asScala.toList
          )
          _ = assertEquals(capturedAfterDeletion, List(firstPublisher, secondPublisher, racingPublisher))
          storedUser <- PublisherBridge.first(
            database.getCollection("users").find(new Document("_id", userId.value.toString))
          )
          _ = assertEquals(storedUser.map(_.getString("accountStatus")), Some("Deleted"))
          _ = assertEquals(requestAfterDeletion.map(_.getString("receiptId")), Some(receiptId))
          lateClaim <- outbox.claim(
            "publisher-d",
            "hiring-publisher-generation-d",
            deletionAt.plusSeconds(200L),
            deletionAt.plusSeconds(260L),
            1
          )
          _ = assertEquals(lateClaim, Right(Nil))
          outboxAfterLateClaim <- PublisherBridge.first(
            database.getCollection("event_outbox").find(new Document("_id", eventValue.eventId.toString))
          )
          _ = assertEquals(outboxAfterLateClaim.map(_.getString("state")), Some("Failed"))
          status <- erasures.statusForSubject(userId, receiptId)
          _ = assertEquals(status, Right(com.example.graphQL.cats.domain.model.AccountDeletionStatus.Pending))
        } yield ()
      }
    }
  }

  private def recruiterUser(id: UserId): User =
    User(
      id,
      Some(email(s"$id@example.com")),
      "Recruiter",
      UserRole.Recruiter,
      Some(UserProfile.Recruiter(RecruiterProfile("Hiring Co", Some("Lead Recruiter")))),
      now
    )
}
