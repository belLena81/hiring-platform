package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.AccountValueFixtures.email
import cats.effect.{Deferred, IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.repository.mongo.MongoRepositoryTestSupport.*
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
import com.example.graphQL.cats.service.{AnalyticsFunnelDay, AnalyticsReportSnapshot, AnalyticsSkillPostingDay}
import com.example.graphQL.cats.service.port.{
  AnalyticsRangeFingerprint,
  AnalyticsRunId,
  MutationWriteContext,
  RepositoryError,
  RepositoryIO
}
import com.example.graphQL.cats.service.events.{
  OperationalAggregateType,
  OperationalEventEnvelope,
  OperationalEventType,
  OperationalEvents,
  SearchSession,
  SearchSessionResult
}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics}
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
import org.bson.Document
import com.example.hiring.testing.LocalTestServices

import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

class MongoOutboxSubjectReferencesIntegrationSpec extends MongoIntegrationSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private val now = Instant.parse("2026-09-22T12:00:00Z")
  private def replicaSet: Resource[IO, LocalTestServices.MongoEndpoint] = Resource.pure(mongoEndpoint)
  private def uri(instance: LocalTestServices.MongoEndpoint): String = instance.uri

  private def uuid(value: Long): UUID = new UUID(0L, value)
  private def runId(value: String): AnalyticsRunId = AnalyticsRunId.from(value).getOrElse(fail("invalid test run ID"))
  private def fingerprint(value: String): AnalyticsRangeFingerprint =
    AnalyticsRangeFingerprint.from(value).getOrElse(fail("invalid test range fingerprint"))

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
      MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        LocalTestServices.database(client).use { database =>
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
          val legacyClick = outboxRecord(click.fold(error => fail(error.toString), identity))
          legacyClick.remove("subjectRefsVersion")
          val performed =
            OperationalEvents.searchPerformed(uuid(3L), session).fold(error => fail(error.toString), identity)
          val application = event(
            4L,
            OperationalEventType.APPLICATION_CREATED,
            OperationalAggregateType.Application,
            uuid(400L),
            actor,
            Json.obj(
              "applicationId" -> Json.fromString(uuid(400L).toString),
              "candidateId" -> Json.fromString(candidate.value.toString),
              "jobId" -> Json.fromString(uuid(401L).toString),
              "status" -> Json.fromString("Created")
            )
          )
          val incompleteLegacyRecord = outboxRecord(application)
            .append("subjectIds", List(actor.value.toString).asJava)
          incompleteLegacyRecord.remove("subjectRefsVersion")
          val outbox = database.getCollection(MongoCollections.EventOutbox)

          for {
            _ <- MongoRepositoryTestSupport
              .first(
                outbox.insertOne(
                  outboxRecord(
                    event(
                      1L,
                      OperationalEventType.JOB_CREATED,
                      OperationalAggregateType.Job,
                      uuid(300L),
                      actor,
                      Json.obj(
                        "job" -> Json.obj(
                          "jobId" -> Json.fromString(uuid(300L).toString),
                          "skills" -> Json.arr(Json.fromString("Scala")),
                          "status" -> Json.fromString("Open")
                        )
                      )
                    )
                  )
                )
              )
              .void
            _ <- MongoRepositoryTestSupport.first(outbox.insertOne(legacyClick)).void
            _ <- MongoRepositoryTestSupport.first(outbox.insertOne(outboxRecord(performed))).void
            _ <- MongoRepositoryTestSupport.first(outbox.insertOne(incompleteLegacyRecord)).void
            failed <- MongoHiringSetup.initialize(database, Diagnostics.noop).attempt
            _ = assert(failed.isLeft, "migration must fail while a search click cannot be attributed")
            first <- MongoRepositoryTestSupport.first(outbox.find(org.bson.Document("_id", uuid(1L).toString)))
            _ = assertEquals(
              first.map(_.getList("subjectIds", classOf[String]).asScala.toList),
              Some(List(actor.value.toString))
            )
            ledger <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.HiringMigrationLedger)
                .find(
                  org.bson.Document("_id", "003_event_outbox_subject_references")
                )
            )
            _ = assertEquals(ledger.map(_.getString("state")), Some("Running"))
            _ <- MongoRepositoryTestSupport
              .first(
                database
                  .getCollection(MongoCollections.SearchSessions)
                  .insertOne(
                    MongoHiringCodecs.searchSession(session)
                  )
              )
              .void
            _ <- MongoHiringSetup.initialize(database, Diagnostics.noop)
            clickDocument <- MongoRepositoryTestSupport.first(outbox.find(org.bson.Document("_id", uuid(2L).toString)))
            performedDocument <- MongoRepositoryTestSupport.first(
              outbox.find(org.bson.Document("_id", uuid(3L).toString))
            )
            migratedApplication <- MongoRepositoryTestSupport.first(
              outbox.find(org.bson.Document("_id", uuid(4L).toString))
            )
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
            indexes <- MongoRepositoryTestSupport.collectWithin(outbox.listIndexes(), 100)
            _ = assert(indexes.exists(_.getString("name") == MongoHiringSetup.EventOutboxSubjectIdsIndex))
            complete <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.HiringMigrationLedger)
                .find(
                  org.bson.Document("_id", "003_event_outbox_subject_references")
                )
            )
            _ = assertEquals(complete.map(_.getString("state")), Some("Complete"))
            invalidInsert <- MongoRepositoryTestSupport
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
  }

  test("report generations hide snapshots and reject stale or conflicting batch publications") {
    replicaSet.use { instance =>
      MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        LocalTestServices.database(client).use { database =>
          val reports = MongoAnalyticsReportRepository.transactional(database, client, Diagnostics.noop)
          val erasures = MongoAnalyticsErasureRequestRepository.transactional(database, client, Diagnostics.noop)
          val runner = MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = Diagnostics.noop)
          val deletedUser = UserId(UUID.randomUUID())
          val reservationExpiry = now.plusSeconds(90L * 24L * 60L * 60L)
          val reportExpiry = now.plusSeconds(120L * 24L * 60L * 60L)

          for {
            _ <- MongoHiringSetup.initialize(database, Diagnostics.noop)
            _ <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.Users)
                .insertOne(
                  new Document("_id", deletedUser.value.toString)
                    .append("role", "Candidate")
                    .append("accountStatus", "Deleted")
                    .append("version", 0L)
                )
            )
            workerMissing <- erasures.workerReady(now).value
            _ = assertEquals(workerMissing, Left(RepositoryError.Unavailable))
            _ <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsWorkerHeartbeats)
                .insertOne(
                  new Document("_id", "analytics-erasure")
                    .append("state", "Ready")
                    .append("leaseUntil", java.util.Date.from(now.plusSeconds(60L)))
                )
            )
            workerReady <- erasures.workerReady(now).value
            _ = assertEquals(workerReady, Right(()))
            initial <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsReportControl)
                .find(new Document("_id", "analytics-report"))
            )
            _ = assertEquals(initial.map(_.getLong("generation").longValue()), Option(0L))
            _ = assertEquals(initial.map(_.getString("state")), Some("Unpublished"))
            rolledBack <- runner.run { session =>
              erasures.enqueue(deletedUser, now, MongoMutationWriteContext(session)) *>
                RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
            }.value
            _ = assertEquals(rolledBack, Left(RepositoryError.Unavailable))
            afterRollback <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsReportControl)
                .find(new Document("_id", "analytics-report"))
            )
            requestAfterRollback <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsErasureRequests)
                .find(new Document("_id", deletedUser.value.toString))
            )
            _ = assertEquals(afterRollback.map(_.getLong("generation").longValue()), Option(0L))
            _ = assertEquals(requestAfterRollback, None)
            oldRun <- reports.reserve(runId("batch-old"), fingerprint("range-old"), now, reservationExpiry).value
            sameOldRun <- reports.reserve(runId("batch-old"), fingerprint("range-old"), now, reservationExpiry).value
            conflictingOldRun <- reports
              .reserve(
                runId("batch-old"),
                fingerprint("different-range"),
                now,
                reservationExpiry
              )
              .value
            newerRun <- reports.reserve(runId("batch-new"), fingerprint("range-new"), now, reservationExpiry).value
            _ = assertEquals(oldRun, sameOldRun)
            _ = assertEquals(conflictingOldRun, Left(RepositoryError.Conflict))
            oldReservation <- right(oldRun)
            newerReservation <- right(newerRun)
            newerPublished <- reports.publish(newerReservation, report(now.plusSeconds(2), 2L), reportExpiry).value
            stalePublished <- reports.publish(oldReservation, report(now.plusSeconds(1), 1L), reportExpiry).value
            _ = assertEquals(newerPublished, Right(()))
            _ = assertEquals(stalePublished, Left(RepositoryError.Conflict))
            retryAfterNewer <- reports
              .reserve(
                runId("batch-old"),
                fingerprint("range-old"),
                now.plusSeconds(2L),
                reservationExpiry
              )
              .value
            retryAfterNewerReservation <- right(retryAfterNewer)
            _ = assertEquals(retryAfterNewerReservation.generation, oldReservation.generation)
            _ = assert(retryAfterNewerReservation.revision > newerReservation.revision)
            visible <- reports.latest.value
            _ = assertEquals(visible.map(_.map(_.funnel.head.created)), Right(Some(2L)))
            _ <- MongoRepositoryTestSupport.first(
              MongoRepositoryTestSupport
                .collection(database, MongoCollections.AnalyticsReportSnapshots)
                .flatMap(
                  _.updateOne(
                    new Document("_id", "current"),
                    com.mongodb.client.model.Updates.set("expiresAt", java.util.Date.from(now.minusSeconds(1L)))
                  )
                )
            )
            expiredSnapshot <- reports.latest.value
            _ = assertEquals(expiredSnapshot, Right(None))
            restoredRetry <- reports
              .publish(
                newerReservation,
                report(now.plusSeconds(2), 2L),
                reportExpiry
              )
              .value
            _ = assertEquals(restoredRetry, Right(()))
            visibleAfterRetry <- reports.latest.value
            _ = assertEquals(visibleAfterRetry.map(_.map(_.funnel.head.created)), Right(Some(2L)))
            _ <- MongoHiringSetup.initialize(database, Diagnostics.noop)
            receiptIdResult <- erasures
              .enqueue(deletedUser, now.plusSeconds(3L), MutationWriteContext.directWrite)
              .value
            receiptId <- receiptIdResult.fold(
              error => IO.raiseError[String](new AssertionError(error.toString)),
              IO.pure
            )
            pendingStatus <- erasures.statusForSubject(deletedUser, receiptId).value
            _ = assertEquals(pendingStatus, Right(com.example.graphQL.cats.domain.model.AccountDeletionStatus.Pending))
            foreignPendingStatus <- erasures.statusForSubject(UserId(UUID.randomUUID()), receiptId).value
            _ = assertEquals(
              foreignPendingStatus,
              Right(com.example.graphQL.cats.domain.model.AccountDeletionStatus.NotFound)
            )
            hiddenBeforeCompletion <- reports.latest.value
            _ = assertEquals(hiddenBeforeCompletion, Right(None))
            generationAfterDelete <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsReportControl)
                .find(new Document("_id", "analytics-report"))
            )
            _ = assertEquals(generationAfterDelete.map(_.getLong("generation").longValue()), Option(1L))
            refreshedRetry <- reports
              .reserve(
                runId("batch-old"),
                fingerprint("range-old"),
                now.plusSeconds(3L),
                reservationExpiry
              )
              .value
            refreshedReservation <- right(refreshedRetry)
            _ = assertEquals(refreshedReservation.generation, 1L)
            _ = assert(refreshedReservation.revision > oldReservation.revision)
            oldGenerationPublish <- reports
              .publish(newerReservation, report(now.plusSeconds(4), 3L), reportExpiry)
              .value
            _ = assertEquals(oldGenerationPublish, Left(RepositoryError.Conflict))
            _ <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.EventOutbox)
                .insertOne(
                  new Document("_id", "delete-subject-event")
                    .append("subjectIds", List(deletedUser.value.toString).asJava)
                    .append("subjectRefsVersion", Integer.valueOf(1))
                )
            )
            purged <- erasures.purgeSubjectOutbox(deletedUser).value
            _ = assertEquals(purged, Right(()))
            remainingOutbox <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.EventOutbox).find(new Document("_id", "delete-subject-event"))
            )
            _ = assertEquals(remainingOutbox, None)
            _ <- erasures.markComplete(deletedUser, now.plusSeconds(4L)).value.flatMap {
              case Right(())   => IO.unit
              case Left(error) => IO.raiseError(new AssertionError(s"Could not complete erasure request: $error"))
            }
            completed <- erasures.statusForSubject(deletedUser, receiptId).value
            _ = assertEquals(completed, Right(com.example.graphQL.cats.domain.model.AccountDeletionStatus.Complete))
            foreignCompletedStatus <- erasures.statusForSubject(UserId(UUID.randomUUID()), receiptId).value
            _ = assertEquals(
              foreignCompletedStatus,
              Right(com.example.graphQL.cats.domain.model.AccountDeletionStatus.NotFound)
            )
            completedRequest <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsErasureRequests)
                .find(new Document("_id", deletedUser.value.toString))
            )
            _ = assertEquals(completedRequest.map(_.getString("state")), Some("Complete"))
            _ = assert(
              completedRequest
                .flatMap(document => Option(document.getDate("expiresAt")))
                .exists(_.after(java.util.Date.from(now)))
            )
            duplicateEnqueue <- erasures
              .enqueue(deletedUser, now.plusSeconds(5L), MutationWriteContext.directWrite)
              .value
            _ = assertEquals(duplicateEnqueue, Right(receiptId))
            generationAfterDuplicateEnqueue <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsReportControl)
                .find(new Document("_id", "analytics-report"))
            )
            _ = assertEquals(generationAfterDuplicateEnqueue.map(_.getLong("generation").longValue()), Option(1L))
            _ <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsErasureRequests)
                .deleteOne(new Document("_id", deletedUser.value.toString))
            )
            completionSurvivesMarkerTtl <- erasures.statusForSubject(deletedUser, receiptId).value
            _ = assertEquals(
              completionSurvivesMarkerTtl,
              Right(com.example.graphQL.cats.domain.model.AccountDeletionStatus.Complete)
            )
            enqueueAfterMarkerTtl <- erasures
              .enqueue(
                deletedUser,
                now.plusSeconds(6L),
                MutationWriteContext.directWrite
              )
              .value
            _ = assertEquals(enqueueAfterMarkerTtl, Right(receiptId))
            noRecreatedMarker <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsErasureRequests)
                .find(new Document("_id", deletedUser.value.toString))
            )
            _ = assertEquals(noRecreatedMarker, None)
            generationAfterTtlReplay <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsReportControl)
                .find(new Document("_id", "analytics-report"))
            )
            _ = assertEquals(generationAfterTtlReplay.map(_.getLong("generation").longValue()), Option(1L))
            postDeleteRun <- reports
              .reserve(
                runId("batch-after-delete"),
                fingerprint("range-after-delete"),
                now,
                reservationExpiry
              )
              .value
            postDeleteReservation <- right(postDeleteRun)
            hiddenPublish <- reports.publish(postDeleteReservation, report(now.plusSeconds(5), 3L), reportExpiry).value
            _ = assertEquals(hiddenPublish, Left(RepositoryError.Conflict))
            visibleAfterCompletion <- reports.latest.value
            _ = assertEquals(visibleAfterCompletion, Right(None))
          } yield ()
        }
      }
    }
  }

  test("account deletion returns and replays a durable pending receipt with status independent of the account") {
    replicaSet.use { instance =>
      MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        LocalTestServices.database(client).use { database =>
          val users = MongoUserRepository.transactional(
            database,
            client,
            new MongoEmbeddingWorkRepository(database, Diagnostics.noop),
            Diagnostics.noop
          )
          val mutationReceipts = MongoMutationReceiptRepository.transactional(database, client, Diagnostics.noop)
          val erasures = MongoAnalyticsErasureRequestRepository.transactional(database, client, Diagnostics.noop)
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
            com.example.graphQL.cats.service.search.TestEmbeddingWorkPublisher.noop,
            idempotent = Idempotent(mutationReceipts),
            diagnostics = Diagnostics.noop,
            clock = com.example.graphQL.cats.FixedTestClock.at(serviceNow)
          )

          for {
            _ <- MongoHiringSetup.initialize(database, Diagnostics.noop)
            _ <- users.insert(recruiterUser(userId)).value.flatMap {
              case Right(())   => IO.unit
              case Left(error) => IO.raiseError(new AssertionError(s"Could not insert deletion-test user: $error"))
            }
            _ <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsWorkerHeartbeats)
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
            deleted <- users.find(userId).value
            _ = assertEquals(deleted.map(_.map(_.accountStatus)), Right(Some(AccountStatus.Deleted)))
            mutationReceipt <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.MutationReceipts)
                .find(new Document("operation", "deleteMyAccount"))
            )
            _ = assert(mutationReceipt.nonEmpty, "the deletion receipt must be committed with the account tombstone")
            _ = assertEquals(mutationReceipt.map(_.getString("state")), Some("Completed"))
            storedEntity = mutationReceipt.flatMap(value => Option(value.get("entity", classOf[Document])))
            _ = assertEquals(storedEntity.map(_.getString("type")), Some("analytics-erasure-receipt"))
            _ = assertEquals(storedEntity.map(_.getString("id")), Some(receiptId))
            erasureRequest <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsErasureRequests)
                .find(new Document("_id", userId.value.toString))
            )
            _ = assertEquals(erasureRequest.map(_.getString("receiptId")), Some(receiptId))
            replay <- service.deleteMyAccount(request, actor).value
            _ = assertEquals(replay, Right(receiptId))
            pendingAfterReplay <- service.accountDeletionStatus(actor, receiptId).value
            _ = assertEquals(pendingAfterReplay, Right(AccountDeletionStatus.Pending))
            completion <- erasures.markComplete(userId, serviceNow.plusSeconds(1L)).value
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
      MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        LocalTestServices.database(client).use { database =>
          val erasures = MongoAnalyticsErasureRequestRepository.transactional(database, client, Diagnostics.noop)
          val runner = MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = Diagnostics.noop)
          val userId = UserId(UUID.randomUUID())
          val transactionalIds = List(
            "hiring-publisher-00000000-0000-0000-0000-000000000385",
            "hiring-publisher-00000000-0000-0000-0000-000000000386"
          )
          for {
            _ <- MongoHiringSetup.initialize(database, Diagnostics.noop)
            _ <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.OutboxSubjectFences)
                .insertOne(
                  new Document("_id", userId.value.toString)
                    .append("deleted", false)
                )
            )
            _ <- transactionalIds.traverse_(id =>
              MongoProducerRegistrations
                .register(database, None, userId.value.toString, id, "Operational", now)
                .value
                .flatMap(result => IO(assert(result.isRight)))
            )
            _ <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsWorkerHeartbeats)
                .insertOne(
                  new Document("_id", "analytics-erasure")
                    .append("state", "Ready")
                    .append("leaseUntil", java.util.Date.from(now.plusSeconds(60L)))
                )
            )
            receipt <- runner.run(session => erasures.enqueue(userId, now, MongoMutationWriteContext(session))).value
            _ = assert(receipt.isRight)
            request <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsErasureRequests)
                .find(new Document("_id", userId.value.toString))
            )
            storedIds <- MongoProducerRegistrations.batch(database, userId.value.toString, "Operational").value
            _ = assertEquals(storedIds.map(_.toList), Right(transactionalIds))
            _ = assertEquals(request.map(_.getBoolean("producerRegistry").booleanValue()), Some(true))
            fencingVersion = request.flatMap(value => Option(value.getInteger("fencingVersion"))).map(_.intValue())
            _ = assertEquals(fencingVersion, Option(1))
          } yield ()
        }
      }
    }
  }

  test("outbox claim records its transactional publisher ID on every subject fence") {
    replicaSet.use { instance =>
      MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        LocalTestServices.database(client).use { database =>
          val outbox = MongoOperationalEventOutboxRepository.transactional(database, client, Diagnostics.noop)
          val actor = UserId(UUID.randomUUID())
          val value = event(
            700L,
            OperationalEventType.JOB_CREATED,
            OperationalAggregateType.Job,
            uuid(701L),
            actor,
            Json.obj(
              "job" -> Json.obj(
                "jobId" -> Json.fromString(uuid(701L).toString),
                "skills" -> Json.arr(Json.fromString("Scala")),
                "status" -> Json.fromString("Open")
              )
            )
          )
          val secondValue = value.copy(eventId = uuid(702L))
          val transactionalId = "hiring-publisher-00000000-0000-0000-0000-000000000389"
          for {
            _ <- MongoHiringSetup.initialize(database, Diagnostics.noop)
            _ <- MongoRepositoryTestSupport.first(
              MongoRepositoryTestSupport
                .collection(database, MongoCollections.EventOutbox)
                .flatMap(_.insertMany(List(outboxRecord(value), outboxRecord(secondValue))))
            )
            claimed <- outbox.claim("fence-test", transactionalId, now, now.plusSeconds(60L), 50).value
            _ = assert(claimed.exists(_.size == 1), clues(claimed))
            fence <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.OutboxSubjectFences)
                .find(new Document("_id", actor.value.toString))
            )
            storedIds <- MongoProducerRegistrations.batch(database, actor.value.toString, "Operational").value
            _ = assertEquals(storedIds, Right(Vector(transactionalId)))
            _ = assert(fence.forall(!_.containsKey("transactionalIds")))
          } yield ()
        }
      }
    }
  }

  test("expired-lease reclaim and account deletion serialize the publisher subject fence") {
    replicaSet.use { instance =>
      MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        LocalTestServices.database(client).use { database =>
          val userId = UserId(UUID.randomUUID())
          val firstPublisher = "hiring-publisher-00000000-0000-0000-0000-000000000385"
          val secondPublisher = "hiring-publisher-00000000-0000-0000-0000-000000000386"
          val racingPublisher = "hiring-publisher-00000000-0000-0000-0000-000000000387"
          val eventValue = event(
            800L,
            OperationalEventType.JOB_CREATED,
            OperationalAggregateType.Job,
            uuid(801L),
            userId,
            Json.obj(
              "job" -> Json.obj(
                "jobId" -> Json.fromString(uuid(801L).toString),
                "skills" -> Json.arr(Json.fromString("Scala")),
                "status" -> Json.fromString("Open")
              )
            )
          )
          val erasures = MongoAnalyticsErasureRequestRepository.transactional(database, client, Diagnostics.noop)
          val outbox = MongoOperationalEventOutboxRepository.transactional(database, client, Diagnostics.noop)
          val users = MongoUserRepository.transactional(
            database,
            client,
            new MongoEmbeddingWorkRepository(database, Diagnostics.noop),
            Diagnostics.noop
          )
          val runner = MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = Diagnostics.noop)
          val deletionAt = now.plusSeconds(122L)

          for {
            _ <- MongoHiringSetup.initialize(database, Diagnostics.noop)
            _ <- users.insert(recruiterUser(userId)).value.flatMap {
              case Right(())   => IO.unit
              case Left(error) => IO.raiseError(new AssertionError(s"Could not insert race-test user: $error"))
            }
            _ <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.EventOutbox).insertOne(outboxRecord(eventValue))
            )
            _ <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsWorkerHeartbeats)
                .insertOne(
                  new Document("_id", "analytics-erasure")
                    .append("state", "Ready")
                    .append("leaseUntil", java.util.Date.from(deletionAt.plusSeconds(300L)))
                )
            )
            firstClaim <- outbox
              .claim(
                "publisher-a",
                firstPublisher,
                now,
                now.plusSeconds(60L),
                1
              )
              .value
            _ = assertEquals(firstClaim.map(_.map(_.event.eventId)), Right(List(eventValue.eventId)))
            expiredLeaseClaim <- outbox
              .claim(
                "publisher-b",
                secondPublisher,
                now.plusSeconds(61L),
                now.plusSeconds(121L),
                1
              )
              .value
            _ = assertEquals(expiredLeaseClaim.map(_.map(_.event.eventId)), Right(List(eventValue.eventId)))
            fenceBeforeRace <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.OutboxSubjectFences)
                .find(new Document("_id", userId.value.toString))
            )
            generationsBeforeRace <- MongoProducerRegistrations
              .batch(database, userId.value.toString, "Operational")
              .value
            _ = assertEquals(generationsBeforeRace.map(_.toList), Right(List(firstPublisher, secondPublisher).sorted))
            _ = assert(fenceBeforeRace.forall(!_.containsKey("transactionalIds")))
            requestWritten <- Deferred[IO, Unit]
            allowDeletionToContinue <- Deferred[IO, Unit]
            deletionFiber <- runner
              .run { session =>
                val context = MongoMutationWriteContext(session)
                for {
                  receiptId <- erasures.enqueue(userId, deletionAt, context)
                  _ <- RepositoryIO.lift(requestWritten.complete(()).void *> allowDeletionToContinue.get)
                  _ <- users.deleteAccount(userId, deletionAt, "deleted-race-account", context)
                } yield receiptId
              }
              .value
              .start
            _ <- requestWritten.get
            racingClaim <- outbox
              .claim(
                "publisher-c",
                racingPublisher,
                deletionAt,
                deletionAt.plusSeconds(60L),
                1
              )
              .value
            _ = assertEquals(racingClaim.map(_.map(_.event.eventId)), Right(List(eventValue.eventId)))
            _ <- allowDeletionToContinue.complete(()).void
            firstDeletion <- deletionFiber.joinWithNever
            firstRequest <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsErasureRequests)
                .find(new Document("_id", userId.value.toString))
            )
            _ = firstDeletion
              .foreach(_ => assertEquals(firstRequest.map(_.getBoolean("producerRegistry").booleanValue()), Some(true)))
            _ = assert(firstDeletion.isRight || firstRequest.isEmpty, clues(firstDeletion, firstRequest))
            finalDeletion <- firstDeletion match {
              case right @ Right(_) => IO.pure(right)
              case Left(_)          =>
                runner.run { session =>
                  val context = MongoMutationWriteContext(session)
                  erasures.enqueue(userId, deletionAt.plusSeconds(1L), context).flatMap { receiptId =>
                    users
                      .deleteAccount(userId, deletionAt.plusSeconds(1L), "deleted-race-account", context)
                      .as(receiptId)
                  }
                }.value
            }
            receiptId <- finalDeletion.fold(
              error => IO.raiseError[String](new AssertionError(s"Deletion retry failed after fence race: $error")),
              IO.pure
            )
            requestAfterDeletion <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.AnalyticsErasureRequests)
                .find(new Document("_id", userId.value.toString))
            )
            capturedAfterDeletion <- MongoProducerRegistrations
              .batch(database, userId.value.toString, "Operational")
              .value
            _ = assertEquals(
              capturedAfterDeletion.map(_.toList),
              Right(List(firstPublisher, secondPublisher, racingPublisher).sorted)
            )
            storedUser <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.Users).find(new Document("_id", userId.value.toString))
            )
            _ = assertEquals(storedUser.map(_.getString("accountStatus")), Some("Deleted"))
            _ = assertEquals(requestAfterDeletion.map(_.getString("receiptId")), Some(receiptId))
            lateClaim <- outbox
              .claim(
                "publisher-d",
                "hiring-publisher-00000000-0000-0000-0000-000000000388",
                deletionAt.plusSeconds(200L),
                deletionAt.plusSeconds(260L),
                1
              )
              .value
            _ = assertEquals(lateClaim, Right(Nil))
            outboxAfterLateClaim <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.EventOutbox)
                .find(new Document("_id", eventValue.eventId.toString))
            )
            _ = assertEquals(outboxAfterLateClaim.map(_.getString("state")), Some("Failed"))
            status <- erasures.statusForSubject(userId, receiptId).value
            _ = assertEquals(status, Right(com.example.graphQL.cats.domain.model.AccountDeletionStatus.Pending))
          } yield ()
        }
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
