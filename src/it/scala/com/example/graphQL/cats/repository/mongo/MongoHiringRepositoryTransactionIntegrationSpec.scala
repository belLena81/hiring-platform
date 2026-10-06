package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.AccountValueFixtures.email
import cats.effect.{Deferred, IO, Outcome, Ref, Resource}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField}
import cats.syntax.all.*
import com.example.graphQL.cats.repository.mongo.MongoRepositoryTestSupport.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{
  AccountStatus,
  CandidateProfile,
  Job,
  JobStatus,
  Location,
  RecruiterProfile,
  User,
  UserProfile,
  UserRole
}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, UseCaseIO}
import com.example.graphQL.cats.service.events.{
  OperationalAggregateType,
  OperationalEventEnvelope,
  OperationalEventType
}
import com.mongodb.client.model.{Filters, InsertOneOptions}
import io.circe.Json
import munit.CatsEffectSuite
import org.bson.Document
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.{Duration, Instant}
import java.util.{Date, UUID}
import scala.concurrent.duration.*

class MongoHiringRepositoryTransactionIntegrationSpec extends CatsEffectSuite {
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

  private def requireResult[A](result: Either[RepositoryError, A]): IO[A] =
    result.fold(error => IO.raiseError(new AssertionError(s"Mongo repository failure: $error")), IO.pure)

  private def job(id: JobId, recruiterId: UserId): Job =
    Job(
      id,
      recruiterId,
      "Scala Engineer",
      "Build hiring infrastructure",
      List("Cats Effect"),
      Set("Scala", "MongoDB"),
      Location("Cyprus", "Nicosia", remote = true),
      JobStatus.Open,
      now,
      now
    )

  private def event(value: Job, actorId: UserId): OperationalEventEnvelope =
    OperationalEventEnvelope(
      UUID.randomUUID(),
      OperationalEventType.JOB_CREATED,
      now,
      OperationalAggregateType.Job,
      value.id.value.toString,
      actorId,
      Json.obj("jobId" -> Json.fromString(value.id.value.toString))
    )

  test("native transaction cleanup observes committed rejected failed and cancelled sessions") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        client.getDatabase(s"transaction_cleanup_${UUID.randomUUID()}").flatMap { database =>
          for {
            failures <- Ref.of[IO, Vector[Map[LogField, String]]](Vector.empty)
            diagnostics = new Diagnostics {
              override def event(
                  event: LogEvent,
                  requestId: Option[String],
                  fields: => Map[LogField, String]
              ): IO[Unit] =
                if (event == LogEvent.MongoRepositoryFailed) failures.update(_ :+ fields) else IO.unit
            }
            _ <- database.createCollection("transaction_cleanup")
            documents <- Mongo4catsCollections.documents(database, "transaction_cleanup")
            runner = MongoTransactionRunner.sessions(client, RepositoryError.Conflict, diagnostics = diagnostics)
            insert = (id: String, session: Option[mongo4cats.client.ClientSession[IO]]) =>
              RepositoryIO
                .lift(
                  MongoSessionOperations.insertOne(IO.pure(documents), session, new Document("_id", id))
                )
                .void
            first <- runner.run(session => insert("committed-1", session).as(1)).value
            second <- runner.run(session => insert("committed-2", session).as(2)).value
            rejected <- runner.run { session =>
              insert("rejected", session) *> RepositoryIO.fromEither[Unit](Left(RepositoryError.Conflict))
            }.value
            failed <- runner.run { session =>
              insert("failed", session) *> RepositoryIO.lift[Unit](
                IO.raiseError(new IllegalStateException("injected operation failure"))
              )
            }.value
            entered <- Deferred[IO, Unit]
            fiber <- runner
              .run { session =>
                insert("cancelled", session) *> RepositoryIO.lift(entered.complete(()) *> IO.never[Unit])
              }
              .value
              .start
            _ <- entered.get.timeout(10.seconds)
            _ <- fiber.cancel
            outcome <- fiber.join
            count <- documents.count
            committed <- documents.find(Filters.in("_id", "committed-1", "committed-2")).all
            logged <- failures.get
          } yield {
            assertEquals(first, Right(1))
            assertEquals(second, Right(2))
            assertEquals(rejected, Left(RepositoryError.Conflict))
            assertEquals(failed, Left(RepositoryError.Unavailable))
            assert(outcome.isCanceled)
            assertEquals(count, 2L)
            assertEquals(committed.size, 2)
            assertEquals(logged.count(_.get(LogField.SpanName).contains("transaction.operation")), 1)
            assert(!logged.exists(_.get(LogField.SpanName).contains("transaction.abort")))
          }
        }
      }
    }
  }

  test("startup backfills revisions and skips scans after the migration is complete") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        client.getDatabase(s"revision_migration_${UUID.randomUUID()}").flatMap { database =>
          val legacyJob = MongoHiringCodecs.job(job(JobId(UUID.randomUUID()), UserId(UUID.randomUUID())))
          val legacyUser = MongoHiringCodecs.user(
            User(UserId(UUID.randomUUID()), None, "Admin", UserRole.Admin, None, now, adminSingleton = true)
          )
          val legacyCandidate = MongoHiringCodecs.user(
            User(
              UserId(UUID.randomUUID()),
              None,
              "Legacy Candidate",
              UserRole.Candidate,
              Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), Some("Engineer"), None))),
              now
            )
          )
          val _ = legacyJob.remove("version")
          val _ = legacyUser.remove("version")
          val _ = legacyCandidate.remove("version")
          val legacyProfile = legacyCandidate.get("profile", classOf[Document])
          val _ = legacyProfile.remove("recruiterSearchOptIn")
          for {
            _ <- MongoRepositoryTestSupport
              .first(database.getCollection(MongoCollections.Jobs).insertOne(legacyJob))
              .void
            _ <- MongoRepositoryTestSupport
              .first(database.getCollection(MongoCollections.Users).insertOne(legacyUser))
              .void
            _ <- MongoRepositoryTestSupport
              .first(database.getCollection(MongoCollections.Users).insertOne(legacyCandidate))
              .void
            _ <- MongoHiringSetup.initialize(database, com.example.graphQL.cats.service.Diagnostics.noop)
            migratedJob <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.Jobs).find(Filters.eq("_id", legacyJob.getString("_id")))
            )
            migratedUser <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.Users).find(Filters.eq("_id", legacyUser.getString("_id")))
            )
            missingVersionJob = MongoHiringCodecs.job(job(JobId(UUID.randomUUID()), UserId(UUID.randomUUID())))
            invalidVersionUser = MongoHiringCodecs.user(
              User(
                UserId(UUID.randomUUID()),
                None,
                "Recruiter",
                UserRole.Recruiter,
                Some(UserProfile.Recruiter(RecruiterProfile("Another Hiring Co", Some("Recruiter")))),
                now
              )
            )
            _ = missingVersionJob.remove("version")
            _ = invalidVersionUser.put("version", "invalid")
            _ <- MongoRepositoryTestSupport
              .first(
                database
                  .getCollection(MongoCollections.Jobs)
                  .insertOne(missingVersionJob, new InsertOneOptions().bypassDocumentValidation(true))
              )
              .void
            _ <- MongoRepositoryTestSupport
              .first(
                database
                  .getCollection(MongoCollections.Users)
                  .insertOne(invalidVersionUser, new InsertOneOptions().bypassDocumentValidation(true))
              )
              .void
            _ <- MongoHiringSetup.initialize(database, com.example.graphQL.cats.service.Diagnostics.noop)
            completedMigration <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.HiringMigrationLedger)
                .find(Filters.eq("_id", "001_user_job_revisions"))
            )
            completedCandidateProfileMigration <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.HiringMigrationLedger)
                .find(Filters.eq("_id", "002_candidate_search_profile_verification"))
            )
            storedLegacyCandidate <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.Users).find(Filters.eq("_id", legacyCandidate.getString("_id")))
            )
            skippedBackfill <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.Jobs).find(Filters.eq("_id", missingVersionJob.getString("_id")))
            )
            skippedVerification <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.Users)
                .find(Filters.eq("_id", invalidVersionUser.getString("_id")))
            )
            _ <- IO {
              assertEquals(migratedJob.map(_.getLong("version").longValue()), Some(0L))
              assertEquals(migratedUser.map(_.getLong("version").longValue()), Some(0L))
              assertEquals(completedMigration.map(_.getString("state")), Some("Complete"))
              assertEquals(completedCandidateProfileMigration.map(_.getString("state")), Some("Complete"))
              assertEquals(
                storedLegacyCandidate
                  .flatMap(document => MongoHiringCodecs.readUser(document).toOption)
                  .flatMap(_.candidateProfile)
                  .map(_.recruiterSearchOptIn),
                Some(false)
              )
              assertEquals(skippedBackfill.map(_.containsKey("version")), Some(false))
              assertEquals(skippedVerification.map(_.getString("version")), Some("invalid"))
            }
          } yield ()
        }
      }
    }
  }

  test("a stale backfill batch cannot reset a revision advanced by a repository write") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        client.getDatabase(s"revision_race_${UUID.randomUUID()}").flatMap { database =>
          val jobs = MongoJobRepository.transactional(
            database,
            client,
            new MongoEmbeddingWorkRepository(database, com.example.graphQL.cats.service.Diagnostics.noop),
            com.example.graphQL.cats.service.Diagnostics.noop
          )
          val original = job(JobId(UUID.randomUUID()), UserId(UUID.randomUUID()))
          val legacyDocument = MongoHiringCodecs.job(original)
          val ids = List(original.id.value.toString)
          val _ = legacyDocument.remove("version")
          for {
            _ <- MongoHiringSetup.initialize(database, com.example.graphQL.cats.service.Diagnostics.noop)
            _ <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.Jobs)
                .insertOne(legacyDocument, new InsertOneOptions().bypassDocumentValidation(true))
            )
            // Both migrators selected this ID while its version was missing.
            _ <- MongoRepositoryTestSupport
              .collection(database, MongoCollections.Jobs)
              .flatMap(collection => MongoHiringMigrations.backfillVersionBatch(collection, ids))
            advanced <- jobs
              .updateWithEvents(
                Versioned(original, 0L),
                original.copy(title = "Updated title"),
                now,
                Nil,
                MutationWriteContext.directWrite
              )
              .value
            // Replay the second migrator's stale selection after the repository write advanced the version.
            _ <- MongoRepositoryTestSupport
              .collection(database, MongoCollections.Jobs)
              .flatMap(collection => MongoHiringMigrations.backfillVersionBatch(collection, ids))
            stale <- jobs
              .updateWithEvents(
                Versioned(original, 0L),
                original.copy(description = "Stale update"),
                now,
                Nil,
                MutationWriteContext.directWrite
              )
              .value
            stored <- jobs.findVersioned(original.id).value
          } yield {
            assertEquals(advanced.map(_.version), Right(1L))
            assertEquals(stale, Left(RepositoryError.Conflict))
            assertEquals(stored.map(_.map(_.version)), Right(Some(1L)))
            assertEquals(stored.map(_.map(_.value.title)), Right(Some("Updated title")))
          }
        }
      }
    }
  }

  test("repository-owned and receipt-owned job writes atomically persist all follow-ups") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        client.getDatabase(s"write_paths_${UUID.randomUUID()}").flatMap { database =>
          val embeddingWork =
            new MongoEmbeddingWorkRepository(database, com.example.graphQL.cats.service.Diagnostics.noop)
          val jobs = MongoJobRepository.transactional(
            database,
            client,
            embeddingWork,
            com.example.graphQL.cats.service.Diagnostics.noop
          )
          val receipts = MongoMutationReceiptRepository.transactional(
            database,
            client,
            com.example.graphQL.cats.service.Diagnostics.noop
          )
          val recruiterId = UserId(UUID.randomUUID())
          val directJob = job(JobId(UUID.randomUUID()), recruiterId)
          val receiptJob = job(JobId(UUID.randomUUID()), recruiterId)
          val rolledBackJob = job(JobId(UUID.randomUUID()), recruiterId)
          val receiptRolledBackJob = job(JobId(UUID.randomUUID()), recruiterId)
          val directEvent = event(directJob, recruiterId)
          val receiptEvent = event(receiptJob, recruiterId)
          val duplicateEvent = event(rolledBackJob, recruiterId).copy(eventId = directEvent.eventId)
          val receiptDuplicateEvent = event(receiptRolledBackJob, recruiterId).copy(eventId = directEvent.eventId)
          val receiptKey = MutationReceiptKey("createJob", recruiterId.value.toString, UUID.randomUUID())
          val receiptFingerprint = MutationReceiptFingerprint.fromCanonicalInput(receiptJob.id.value.toString)

          for {
            _ <- MongoHiringSetup.initialize(database, com.example.graphQL.cats.service.Diagnostics.noop)
            _ <- jobs
              .createWithEvents(directJob, now, List(directEvent), MutationWriteContext.directWrite)
              .value
              .flatMap(requireResult)
            receiptResult <- receipts
              .execute[Unit, String](receiptKey, receiptFingerprint, now, now.plusSeconds(3600)) { context =>
                jobs
                  .createWithEvents(receiptJob, now, List(receiptEvent), context)
                  .as(
                    MutationWriteOutcome.Applied(
                      MutationReceiptWrite((), MutationEntityReference("Job", receiptJob.id.value.toString))
                    )
                  )
              }
              .value
              .flatMap(requireResult)
            rejectedReceipt <- receipts
              .execute[Unit, String](
                MutationReceiptKey("createJob", recruiterId.value.toString, UUID.randomUUID()),
                MutationReceiptFingerprint.fromCanonicalInput("rejected-write"),
                now,
                now.plusSeconds(3600)
              )(_ => RepositoryIO.fromEither(Right(MutationWriteOutcome.Rejected("business rejection"))))
              .value
              .flatMap(requireResult)
            receiptRollback <- Idempotent(receipts)
              .execute[Job](
                "createJob",
                recruiterId.value.toString,
                IdempotencyRequest.fromCanonicalInput(UUID.randomUUID(), receiptRolledBackJob.id.value.toString),
                value => MutationEntityReference("Job", value.id.value.toString),
                _ => UseCaseIO.pure(receiptRolledBackJob)
              ) { context =>
                UseCaseIO
                  .repository(jobs.createWithEvents(receiptRolledBackJob, now, List(receiptDuplicateEvent), context))
                  .as(receiptRolledBackJob)
              }
              .value
            storedJobs <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.Jobs).countDocuments()
            )
            storedEvents <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.EventOutbox).countDocuments()
            )
            storedEmbeddingWork <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.EmbeddingWork).countDocuments()
            )
            storedMutationReceipts <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.MutationReceipts).countDocuments()
            )
            receiptRolledBackStored <- jobs.find(receiptRolledBackJob.id).value.flatMap(requireResult)
            rollbackResult <- jobs
              .createWithEvents(
                rolledBackJob,
                now,
                List(duplicateEvent),
                MutationWriteContext.directWrite
              )
              .value
            rolledBackStored <- jobs.find(rolledBackJob.id).value.flatMap(requireResult)
            jobsAfterRollback <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.Jobs).countDocuments()
            )
            eventsAfterRollback <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.EventOutbox).countDocuments()
            )
            embeddingAfterRollback <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.EmbeddingWork).countDocuments()
            )
            _ <- IO {
              receiptResult match {
                case MutationReceiptExecution.Applied((), entity) =>
                  assertEquals(entity, MutationEntityReference("Job", receiptJob.id.value.toString))
                case other => fail(s"Expected an applied receipt, received $other")
              }
              assertEquals(rejectedReceipt, MutationReceiptExecution.Rejected("business rejection"))
              assertEquals(storedJobs.map(_.longValue), Some(2L))
              assertEquals(storedEvents.map(_.longValue), Some(2L))
              assertEquals(storedEmbeddingWork.map(_.longValue), Some(2L))
              assertEquals(
                receiptRollback,
                Left(com.example.graphQL.cats.service.UseCaseError.Repository(RepositoryError.Conflict))
              )
              assertEquals(storedMutationReceipts.map(_.longValue), Some(1L))
              assertEquals(receiptRolledBackStored, None)
              assertEquals(rollbackResult, Left(RepositoryError.Conflict))
              assertEquals(rolledBackStored, None)
              assertEquals(jobsAfterRollback.map(_.longValue), Some(2L))
              assertEquals(eventsAfterRollback.map(_.longValue), Some(2L))
              assertEquals(embeddingAfterRollback.map(_.longValue), Some(2L))
            }
          } yield ()
        }
      }
    }
  }

  test("cancelling a receipt-owned job write aborts the job and every transactional follow-up") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        client.getDatabase(s"cancelled_job_write_${UUID.randomUUID()}").flatMap { database =>
          val diagnostics = com.example.graphQL.cats.service.Diagnostics.noop
          val embeddingWork = new MongoEmbeddingWorkRepository(database, diagnostics)
          val jobs = MongoJobRepository.transactional(database, client, embeddingWork, diagnostics)
          val receipts = MongoMutationReceiptRepository.transactional(database, client, diagnostics)
          val recruiterId = UserId(UUID.randomUUID())
          val value = job(JobId(UUID.randomUUID()), recruiterId)
          val created = event(value, recruiterId)
          val key = MutationReceiptKey("createJob", recruiterId.value.toString, UUID.randomUUID())
          val fingerprint = MutationReceiptFingerprint.fromCanonicalInput(value.id.value.toString)

          for {
            _ <- MongoHiringSetup.initialize(database, diagnostics)
            written <- Deferred[IO, Unit]
            _ <- Resource
              .make(
                receipts
                  .execute[Unit, String](key, fingerprint, now, now.plusSeconds(3600)) { context =>
                    jobs.createWithEvents(value, now, List(created), context) *>
                      RepositoryIO.lift(
                        written.complete(()).void *> IO.never[MutationWriteOutcome[Unit, String]]
                      )
                  }
                  .value
                  .start
              )(_.cancel)
              .use { fiber =>
                for {
                  _ <- written.get.timeout(30.seconds)
                  _ <- fiber.cancel
                  outcome <- fiber.join
                  storedJob <- jobs.find(value.id).value
                  storedReceipts <- MongoRepositoryTestSupport.count(database, MongoCollections.MutationReceipts)
                  storedEmbeddingWork <- MongoRepositoryTestSupport.count(database, MongoCollections.EmbeddingWork)
                  storedEvents <- MongoRepositoryTestSupport.count(database, MongoCollections.EventOutbox)
                } yield {
                  assert(outcome match {
                    case Outcome.Canceled() => true
                    case _                  => false
                  })
                  assertEquals(storedJob, Right(None))
                  assertEquals(storedReceipts, 0L)
                  assertEquals(storedEmbeddingWork, 0L)
                  assertEquals(storedEvents, 0L)
                }
              }
          } yield ()
        }
      }
    }
  }

  test("disabled embedding is explicit and job events still persist without embedding work") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        client.getDatabase(s"disabled_embedding_${UUID.randomUUID()}").flatMap { database =>
          val jobs = MongoJobRepository.transactional(
            database,
            client,
            MongoEmbeddingWorkEnqueuer.disabled,
            com.example.graphQL.cats.service.Diagnostics.noop
          )
          val recruiterId = UserId(UUID.randomUUID())
          val value = job(JobId(UUID.randomUUID()), recruiterId)
          val created = event(value, recruiterId)

          for {
            _ <- MongoHiringSetup.initialize(database, com.example.graphQL.cats.service.Diagnostics.noop)
            result <- jobs.createWithEvents(value, now, List(created), MutationWriteContext.directWrite).value
            storedJobs <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.Jobs).countDocuments()
            )
            storedEvents <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.EventOutbox).countDocuments()
            )
            storedEmbeddingWork <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.EmbeddingWork).countDocuments()
            )
          } yield {
            assertEquals(result, Right(()))
            assertEquals(storedJobs.map(_.longValue), Some(1L))
            assertEquals(storedEvents.map(_.longValue), Some(1L))
            assertEquals(storedEmbeddingWork.map(_.longValue), Some(0L))
          }
        }
      }
    }
  }

  test("recruiter deletion tombstones the account and closes only newly open jobs") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        client.getDatabase(s"delete_success_${UUID.randomUUID()}").flatMap { database =>
          val embeddingWork =
            new MongoEmbeddingWorkRepository(database, com.example.graphQL.cats.service.Diagnostics.noop)
          val jobs = MongoJobRepository.transactional(
            database,
            client,
            embeddingWork,
            com.example.graphQL.cats.service.Diagnostics.noop
          )
          val users = MongoUserRepository.transactional(
            database,
            client,
            embeddingWork,
            com.example.graphQL.cats.service.Diagnostics.noop
          )
          val recruiterId = UserId(UUID.randomUUID())
          val recruiter = recruiterUser(recruiterId)
          val firstOpen = job(JobId(UUID.randomUUID()), recruiterId)
          val secondOpen = job(JobId(UUID.randomUUID()), recruiterId).copy(title = "Platform Engineer")
          val alreadyClosed = job(JobId(UUID.randomUUID()), recruiterId).copy(
            status = JobStatus.Closed,
            closedAt = Some(now.minusSeconds(60)),
            updatedAt = now.minusSeconds(60)
          )
          val deletionTime = now.plusSeconds(2)

          for {
            _ <- MongoHiringSetup.initialize(database, com.example.graphQL.cats.service.Diagnostics.noop)
            _ <- users.insert(recruiter).value.flatMap(requireResult)
            _ <- List(firstOpen, secondOpen, alreadyClosed).traverse_ { value =>
              MongoRepositoryTestSupport
                .first(database.getCollection(MongoCollections.Jobs).insertOne(MongoHiringCodecs.job(value)))
                .void
            }
            deletion <- users
              .deleteAccount(
                recruiterId,
                deletionTime,
                "deleted-account",
                MutationWriteContext.directWrite
              )
              .value
            storedRecruiter <- users.find(recruiterId).value.flatMap(requireResult)
            storedFirst <- jobs.find(firstOpen.id).value.flatMap(requireResult)
            storedSecond <- jobs.find(secondOpen.id).value.flatMap(requireResult)
            storedClosed <- jobs.find(alreadyClosed.id).value.flatMap(requireResult)
            closeEvents <- MongoRepositoryTestSupport.collectWithin(
              database
                .getCollection(MongoCollections.EventOutbox)
                .find(
                  Filters.eq("eventType", OperationalEventType.JOB_CLOSED.toString)
                ),
              10
            )
          } yield {
            assertEquals(deletion, Right(()))
            assertEquals(storedRecruiter.map(_.accountStatus), Some(AccountStatus.Deleted))
            assertEquals(storedRecruiter.map(_.name), Some("deleted-account"))
            assertEquals(storedRecruiter.flatMap(_.email), None)
            assertEquals(storedRecruiter.flatMap(_.profile), None)
            assertEquals(storedRecruiter.flatMap(_.deletedAt), Some(deletionTime))
            assertEquals(storedFirst.map(_.status), Some(JobStatus.Closed))
            assertEquals(storedSecond.map(_.status), Some(JobStatus.Closed))
            assertEquals(storedClosed, Some(alreadyClosed))
            assertEquals(closeEvents.size, 2)
            assertEquals(
              closeEvents.map(_.getString("aggregateId")).toSet,
              Set(firstOpen.id.value.toString, secondOpen.id.value.toString)
            )
          }
        }
      }
    }
  }

  test("recruiter deletion rolls back on malformed owned job") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        client.getDatabase(s"delete_rollback_${UUID.randomUUID()}").flatMap { database =>
          val users = MongoUserRepository.transactional(
            database,
            client,
            new MongoEmbeddingWorkRepository(database, com.example.graphQL.cats.service.Diagnostics.noop),
            com.example.graphQL.cats.service.Diagnostics.noop
          )
          val recruiterId = UserId(UUID.randomUUID())
          val recruiter = recruiterUser(recruiterId)

          for {
            _ <- MongoHiringSetup.initialize(database, com.example.graphQL.cats.service.Diagnostics.noop)
            _ <- users.insert(recruiter).value.flatMap(requireResult)
            _ <- MongoRepositoryTestSupport.first(
              database
                .getCollection(MongoCollections.Jobs)
                .insertOne(
                  new Document("_id", UUID.randomUUID().toString)
                    .append("recruiterId", recruiterId.value.toString)
                    .append("status", JobStatus.Open.toString)
                    .append("createdAt", Date.from(now.plusSeconds(1))),
                  new InsertOneOptions().bypassDocumentValidation(true)
                )
            )
            deletion <- users
              .deleteAccount(
                recruiterId,
                now.plusSeconds(2),
                "deleted-account",
                MutationWriteContext.directWrite
              )
              .value
            storedRecruiter <- users.find(recruiterId).value.flatMap(requireResult)
            eventsAfterFailure <- MongoRepositoryTestSupport.first(
              database.getCollection(MongoCollections.EventOutbox).countDocuments()
            )
          } yield {
            assertEquals(deletion, Left(RepositoryError.InvalidStoredData))
            assertEquals(storedRecruiter, Some(recruiter))
            assertEquals(eventsAfterFailure.map(_.longValue), Some(0L))
          }
        }
      }
    }
  }

  test("canonical mutation fingerprints conflict with receipts from the prior input format") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        client.getDatabase(s"mutation_fingerprint_${UUID.randomUUID()}").flatMap { database =>
          val receipts = MongoMutationReceiptRepository.transactional(
            database,
            client,
            com.example.graphQL.cats.service.Diagnostics.noop
          )
          val key = MutationReceiptKey("createJob", "actor", UUID.randomUUID())
          val legacyFingerprint = MutationReceiptFingerprint.fromCanonicalInput(
            Json.fromString("CreateJobGraphQLInput(legacy-rendering)").noSpaces
          )
          val canonicalFingerprint = MutationReceiptFingerprint.fromCanonicalInput(
            """{"idempotencyKey":"00000000-0000-0000-0000-000000000001","title":"New format"}"""
          )
          val entity = MutationEntityReference("job", UUID.randomUUID().toString)

          for {
            _ <- MongoHiringSetup.initialize(database, com.example.graphQL.cats.service.Diagnostics.noop)
            oldReceipt <- receipts
              .execute[Unit, String](key, legacyFingerprint, now, now.plusSeconds(3600)) { _ =>
                RepositoryIO.fromEither(Right(MutationWriteOutcome.Applied(MutationReceiptWrite((), entity))))
              }
              .value
            newFormatRetry <- receipts
              .execute[Unit, String](key, canonicalFingerprint, now, now.plusSeconds(3600)) { _ =>
                RepositoryIO.lift(
                  IO.raiseError(new AssertionError("a prior-format receipt must not replay under the new fingerprint"))
                )
              }
              .value
          } yield {
            assertEquals(oldReceipt, Right(MutationReceiptExecution.Applied((), entity)))
            assertEquals(newFormatRetry, Right(MutationReceiptExecution.FingerprintMismatch))
          }
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
  test("authorized application selection preserves tied pagination and rejects changed relationships") {
    import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus}
    import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, ApplicationEventId}
    import com.example.graphQL.cats.domain.pagination.*
    import com.example.graphQL.cats.service.{ActorContext, HiringReadService}
    import com.example.graphQL.cats.service.auth.ActorAuthorization
    import com.example.graphQL.cats.service.read.*
    val pageSize = PageSize.fromInt(2).fold(errors => fail(errors.toString), identity)
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        client.getDatabase(s"authorized_hiring_${UUID.randomUUID()}").flatMap { database =>
          val recruiter = User(
            UserId(UUID.randomUUID()),
            None,
            "Recruiter",
            UserRole.Recruiter,
            Some(UserProfile.Recruiter(RecruiterProfile("Synthetic employer", None))),
            now
          )
          val otherRecruiter = recruiter.copy(id = UserId(UUID.randomUUID()), name = "Other recruiter")
          val candidate = User(
            UserId(UUID.randomUUID()),
            None,
            "Candidate",
            UserRole.Candidate,
            Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), Some("Synthetic experience"), None))),
            now
          )
          val otherCandidate = candidate.copy(id = UserId(UUID.randomUUID()), name = "Other candidate")
          val admin = User(UserId(UUID.randomUUID()), None, "Admin", UserRole.Admin, None, now, adminSingleton = true)
          val original = job(JobId(UUID.randomUUID()), recruiter.id)
          val applicants = List(
            candidate,
            candidate.copy(id = UserId(UUID.randomUUID()), name = "Second applicant"),
            candidate.copy(id = UserId(UUID.randomUUID()), name = "Third applicant")
          )
          val applications = List(1L, 2L, 3L).zip(applicants).map { case (value, applicant) =>
            Application.create(ApplicationId(new UUID(0L, value)), applicant.id, original.id, now)
          }
          val history = List(1L, 2L, 3L).map(value =>
            ApplicationEvent(
              ApplicationEventId(new UUID(1L, value)),
              applications.head.id,
              None,
              ApplicationStatus.Created,
              candidate.id,
              now,
              None,
              None
            )
          )
          val users =
            MongoUserRepository.transactional(database, client, MongoEmbeddingWorkEnqueuer.disabled, Diagnostics.noop)
          val jobs =
            MongoJobRepository.transactional(database, client, MongoEmbeddingWorkEnqueuer.disabled, Diagnostics.noop)
          val repository = MongoApplicationRepository.transactional(database, client, Diagnostics.noop)
          val read = HiringReadService(users, jobs, repository)
          def scope(user: User): HiringReadScope = HiringReadScope
            .validated(ActorContext(user.id, user.role), user, ActorAuthorization(users))
            .fold(error => fail(error.toString), identity)
          val recruiterScope = scope(recruiter)
          val candidateScope = scope(candidate)
          val adminScope = scope(admin)
          val appPage = ApplicationPageRequest(None, None, pageSize)
          val eventPage = ApplicationEventPageRequest(None, pageSize)
          for {
            _ <- MongoHiringSetup.initialize(database, Diagnostics.noop)
            _ <- (List(recruiter, otherRecruiter, otherCandidate, admin) ++ applicants)
              .traverse_(user => users.insert(user).value.flatMap(requireResult))
            _ <- jobs
              .createWithEvents(original, now, Nil, MutationWriteContext.directWrite)
              .value
              .flatMap(requireResult)
            _ <- applications.traverse_(application =>
              MongoRepositoryTestSupport
                .first(
                  database
                    .getCollection(MongoCollections.Applications)
                    .insertOne(MongoHiringCodecs.application(application))
                )
                .void
            )
            _ <- history.traverse_(event =>
              MongoRepositoryTestSupport
                .first(
                  database.getCollection(MongoCollections.ApplicationEvents).insertOne(MongoHiringCodecs.event(event))
                )
                .void
            )
            first <- repository.findByJob(recruiterScope, original.id, appPage).value.flatMap(requireResult)
            next <- repository
              .findByJob(
                recruiterScope,
                original.id,
                appPage.copy(cursor = first.lastOption.map(value => ApplicationCursor(value.createdAt, value.id)))
              )
              .value
              .flatMap(requireResult)
            empty <- repository
              .findByJob(
                recruiterScope,
                original.id,
                appPage.copy(cursor = next.lastOption.map(value => ApplicationCursor(value.createdAt, value.id)))
              )
              .value
              .flatMap(requireResult)
            filtered <- repository
              .findByJob(recruiterScope, original.id, appPage.copy(status = Some(ApplicationStatus.Hired)))
              .value
              .flatMap(requireResult)
            own <- repository.findByCandidate(candidateScope, appPage).value.flatMap(requireResult)
            unrelated <- repository.findByCandidate(scope(otherCandidate), appPage).value.flatMap(requireResult)
            eventFirst <- repository
              .history(candidateScope, applications.head.id, eventPage)
              .value
              .flatMap(requireResult)
            eventNext <- repository
              .history(
                candidateScope,
                applications.head.id,
                eventPage
                  .copy(cursor = eventFirst.lastOption.map(value => ApplicationEventCursor(value.occurredAt, value.id)))
              )
              .value
              .flatMap(requireResult)
            denied <- repository
              .history(scope(otherRecruiter), applications.head.id, eventPage)
              .value
              .flatMap(requireResult)
            adminHistory <- repository.history(adminScope, applications.head.id, eventPage).value.flatMap(requireResult)
            _ <- read
              .canViewApplication(ActorContext(recruiter.id, recruiter.role), applications.head.id)
              .value
              .flatMap(value => value.fold(error => IO.raiseError(new AssertionError(error.toString)), IO.pure))
            // Ownership changes after the precheck, before the final repository selection.
            _ <- MongoRepositoryTestSupport
              .first(
                database
                  .getCollection(MongoCollections.Jobs)
                  .flatMap(
                    _.updateOne(
                      Filters.eq(MongoFields.Id, original.id.value.toString),
                      new Document(
                        "$set",
                        new Document(MongoFields.RecruiterId, otherRecruiter.id.value.toString)
                          .append(MongoFields.Status, JobStatus.Closed.toString)
                          .append(MongoFields.ClosedAt, Date.from(now))
                      )
                    )
                  )
              )
              .void
            reassigned <- repository.findByJob(recruiterScope, original.id, appPage).value.flatMap(requireResult)
            reassignedHistory <- repository
              .history(recruiterScope, applications.head.id, eventPage)
              .value
              .flatMap(requireResult)
            adminApplications <- repository.findByJob(adminScope, original.id, appPage).value.flatMap(requireResult)
            historical <- jobs
              .relatedJobs(candidateScope, List(JobRelationKey(applications.head.id, original.id)))
              .value
              .flatMap(requireResult)
            historicalRecruiter <- users
              .relatedUsers(candidateScope, List(UserRelationKey.JobRecruiter(original.id, otherRecruiter.id)))
              .value
              .flatMap(requireResult)
            forgedCandidate <- users
              .relatedUsers(
                candidateScope,
                List(UserRelationKey.ApplicationCandidate(applications.head.id, otherCandidate.id))
              )
              .value
              .flatMap(requireResult)
            forgedJob <- jobs
              .relatedJobs(candidateScope, List(JobRelationKey(applications.head.id, JobId(UUID.randomUUID()))))
              .value
              .flatMap(requireResult)
            mixedCandidateKeys = List(
              UserRelationKey.ApplicationCandidate(applications.head.id, otherCandidate.id),
              UserRelationKey.ApplicationCandidate(applications.head.id, candidate.id)
            )
            mixedCandidates <- users.relatedUsers(candidateScope, mixedCandidateKeys).value.flatMap(requireResult)
            mixedJobKeys = List(
              JobRelationKey(applications.head.id, JobId(UUID.randomUUID())),
              JobRelationKey(applications.head.id, original.id)
            )
            mixedJobs <- jobs.relatedJobs(candidateScope, mixedJobKeys).value.flatMap(requireResult)
            wrongRecruiter <- users
              .relatedUsers(candidateScope, List(UserRelationKey.JobRecruiter(original.id, recruiter.id)))
              .value
              .flatMap(requireResult)
            _ <- MongoRepositoryTestSupport
              .first(
                database
                  .getCollection(MongoCollections.Users)
                  .flatMap(
                    _.updateOne(
                      Filters.eq(MongoFields.Id, candidate.id.value.toString),
                      new Document("$set", new Document(MongoFields.AccountStatus, AccountStatus.Deleted.toString))
                    )
                  )
              )
              .void
            revoked <- repository.history(candidateScope, applications.head.id, eventPage).value.flatMap(requireResult)
            revokedRelation <- jobs
              .relatedJobs(candidateScope, List(JobRelationKey(applications.head.id, original.id)))
              .value
              .flatMap(requireResult)
          } yield {
            assertEquals(first.map(_.id), applications.reverse.take(2).map(_.id))
            assertEquals(next.map(_.id), applications.take(1).map(_.id))
            assertEquals(empty, Nil)
            assertEquals(filtered, Nil)
            assertEquals(own.map(_.id), List(applications.head.id))
            assertEquals(unrelated, Nil)
            assertEquals(eventFirst.map(_.id), history.reverse.take(2).map(_.id))
            assertEquals(eventNext.map(_.id), history.take(1).map(_.id))
            assertEquals(denied, Nil)
            assertEquals(adminHistory.map(_.id), eventFirst.map(_.id))
            assertEquals(reassigned, Nil)
            assertEquals(reassignedHistory, Nil)
            assertEquals(adminApplications.map(_.id), first.map(_.id))
            assertEquals(historical.map(_.value.id), List(original.id))
            assertEquals(historicalRecruiter.map(_.value.id), List(otherRecruiter.id))
            assertEquals(forgedCandidate, Nil)
            assertEquals(forgedJob, Nil)
            assertEquals(wrongRecruiter, Nil)
            assertEquals(mixedCandidates.map(_.key), mixedCandidateKeys.drop(1))
            assertEquals(mixedJobs.map(_.key), mixedJobKeys.drop(1))
            assertEquals(revoked, Nil)
            assertEquals(revokedRelation, Nil)
          }
        }
      }
    }
  }

}
