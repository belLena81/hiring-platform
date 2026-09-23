package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{
  AccountStatus,
  Job,
  JobStatus,
  Location,
  RecruiterProfile,
  User,
  UserProfile,
  UserRole
}
import com.example.graphQL.cats.repository.protocol.*
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, UseCaseIO}
import com.example.graphQL.cats.shared.events.{OperationalAggregateType, OperationalEventEnvelope, OperationalEventType}
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

  test("startup backfills user and job revisions once and records the completed migration") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        val database = client.getDatabase(s"revision_migration_${UUID.randomUUID()}")
        val legacyJob = MongoHiringCodecs.job(job(JobId(UUID.randomUUID()), UserId(UUID.randomUUID())))
        val legacyUser = MongoHiringCodecs.user(
          User(UserId(UUID.randomUUID()), None, "Admin", UserRole.Admin, None, now, adminSingleton = true)
        )
        val _ = legacyJob.remove("version")
        val _ = legacyUser.remove("version")
        for {
          _ <- PublisherBridge.first(database.getCollection("jobs").insertOne(legacyJob)).void
          _ <- PublisherBridge.first(database.getCollection("users").insertOne(legacyUser)).void
          _ <- MongoHiringSetup.initialize(database)
          migratedJob <- PublisherBridge.first(
            database.getCollection("jobs").find(Filters.eq("_id", legacyJob.getString("_id")))
          )
          migratedUser <- PublisherBridge.first(
            database.getCollection("users").find(Filters.eq("_id", legacyUser.getString("_id")))
          )
          _ <- MongoHiringSetup.initialize(database)
          completedMigration <- PublisherBridge.first(
            database.getCollection("hiring_migration_ledger").find(Filters.eq("_id", "001_user_job_revisions"))
          )
          _ <- IO {
            assertEquals(migratedJob.map(_.getLong("version").longValue()), Some(0L))
            assertEquals(migratedUser.map(_.getLong("version").longValue()), Some(0L))
            assertEquals(completedMigration.map(_.getString("state")), Some("Complete"))
          }
        } yield ()
      }
    }
  }

  test("a stale backfill batch cannot reset a revision advanced by a repository write") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        val database = client.getDatabase(s"revision_race_${UUID.randomUUID()}")
        val jobs = MongoJobRepository.transactional(database, client)
        val original = job(JobId(UUID.randomUUID()), UserId(UUID.randomUUID()))
        val legacyDocument = MongoHiringCodecs.job(original)
        val ids = List(original.id.value.toString)
        val _ = legacyDocument.remove("version")
        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- PublisherBridge.first(
            database
              .getCollection("jobs")
              .insertOne(legacyDocument, new InsertOneOptions().bypassDocumentValidation(true))
          )
          // Both migrators selected this ID while its version was missing.
          _ <- MongoHiringSetup.backfillVersionBatch(database.getCollection("jobs"), ids)
          advanced <- jobs.update(Versioned(original, 0L), original.copy(title = "Updated title"), now)
          // Replay the second migrator's stale selection after the repository write advanced the version.
          _ <- MongoHiringSetup.backfillVersionBatch(database.getCollection("jobs"), ids)
          stale <- jobs.update(Versioned(original, 0L), original.copy(description = "Stale update"), now)
          stored <- jobs.findVersioned(original.id)
        } yield {
          assertEquals(advanced.map(_.version), Right(1L))
          assertEquals(stale, Left(RepositoryError.Conflict))
          assertEquals(stored.map(_.map(_.version)), Right(Some(1L)))
          assertEquals(stored.map(_.map(_.value.title)), Right(Some("Updated title")))
        }
      }
    }
  }

  test("repository-owned and receipt-owned job writes atomically persist all follow-ups") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        val database = client.getDatabase(s"write_paths_${UUID.randomUUID()}")
        val embeddingWork = new MongoEmbeddingWorkRepository(database)
        val jobs = MongoJobRepository.transactional(database, client, Some(embeddingWork))
        val receipts = MongoMutationReceiptRepository.transactional(database, client)
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
          _ <- MongoHiringSetup.initialize(database)
          _ <- jobs.createWithEvents(directJob, now, List(directEvent)).flatMap(requireResult)
          receiptResult <- receipts
            .execute[Unit, String](receiptKey, receiptFingerprint, now, now.plusSeconds(3600)) { context =>
              jobs
                .createWithEvents(receiptJob, now, List(receiptEvent), context)
                .map(
                  _.map(_ =>
                    MutationWriteOutcome.Applied(
                      MutationReceiptWrite((), MutationEntityReference("Job", receiptJob.id.value.toString))
                    )
                  )
                )
            }
            .flatMap(requireResult)
          rejectedReceipt <- receipts
            .execute[Unit, String](
              MutationReceiptKey("createJob", recruiterId.value.toString, UUID.randomUUID()),
              MutationReceiptFingerprint.fromCanonicalInput("rejected-write"),
              now,
              now.plusSeconds(3600)
            )(_ => IO.pure(Right(MutationWriteOutcome.Rejected("business rejection"))))
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
          storedJobs <- PublisherBridge.first(database.getCollection("jobs").countDocuments())
          storedEvents <- PublisherBridge.first(database.getCollection("event_outbox").countDocuments())
          storedEmbeddingWork <- PublisherBridge.first(database.getCollection("embedding_work").countDocuments())
          storedMutationReceipts <- PublisherBridge.first(database.getCollection("mutation_receipts").countDocuments())
          receiptRolledBackStored <- jobs.find(receiptRolledBackJob.id).flatMap(requireResult)
          rollbackResult <- jobs.createWithEvents(rolledBackJob, now, List(duplicateEvent))
          rolledBackStored <- jobs.find(rolledBackJob.id).flatMap(requireResult)
          jobsAfterRollback <- PublisherBridge.first(database.getCollection("jobs").countDocuments())
          eventsAfterRollback <- PublisherBridge.first(database.getCollection("event_outbox").countDocuments())
          embeddingAfterRollback <- PublisherBridge.first(database.getCollection("embedding_work").countDocuments())
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

  test("recruiter deletion tombstones the account and closes only newly open jobs") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        val database = client.getDatabase(s"delete_success_${UUID.randomUUID()}")
        val jobs = MongoJobRepository.transactional(database, client)
        val users = MongoUserRepository.transactional(database, client)
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
          _ <- MongoHiringSetup.initialize(database)
          _ <- users.insert(recruiter).flatMap(requireResult)
          _ <- List(firstOpen, secondOpen, alreadyClosed).traverse_ { value =>
            PublisherBridge.first(database.getCollection("jobs").insertOne(MongoHiringCodecs.job(value))).void
          }
          deletion <- users.deleteAccount(recruiterId, deletionTime, "deleted-account")
          storedRecruiter <- users.find(recruiterId).flatMap(requireResult)
          storedFirst <- jobs.find(firstOpen.id).flatMap(requireResult)
          storedSecond <- jobs.find(secondOpen.id).flatMap(requireResult)
          storedClosed <- jobs.find(alreadyClosed.id).flatMap(requireResult)
          closeEvents <- PublisherBridge.collectWithin(
            database
              .getCollection("event_outbox")
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

  test("recruiter deletion rolls back on malformed owned job") {
    replicaSet.use { instance =>
      awaitPrimary(instance) *> MongoDatabaseProbe.clientResource(uri(instance)).use { client =>
        val database = client.getDatabase(s"delete_rollback_${UUID.randomUUID()}")
        val users = MongoUserRepository.transactional(database, client)
        val recruiterId = UserId(UUID.randomUUID())
        val recruiter = recruiterUser(recruiterId)

        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- users.insert(recruiter).flatMap(requireResult)
          _ <- PublisherBridge.first(
            database
              .getCollection("jobs")
              .insertOne(
                new Document("_id", UUID.randomUUID().toString)
                  .append("recruiterId", recruiterId.value.toString)
                  .append("status", JobStatus.Open.toString)
                  .append("createdAt", Date.from(now.plusSeconds(1))),
                new InsertOneOptions().bypassDocumentValidation(true)
              )
          )
          deletion <- users.deleteAccount(recruiterId, now.plusSeconds(2), "deleted-account")
          storedRecruiter <- users.find(recruiterId).flatMap(requireResult)
          eventsAfterFailure <- PublisherBridge.first(database.getCollection("event_outbox").countDocuments())
        } yield {
          assertEquals(deletion, Left(RepositoryError.Unavailable))
          assertEquals(storedRecruiter, Some(recruiter))
          assertEquals(eventsAfterFailure.map(_.longValue), Some(0L))
        }
      }
    }
  }

  private def recruiterUser(id: UserId): User =
    User(
      id,
      Some(s"$id@example.com"),
      "Recruiter",
      UserRole.Recruiter,
      Some(UserProfile.Recruiter(RecruiterProfile("Hiring Co", Some("Lead Recruiter")))),
      now
    )
}
