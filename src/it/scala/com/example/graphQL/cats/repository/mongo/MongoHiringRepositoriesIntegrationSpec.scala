package com.example.graphQL.cats.repository.mongo

import cats.effect.{Deferred, IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.api.auth.JwtActorAuthenticator
import com.example.graphQL.cats.api.http.{Admission, HiringApiRoutes}
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, HealthService}
import com.example.graphQL.cats.repository.protocol.{
  EmbeddingError, EmbeddingInput, EmbeddingService, EmbeddingVector
}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.job.CreateJobInput
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.shared.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest, PageSize}
import com.example.graphQL.cats.shared.search.JobSearchFilter
import com.example.graphQL.cats.config.{JwtAuthConfig, VectorSearchConfig}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{
  Application, ApplicationEvent, ApplicationStatus, CandidateProfile, EmbeddingMeta, EntityEmbedding, Job, JobStatus, Location,
  RecruiterProfile, SearchableText, User, UserProfile, UserRole
}
import com.example.graphQL.cats.runtime.MongoHiringRuntime
import io.circe.Json
import com.mongodb.client.model.{Filters, IndexOptions, Indexes}
import munit.CatsEffectSuite
import org.bson.Document
import org.http4s.{Header, Method, Request, Uri}
import org.http4s.circe.*
import org.typelevel.ci.CIString
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import pdi.jwt.JwtCirce

import java.time.{Duration, Instant}
import java.util.{Date, UUID}
import scala.jdk.CollectionConverters.*
import scala.concurrent.duration.*

class MongoHiringRepositoriesIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

  private final case class ExplainEvidence(
      indexName: String,
      executionTimeMillis: Int,
      totalDocsExamined: Int,
      totalKeysExamined: Int,
      nReturned: Int,
      fixtureSize: Int
  )

  private val image = "mongo:8.0.32-noble@sha256:01354084d2ae665d2e79b79b0cdc50c2c0c98873618912d9a2c8c9cb5c3d24e6"

  private final class Standalone extends GenericContainer[Standalone](DockerImageName.parse(image))

  private val now = Instant.parse("2026-09-16T10:15:30Z")
  private val later = Instant.parse("2026-09-16T11:15:30Z")
  private val candidateId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000101"))
  private val recruiterId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000102"))
  private val adminId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000103"))
  private val jobId = JobId(UUID.fromString("00000000-0000-0000-0000-000000000104"))
  private val applicationId = ApplicationId(UUID.fromString("00000000-0000-0000-0000-000000000105"))
  private val duplicateApplicationId = ApplicationId(UUID.fromString("00000000-0000-0000-0000-000000000106"))
  private val eventId = ApplicationEventId(UUID.fromString("00000000-0000-0000-0000-000000000107"))
  private val secondEventId = ApplicationEventId(UUID.fromString("00000000-0000-0000-0000-000000000108"))
  private val thirdEventId = ApplicationEventId(UUID.fromString("00000000-0000-0000-0000-000000000109"))

  test("setup backfills legacy canonical names before creating the unique index") {
    container.use { uri =>
      MongoDatabaseProbe.clientResource(uri).use { client =>
        val database = client.getDatabase("hiring_legacy_names")
        val users = database.getCollection("users")
        val legacyCandidate = new Document("_id", "legacy-user-1")
          .append("name", "Legacy Candidate")
          .append("role", "Candidate")
          .append("profile", new Document("skills", List("Scala").asJava))
          .append("createdAt", Date.from(now))
        val legacyRecruiter = new Document("_id", "legacy-user-2")
          .append("name", "Legacy Recruiter")
          .append("role", "Recruiter")
          .append("recruiterProfile", new Document("organizationName", "Acme"))
          .append("createdAt", Date.from(now))
        for {
          _ <- PublisherBridge.first(users.insertMany(List(legacyCandidate, legacyRecruiter).asJava))
          _ <- MongoHiringSetup.initialize(database)
          candidate <- PublisherBridge.first(users.find(Filters.eq("_id", "legacy-user-1")))
          recruiter <- PublisherBridge.first(users.find(Filters.eq("_id", "legacy-user-2")))
          indexes <- indexes(users)
        } yield {
          assertEquals(candidate.map(_.getString("nameCanonical")), Some("legacy candidate"))
          assertEquals(candidate.flatMap(_.get("profile") match {
            case value: Document => Some(value)
            case _ => None
          }).map(_.getString("kind")), Some("Candidate"))
          assertEquals(recruiter.map(_.getString("nameCanonical")), Some("legacy recruiter"))
          assertEquals(recruiter.flatMap(_.get("profile") match {
            case value: Document => Some(value)
            case _ => None
          }).map(_.getString("kind")), Some("Recruiter"))
          assert(recruiter.forall(!_.containsKey("recruiterProfile")))
          assert(indexes.contains(MongoHiringSetup.UsersNameIndex))
        }
      }
    }
  }

  test("setup fails closed for legacy canonical-name collisions") {
    container.use { uri =>
      MongoDatabaseProbe.clientResource(uri).use { client =>
        val database = client.getDatabase("hiring_legacy_name_collision")
        val users = database.getCollection("users")
        val first = new Document("_id", "legacy-user-1").append("name", "Same Name").append("role", "Candidate")
          .append("profile", new Document("skills", List("Scala").asJava)).append("createdAt", Date.from(now))
        val second = new Document("_id", "legacy-user-2").append("name", " same name ").append("role", "Recruiter")
          .append("recruiterProfile", new Document("organizationName", "Acme")).append("createdAt", Date.from(now))
        for {
          _ <- PublisherBridge.first(users.insertMany(List(first, second).asJava))
          result <- MongoHiringSetup.initialize(database).attempt
          indexes <- indexes(users)
        } yield {
          assert(result.left.toOption.exists(_.getMessage.contains("duplicate canonical account names")))
          assert(!indexes.contains(MongoHiringSetup.UsersNameIndex))
        }
      }
    }
  }

  test("setup is idempotent and creates named indexes plus migration record") {
    container.use { uri =>
      MongoDatabaseProbe.clientResource(uri).use { client =>
        val database = client.getDatabase("hiring_setup")
        val migrations = database.getCollection("schema_migrations")
        val existingAppliedAt = Date.from(Instant.parse("2026-09-16T00:00:00Z"))
        val existingDomainMigration = new Document("_id", MongoHiringSetup.HiringDomainMongoMigrationId)
          .append("schemaVersion", 1)
          .append("appliedAt", existingAppliedAt)
          .append("description", "Previously applied hiring domain MongoDB collections and indexes")
          .append("checksum", MongoHiringSetup.HiringDomainMongoMigrationId)
        for {
          _ <- PublisherBridge.first(migrations.insertOne(existingDomainMigration))
          _ <- PublisherBridge.first(database.getCollection("users").createIndex(
            Indexes.ascending("emailCanonical"),
            new IndexOptions().name(MongoHiringSetup.UsersEmailIndex).unique(true)
          ))
          _ <- MongoHiringSetup.initialize(database)
          graphqlPerformanceMigrationAfterFirstRun <- PublisherBridge.first(migrations
            .find(new Document("_id", MongoHiringSetup.HiringGraphQLSearchIndexMigrationId)))
          _ <- MongoHiringSetup.initialize(database)
          users <- indexes(database.getCollection("users"))
          jobs <- indexes(database.getCollection("jobs"))
          applications <- indexes(database.getCollection("applications"))
          events <- indexes(database.getCollection("application_events"))
          migration <- PublisherBridge.first(migrations
            .find(new Document("_id", MongoHiringSetup.HiringDomainMongoMigrationId)))
          graphqlPerformanceMigration <- PublisherBridge.first(migrations
            .find(new Document("_id", MongoHiringSetup.HiringGraphQLSearchIndexMigrationId)))
          adminJobListingMigration <- PublisherBridge.first(migrations
            .find(new Document("_id", MongoHiringSetup.HiringAdminJobListingIndexMigrationId)))
          vectorSearchMigration <- PublisherBridge.first(migrations
            .find(new Document("_id", MongoHiringSetup.HiringVectorSearchMigrationId)))
          userAccountMigration <- PublisherBridge.first(migrations
            .find(new Document("_id", MongoHiringSetup.UserAccountMigrationId)))
          profileMigration <- PublisherBridge.first(migrations
            .find(new Document("_id", MongoHiringSetup.UserProfileOneOfMigrationId)))
          emailIndexMigration <- PublisherBridge.first(migrations
            .find(new Document("_id", MongoHiringSetup.UserEmailSparseIndexMigrationId)))
          accountRegistry <- PublisherBridge.first(database.getCollection("account_registry")
            .find(new Document("_id", "user-account-registry")))
        } yield {
          assertEquals(MongoHiringSetup.HiringDomainMongoMigrationId, "phase-2-domain-mongodb-v1")
          assertIndex(users, MongoHiringSetup.UsersEmailIndex, new Document("emailCanonical", 1), unique = Some(true), partial = None)
          assert(users(MongoHiringSetup.UsersEmailIndex).getBoolean("sparse", false))
          assertIndex(users, MongoHiringSetup.UsersNameIndex, new Document("nameCanonical", 1), unique = Some(true), partial = None)
          assertIndex(users, MongoHiringSetup.UsersStatusCreatedIndex,
            new Document("accountStatus", 1).append("createdAt", -1).append("_id", -1), None, None)
          assertIndex(users, MongoHiringSetup.UsersRoleStatusCreatedIndex,
            new Document("role", 1).append("accountStatus", 1).append("createdAt", -1).append("_id", -1), None, None)
          assertIndex(users, MongoHiringSetup.UsersAdminSingletonIndex, new Document("adminSingletonKey", 1), unique = Some(true),
            partial = Some(new Document("role", "Admin")))
          assertEquals(userAccountMigration.map(_.getString("_id")), Some(MongoHiringSetup.UserAccountMigrationId))
          assertEquals(profileMigration.map(_.getString("_id")), Some(MongoHiringSetup.UserProfileOneOfMigrationId))
          assertEquals(emailIndexMigration.map(_.getString("_id")), Some(MongoHiringSetup.UserEmailSparseIndexMigrationId))
          assertEquals(accountRegistry.map(_.getString("state")), Some("Uninitialized"))
          assertIndex(jobs, MongoHiringSetup.JobsRecruiterStatusCreatedIndex,
            new Document("recruiterId", 1).append("status", 1).append("createdAt", -1).append("_id", -1), None, None)
          assertIndex(jobs, MongoHiringSetup.JobsRecruiterCreatedIndex,
            new Document("recruiterId", 1).append("createdAt", -1).append("_id", -1), None, None)
          assertIndex(jobs, MongoHiringSetup.JobsOpenCreatedIndex,
            new Document("status", 1).append("createdAt", -1).append("_id", -1), None, None)
          assertIndex(jobs, MongoHiringSetup.JobsOpenCityCreatedIndex,
            new Document("status", 1).append("location.city", 1).append("createdAt", -1).append("_id", -1), None, None)
          assertIndex(applications, MongoHiringSetup.ApplicationsCandidateJobIndex,
            new Document("candidateId", 1).append("jobId", 1), unique = Some(true), partial = None)
          assertIndex(applications, MongoHiringSetup.ApplicationsCandidateStatusCreatedIndex,
            new Document("candidateId", 1).append("status", 1).append("createdAt", -1).append("_id", -1), None, None)
          assertIndex(applications, MongoHiringSetup.ApplicationsCandidateCreatedIndex,
            new Document("candidateId", 1).append("createdAt", -1).append("_id", -1), None, None)
          assertIndex(applications, MongoHiringSetup.ApplicationsJobStatusCreatedIndex,
            new Document("jobId", 1).append("status", 1).append("createdAt", -1).append("_id", -1), None, None)
          assertIndex(applications, MongoHiringSetup.ApplicationsJobCreatedIndex,
            new Document("jobId", 1).append("createdAt", -1).append("_id", -1), None, None)
          assertIndex(events, MongoHiringSetup.ApplicationEventsApplicationCreatedIndex,
            new Document("applicationId", 1).append("occurredAt", -1).append("_id", -1), None, None)
          assertIndex(jobs, MongoHiringSetup.JobsCreatedIndex,
            new Document("createdAt", -1).append("_id", -1), None, None)
          assertIndex(jobs, MongoHiringSetup.JobsEmbeddingMetaIndex,
            new Document("embeddingMeta.model", 1).append("embeddingMeta.version", 1).append("status", 1)
              .append("location.city", 1).append("recruiterId", 1), None, None)
          assertIndex(users, MongoHiringSetup.UsersEmbeddingMetaIndex,
            new Document("embeddingMeta.model", 1).append("embeddingMeta.version", 1).append("role", 1), None, None)
          assert(migration.exists(_.getInteger("schemaVersion") == 1))
          assert(migration.exists(_.getDate("appliedAt") == existingAppliedAt))
          assert(migration.exists(_.getString("description") == "Previously applied hiring domain MongoDB collections and indexes"))
          assert(migration.exists(_.getString("checksum") == MongoHiringSetup.HiringDomainMongoMigrationId))
          assert(graphqlPerformanceMigrationAfterFirstRun.exists(_.containsKey("appliedAt")))
          assertEquals(
            graphqlPerformanceMigration.flatMap(migration => Option(migration.getDate("appliedAt"))),
            graphqlPerformanceMigrationAfterFirstRun.flatMap(migration => Option(migration.getDate("appliedAt")))
          )
          assert(graphqlPerformanceMigration.exists(_.getString("checksum") == MongoHiringSetup.HiringGraphQLSearchIndexMigrationId))
          assert(adminJobListingMigration.exists(_.getString("checksum") == MongoHiringSetup.HiringAdminJobListingIndexMigrationId))
          assert(vectorSearchMigration.exists(_.getString("checksum") == MongoHiringSetup.HiringVectorSearchMigrationId))
        }
      }
    }
  }

  test("repositories round-trip users, jobs, applications, and status history") {
    container.use { uri =>
      MongoDatabaseProbe.clientResource(uri).use { client =>
        val database = client.getDatabase("hiring_roundtrip")
        val users = MongoUserRepository(database)
        val jobs = MongoJobRepository(database)
        val applications = MongoApplicationRepository.standalone(database)
        val page = ApplicationPageRequest(None, None, PageSize.fromInt(10).toOption.get)
        val jobPageRequest = JobPageRequest(Some(JobStatus.Open), None, PageSize.fromInt(10).toOption.get)
        val eventPage = ApplicationEventPageRequest(None, PageSize.fromInt(10).toOption.get)
        val candidateProfile = CandidateProfile(
          Set("Scala", "Cats Effect", "MongoDB"),
          Some("Builds backend services"),
          Some("resume://candidate-101")
        )
        val candidate = User(candidateId, Some("candidate@example.com"), "Candidate", UserRole.Candidate,
          Some(UserProfile.Candidate(candidateProfile)), now)
        val recruiter = User(recruiterId, Some("recruiter@example.com"), "Recruiter", UserRole.Recruiter,
          Some(UserProfile.Recruiter(RecruiterProfile("Acme", None))), now)
        val admin = User(adminId, Some("admin@example.com"), "Admin", UserRole.Admin, None, now, adminSingleton = true)
        val unseededAdmin =
          User(UserId(UUID.fromString("00000000-0000-0000-0000-000000000111")), Some("admin2@example.com"), "Admin 2", UserRole.Admin, None, now)
        val job = Job(
          jobId,
          recruiterId,
          "Senior Scala Developer",
          "Build services",
          List("Scala"),
          Set("Scala", "Cats"),
          Location("Ukraine", "Kyiv", remote = true),
          JobStatus.Open,
          now,
          now
        )
        val application = Application.create(applicationId, candidateId, jobId, now)
        val initialEvent = ApplicationEvent(eventId, applicationId, None, ApplicationStatus.Created, candidateId, now, None, None)
        val updatedApplication = application.copy(status = ApplicationStatus.Accepted, updatedAt = later)
        val acceptedEvent =
          ApplicationEvent(secondEventId, applicationId, Some(ApplicationStatus.Created), ApplicationStatus.Accepted, recruiterId, later, None, None)
        val staleEvent =
          ApplicationEvent(thirdEventId, applicationId, Some(ApplicationStatus.Created), ApplicationStatus.Interview, recruiterId, later, None, None)
        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- users.insert(candidate)
          _ <- users.insert(recruiter)
          _ <- users.insert(admin)
          rejectedAdmin <- users.insert(unseededAdmin)
          _ <- jobs.create(job)
          updatedJob <- jobs.update(job.copy(title = "Principal Scala Developer", updatedAt = later))
          staleJob <- jobs.update(job.copy(title = "Stale Scala Developer", updatedAt = later))
          _ <- applications.createForOpenJob(updatedJob.toOption.get, application, initialEvent)
          _ <- applications.updateStatus(updatedApplication, acceptedEvent)
          staleStatus <- applications.updateStatus(application.copy(status = ApplicationStatus.Interview, updatedAt = later), staleEvent)
          foundCandidate <- users.find(candidateId)
          foundAdmin <- users.find(adminId)
          foundJob <- jobs.find(jobId)
          foundApplication <- applications.find(applicationId)
          foundManyUsers <- users.findMany(List(candidateId, recruiterId))
          foundManyJobs <- jobs.findMany(List(jobId))
          openJobs <- jobs.findOpen(JobSearchFilter(Some("Kyiv"), Set("Scala"), None), jobPageRequest)
          allJobs <- jobs.findAll(jobPageRequest)
          recruiterJobs <- jobs.findByRecruiter(recruiterId, jobPageRequest)
          candidatePage <- applications.findByCandidate(candidateId, page)
          jobPage <- applications.findByJob(jobId, page)
          eventHistory <- applications.history(applicationId, eventPage)
          history <- PublisherBridge.all(database.getCollection("application_events").find())
        } yield {
          assertEquals(rejectedAdmin, Left(RepositoryError.Conflict))
          assertEquals(staleJob, Left(RepositoryError.Conflict))
          assertEquals(staleStatus, Left(RepositoryError.Conflict))
          assertEquals(foundCandidate.flatMap(_.email), Some("candidate@example.com"))
          assertEquals(foundCandidate.flatMap(_.profile), Some(UserProfile.Candidate(candidateProfile)))
          assertEquals(foundAdmin.map(_.adminSingleton), Some(true))
          assertEquals(updatedJob.map(_.version), Right(1L))
          assertEquals(foundJob.map(_.title), Some("Principal Scala Developer"))
          assertEquals(foundJob.map(_.version), Some(2L))
          assertEquals(foundApplication.map(_.status), Some(ApplicationStatus.Accepted))
          assertEquals(foundManyUsers.map(_.id).toSet, Set(candidateId, recruiterId))
          assertEquals(foundManyJobs.map(_.id), List(jobId))
          assertEquals(openJobs.map(_.id), List(jobId))
          assertEquals(allJobs.map(_.id), List(jobId))
          assertEquals(recruiterJobs.map(_.id), List(jobId))
          assertEquals(candidatePage.map(_.id), List(applicationId))
          assertEquals(jobPage.map(_.id), List(applicationId))
          assertEquals(eventHistory.map(_.newStatus).toSet, Set(ApplicationStatus.Created, ApplicationStatus.Accepted))
          assertEquals(history.size, 2)
        }
      }
    }
  }

  test("job embedding updates are guarded by observed job version") {
    container.use { uri =>
      MongoDatabaseProbe.clientResource(uri).use { client =>
        val database = client.getDatabase("hiring_job_embedding_guard")
        val jobs = MongoJobRepository(database)
        val job = jobFixture(jobId, JobStatus.Open)
        val currentHash = SourceHash.sha256(SearchableText.job(job.copy(title = "Principal Scala Developer", updatedAt = later)))
        val currentEmbedding = EntityEmbedding(
          List(0.1f, 0.2f),
          EmbeddingMeta("voyage-4-lite", 1, currentHash, later)
        )
        val staleEmbedding = EntityEmbedding(
          List(0.9f, 0.8f),
          EmbeddingMeta("voyage-4-lite", 1, "stale-hash", later)
        )
        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- jobs.create(job)
          updated <- jobs.update(job.copy(title = "Principal Scala Developer", updatedAt = later))
          observed <- IO.fromEither(updated.leftMap(error => new AssertionError(s"job update failed: $error")))
          freshResult <- jobs.updateEmbedding(jobId, observed.version, currentEmbedding)
          staleResult <- jobs.updateEmbedding(jobId, 0L, staleEmbedding)
          stored <- jobs.find(jobId)
        } yield {
          assertEquals(freshResult, Right(()))
          assertEquals(staleResult, Left(RepositoryError.Conflict))
          assertEquals(stored.flatMap(_.embedding).map(_.meta.sourceHash), Some(currentHash))
          assertEquals(stored.flatMap(_.embedding).map(_.values), Some(List(0.1f, 0.2f)))
        }
      }
    }
  }

  test("transactional application repository rejects closed-job submissions without partial writes") {
    replicaSetContainer.use { uri =>
      MongoDatabaseProbe.clientResource(uri).use { client =>
        val database = client.getDatabase("hiring_closed_submit")
        val jobs = MongoJobRepository(database)
        val applications = MongoApplicationRepository.transactional(database, client)
        val openJob = jobFixture(jobId, JobStatus.Open)
        val application = Application.create(applicationId, candidateId, jobId, now)
        val initialEvent = ApplicationEvent(eventId, applicationId, None, ApplicationStatus.Created, candidateId, now, None, None)
        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- jobs.create(openJob)
          closed <- jobs.update(openJob.copy(status = JobStatus.Closed, updatedAt = later, closedAt = Some(later)))
          result <- applications.createForOpenJob(openJob, application, initialEvent)
          storedJob <- jobs.find(jobId)
          storedApplication <- applications.find(applicationId)
          history <- PublisherBridge.all(database.getCollection("application_events").find())
        } yield {
          assertEquals(closed.map(_.status), Right(JobStatus.Closed))
          assertEquals(closed.map(_.closedAt), Right(Some(later)))
          assertEquals(storedJob.flatMap(_.closedAt), Some(later))
          assertEquals(result, Left(RepositoryError.Conflict))
          assertEquals(storedApplication, None)
          assertEquals(history, Nil)
        }
      }
    }
  }

  test("transactional application repository rolls back application when initial event insert fails") {
    replicaSetContainer.use { uri =>
      MongoDatabaseProbe.clientResource(uri).use { client =>
        val database = client.getDatabase("hiring_submit_rollback")
        val jobs = MongoJobRepository(database)
        val applications = MongoApplicationRepository.transactional(database, client)
        val job = jobFixture(jobId, JobStatus.Open)
        val application = Application.create(applicationId, candidateId, jobId, now)
        val event = ApplicationEvent(eventId, applicationId, None, ApplicationStatus.Created, candidateId, now, None, None)
        val conflictingEvent = ApplicationEvent(eventId, duplicateApplicationId, None, ApplicationStatus.Created, candidateId, now, None, None)
        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- jobs.create(job)
          _ <- PublisherBridge.first(database.getCollection("application_events").insertOne(MongoHiringCodecs.event(conflictingEvent)))
          result <- applications.createForOpenJob(job, application, event)
          storedApplication <- applications.find(applicationId)
          history <- PublisherBridge.all(database.getCollection("application_events").find())
        } yield {
          assertEquals(result, Left(RepositoryError.Conflict))
          assertEquals(storedApplication, None)
          assertEquals(history.size, 1)
        }
      }
    }
  }

  test("transactional status changes roll back application update when event insert fails") {
    replicaSetContainer.use { uri =>
      MongoDatabaseProbe.clientResource(uri).use { client =>
        val database = client.getDatabase("hiring_status_rollback")
        val jobs = MongoJobRepository(database)
        val applications = MongoApplicationRepository.transactional(database, client)
        val job = jobFixture(jobId, JobStatus.Open)
        val application = Application.create(applicationId, candidateId, jobId, now)
        val initialEvent = ApplicationEvent(eventId, applicationId, None, ApplicationStatus.Created, candidateId, now, None, None)
        val duplicateEvent = ApplicationEvent(secondEventId, duplicateApplicationId, None, ApplicationStatus.Created, candidateId, now, None, None)
        val accepted = application.copy(status = ApplicationStatus.Accepted, updatedAt = later)
        val acceptedEvent =
          ApplicationEvent(secondEventId, applicationId, Some(ApplicationStatus.Created), ApplicationStatus.Accepted, recruiterId, later, None, None)
        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- jobs.create(job)
          _ <- applications.createForOpenJob(job, application, initialEvent)
          _ <- PublisherBridge.first(database.getCollection("application_events").insertOne(MongoHiringCodecs.event(duplicateEvent)))
          result <- applications.updateStatus(accepted, acceptedEvent)
          storedApplication <- applications.find(applicationId)
          history <- PublisherBridge.all(database.getCollection("application_events").find())
        } yield {
          assertEquals(result, Left(RepositoryError.Conflict))
          assertEquals(storedApplication.map(_.status), Some(ApplicationStatus.Created))
          assertEquals(history.size, 2)
        }
      }
    }
  }

  test("transactional duplicate submissions keep one application and one initial event") {
    replicaSetContainer.use { uri =>
      MongoDatabaseProbe.clientResource(uri).use { client =>
        val database = client.getDatabase("hiring_duplicate_race")
        val jobs = MongoJobRepository(database)
        val applications = MongoApplicationRepository.transactional(database, client)
        val job = jobFixture(jobId, JobStatus.Open)
        val application = Application.create(applicationId, candidateId, jobId, now)
        val duplicate = application.copy(id = duplicateApplicationId)
        val firstEvent = ApplicationEvent(eventId, applicationId, None, ApplicationStatus.Created, candidateId, now, None, None)
        val duplicateEvent = ApplicationEvent(secondEventId, duplicateApplicationId, None, ApplicationStatus.Created, candidateId, now, None, None)
        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- jobs.create(job)
          ready <- Deferred[IO, Unit]
          first = ready.get *> applications.createForOpenJob(job, application, firstEvent)
          second = ready.get *> applications.createForOpenJob(job, duplicate, duplicateEvent)
          fiberA <- first.start
          fiberB <- second.start
          _ <- ready.complete(())
          results <- (fiberA.joinWithNever, fiberB.joinWithNever).tupled
          stored <- PublisherBridge.all(database.getCollection("applications").find(Filters.eq("candidateId", candidateId.value.toString)))
          history <- PublisherBridge.all(database.getCollection("application_events").find())
        } yield {
          val outcomes = List(results._1, results._2)
          assert(outcomes.contains(Right(())))
          assert(outcomes.contains(Left(RepositoryError.DuplicateApplication)))
          assertEquals(stored.size, 1)
          assertEquals(history.size, 1)
        }
      }
    }
  }

  test("application list queries have explain evidence for status and no-status indexes") {
    container.use { uri =>
      MongoDatabaseProbe.clientResource(uri).use { client =>
        val database = client.getDatabase("hiring_explain")
        val jobs = MongoJobRepository(database)
        val applications = MongoApplicationRepository.standalone(database)
        val page = ApplicationPageRequest(None, None, PageSize.fromInt(10).toOption.get)
        val statusPage = ApplicationPageRequest(Some(ApplicationStatus.Created), None, PageSize.fromInt(10).toOption.get)
        val job = jobFixture(jobId, JobStatus.Open)
        val wrongJob = jobFixture(JobId(UUID.fromString("00000000-0000-0000-0000-000000000112")), JobStatus.Open)
        val application = Application.create(applicationId, candidateId, jobId, now)
        val event = ApplicationEvent(eventId, applicationId, None, ApplicationStatus.Created, candidateId, now, None, None)
        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- jobs.create(job)
          _ <- jobs.create(wrongJob)
          _ <- applications.createForOpenJob(job, application, event)
          inconsistent <- applications.createForOpenJob(wrongJob, application.copy(id = duplicateApplicationId), event.copy(id = secondEventId))
          candidateNoStatus <- explainIndex(database, "candidateId", candidateId.value.toString, page)
          candidateStatus <- explainIndex(database, "candidateId", candidateId.value.toString, statusPage)
          jobNoStatus <- explainIndex(database, "jobId", jobId.value.toString, page)
          jobStatus <- explainIndex(database, "jobId", jobId.value.toString, statusPage)
          openJobSearch <- explainJobSearchIndex(database, city = None)
          openCityJobSearch <- explainJobSearchIndex(database, city = Some("Kyiv"))
          recruiterJobs <- explainRecruiterJobListingIndex(database, status = None)
          recruiterOpenJobs <- explainRecruiterJobListingIndex(database, status = Some(JobStatus.Open))
          allJobs <- explainJobListingIndex(database)
          historyIndex <- explainHistoryIndex(database)
        } yield {
          assertEquals(inconsistent, Left(RepositoryError.Conflict))
          assertExplain(candidateNoStatus, MongoHiringSetup.ApplicationsCandidateCreatedIndex, fixtureSize = 1)
          assertExplain(candidateStatus, MongoHiringSetup.ApplicationsCandidateStatusCreatedIndex, fixtureSize = 1)
          assertExplain(jobNoStatus, MongoHiringSetup.ApplicationsJobCreatedIndex, fixtureSize = 1)
          assertExplain(jobStatus, MongoHiringSetup.ApplicationsJobStatusCreatedIndex, fixtureSize = 1)
          assertExplain(openJobSearch, MongoHiringSetup.JobsOpenCreatedIndex, fixtureSize = 2)
          assertExplain(openCityJobSearch, MongoHiringSetup.JobsOpenCityCreatedIndex, fixtureSize = 2)
          assertExplain(recruiterJobs, MongoHiringSetup.JobsRecruiterCreatedIndex, fixtureSize = 2)
          assertExplain(recruiterOpenJobs, MongoHiringSetup.JobsRecruiterStatusCreatedIndex, fixtureSize = 2)
          assertExplain(allJobs, MongoHiringSetup.JobsCreatedIndex, fixtureSize = 2)
          assertExplain(historyIndex, MongoHiringSetup.ApplicationEventsApplicationCreatedIndex, fixtureSize = 1)
        }
      }
    }
  }

  test("served JWT GraphQL submit initializes Mongo setup before the first hiring write") {
    replicaSetContainer.use { uri =>
      MongoHiringRuntime.resource(uri, "hiring_served_jwt", Diagnostics.noop).use { runtime =>
        val jwt = JwtAuthConfig(Some("01234567890123456789012345678901"), "hiring-platform-local", "hiring-graphql-api")
        val candidateUser = User(candidateId, Some("candidate@example.com"), "Candidate", UserRole.Candidate,
          Some(UserProfile.Candidate(CandidateProfile(Set("Scala"), None, None))), now)
        val recruiterUser = User(recruiterId, Some("recruiter@example.com"), "Recruiter", UserRole.Recruiter,
          Some(UserProfile.Recruiter(RecruiterProfile("Acme", None))), now)
        for {
          admission <- Admission.create(16)
          _ <- MongoDatabaseProbe.clientResource(uri).use { client =>
            val database = client.getDatabase("hiring_served_jwt")
            val users = new MongoUserRepository(database)
            val jobs = new MongoJobRepository(database)
            users.insert(candidateUser) *> users.insert(recruiterUser) *> jobs.create(jobFixture(jobId, JobStatus.Open)).void
          }
          authenticator = JwtActorAuthenticator(jwt, runtime.userAuthenticator, IO.pure(now))
          http = HiringApiRoutes(
            HealthService(runtime.probe, Diagnostics.noop),
            Diagnostics.noop,
            admission,
            Some(runtime.services),
            authenticator.authenticate,
            runtime.ensureSetup
          ).app
          token = signedToken(candidateId, jwt)
          submit = s"""mutation {
                      |  submitApplication(input: { jobId: "${jobId.value}" }) {
                      |    application { id status }
                      |    errors { code }
                      |  }
                      |}""".stripMargin
          first <- http(graphqlRequest(submit, token)).flatMap(_.as[Json])
          duplicate <- http(graphqlRequest(submit, token)).flatMap(_.as[Json])
          database <- MongoDatabaseProbe.clientResource(uri).use { client =>
            val db = client.getDatabase("hiring_served_jwt")
            for {
              indexNames <- indexes(db.getCollection("applications")).map(_.keySet)
              migration <- PublisherBridge.first(db.getCollection("schema_migrations")
                .find(new Document("_id", MongoHiringSetup.HiringDomainMongoMigrationId)))
            } yield (indexNames, migration)
          }
        } yield {
          assertEquals(first.hcursor.downField("data").downField("submitApplication")
            .downField("application").get[String]("status"), Right("Created"))
          assertEquals(first.hcursor.downField("data").downField("submitApplication")
            .downField("errors").focus.flatMap(_.asArray).map(_.size), Some(0))
          assertEquals(duplicate.hcursor.downField("data").downField("submitApplication")
            .downField("errors").downArray.get[String]("code"), Right("DUPLICATE_APPLICATION"))
          assert(database._1.contains(MongoHiringSetup.ApplicationsCandidateJobIndex))
          assert(database._2.exists(_.getString("checksum") == MongoHiringSetup.HiringDomainMongoMigrationId))
        }
      }
    }
  }

  test("enabled vector runtime fails readiness without Atlas indexes but wires semantic search and embedding jobs") {
    replicaSetContainer.use { uri =>
      MongoHiringRuntime.resource(uri, "hiring_vector_runtime", Diagnostics.noop, vectorConfig,
        (config, _) => FakeEmbeddingService(config)).use { runtime =>
        val recruiterUser = User(recruiterId, Some("recruiter@example.com"), "Recruiter", UserRole.Recruiter,
          Some(UserProfile.Recruiter(RecruiterProfile("Acme", None))), now)
        val create = CreateJobInput(
          "Vector Scala Developer",
          "Build semantic search services",
          List("Scala and MongoDB"),
          Set("Scala", "MongoDB"),
          Location("Ukraine", "Kyiv", remote = true),
          JobStatus.Open
        )
        for {
          setup <- runtime.ensureSetup
          _ = assert(!setup)
          _ = assert(runtime.services.semanticSearchService.nonEmpty)
          _ <- MongoDatabaseProbe.clientResource(uri).use { client =>
            new MongoUserRepository(client.getDatabase("hiring_vector_runtime")).insert(recruiterUser).void
          }
          created <- runtime.services.jobService.createJob(ActorContext(recruiterId, UserRole.Recruiter), create, now, jobId)
          job <- IO.fromEither(created.leftMap(error => new AssertionError(s"createJob failed: $error")))
          embedded <- eventually(MongoDatabaseProbe.clientResource(uri).use { client =>
            new MongoJobRepository(client.getDatabase("hiring_vector_runtime")).find(jobId).map(_.flatMap(_.embedding))
          })(_.nonEmpty)
        } yield {
          assertEquals(embedded.map(_.meta.model), Some(vectorConfig.voyageModel))
          assertEquals(embedded.map(_.meta.version), Some(vectorConfig.embeddingVersion))
          assertEquals(embedded.map(_.meta.sourceHash), Some(SourceHash.sha256(SearchableText.job(job))))
        }
      }
    }
  }

  private def container: Resource[IO, String] =
    Resource.make(IO.blocking {
      val instance = new Standalone
      val _ = instance.withExposedPorts(27017)
        .withCommand("mongod", "--bind_ip_all")
        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90)))
      try {
        instance.start()
        (instance, s"mongodb://${instance.getHost}:${instance.getMappedPort(27017)}")
      } catch {
        case error: Throwable =>
          instance.stop()
          throw error
      }
    }) { case (instance, _) => IO.blocking(instance.stop()) }.map(_._2)

  private def replicaSetContainer: Resource[IO, String] =
    Resource.make(IO.blocking {
      val instance = new Standalone
      val _ = instance.withExposedPorts(27017)
        .withCommand("mongod", "--bind_ip_all", "--replSet", "rs0")
        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(90)))
      try {
        instance.start()
        val init = instance.execInContainer(
          "mongosh",
          "--quiet",
          "--eval",
          "rs.initiate({_id:'rs0',members:[{_id:0,host:'localhost:27017'}]})"
        )
        if (init.getExitCode != 0) throw new IllegalStateException(init.getStderr)
        waitForPrimary(instance)
        (instance, s"mongodb://${instance.getHost}:${instance.getMappedPort(27017)}/?replicaSet=rs0&directConnection=true")
      } catch {
        case error: Throwable =>
          instance.stop()
          throw error
      }
    }) { case (instance, _) => IO.blocking(instance.stop()) }.map(_._2)

  private def waitForPrimary(instance: Standalone): Unit = {
    val deadline = System.nanoTime() + 30.seconds.toNanos
    var primary = false
    var lastError = ""
    while (!primary && System.nanoTime() < deadline) {
      val result = instance.execInContainer("mongosh", "--quiet", "--eval", "db.hello().isWritablePrimary")
      primary = result.getExitCode == 0 && result.getStdout.trim == "true"
      lastError = result.getStderr
      if (!primary) Thread.sleep(250L)
    }
    if (!primary) throw new IllegalStateException(s"MongoDB replica set primary was not elected: $lastError")
  }

  private def indexes(collection: com.mongodb.reactivestreams.client.MongoCollection[Document]): IO[Map[String, Document]] =
    PublisherBridge.all(collection.listIndexes()).map(_.map(index => index.getString("name") -> index).toMap)

  private def graphqlRequest(query: String, token: String): Request[IO] =
    Request[IO](Method.POST, Uri.unsafeFromString("/graphql"))
      .putHeaders(Header.Raw(CIString("Authorization"), s"Bearer $token"))
      .withEntity(Json.obj("query" -> Json.fromString(query)))

  private def signedToken(userId: UserId, jwt: JwtAuthConfig): String =
    JwtCirce.encode(
      Json.obj("alg" -> Json.fromString("HS256")),
      Json.obj(
        "sub" -> Json.fromString(userId.value.toString),
        "iss" -> Json.fromString(jwt.issuer),
        "aud" -> Json.fromString(jwt.audience),
        "exp" -> Json.fromLong(now.plusSeconds(300).getEpochSecond),
        "role" -> Json.fromString("Admin")
      ),
      jwt.hmacSecret.getOrElse(fail("Missing JWT test secret"))
    )

  private def assertIndex(
      actual: Map[String, Document],
      name: String,
      keys: Document,
      unique: Option[Boolean],
      partial: Option[Document]
  ): Unit = {
    val index = actual.getOrElse(name, fail(s"Missing index $name"))
    assertEquals(index.get("key", classOf[Document]), keys)
    unique.foreach(expected => assertEquals(index.getBoolean("unique", false), expected))
    partial.foreach(expected => assertEquals(index.get("partialFilterExpression", classOf[Document]), expected))
  }

  private val vectorConfig: VectorSearchConfig =
    VectorSearchConfig(
      enabled = true,
      voyageApiKey = Some("synthetic-voyage-key"),
      voyageEndpoint = "https://example.test/embeddings",
      voyageModel = "voyage-4-lite",
      voyageDimension = 1024,
      embeddingVersion = 1,
      queueSize = 16,
      parallelism = 1,
      timeoutMillis = 1000,
      jobVectorIndex = "jobs_embedding_vector",
      candidateVectorIndex = "candidates_embedding_vector",
      jobLexicalIndex = "jobs_text_search",
      indexReadyTimeoutMillis = 120000,
      indexPollIntervalMillis = 1000,
      numCandidates = 10
    )

  private final case class FakeEmbeddingService(config: VectorSearchConfig) extends EmbeddingService[IO] {
    override def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]] =
      IO.pure(Right(EmbeddingVector(List.fill(config.voyageDimension)(0.1f), config.voyageModel, config.voyageDimension)))
  }

  private def eventually[A](effect: IO[A])(accepted: A => Boolean): IO[A] = {
    def loop(remaining: Int): IO[A] =
      effect.flatMap { value =>
        if (accepted(value) || remaining <= 0) IO.pure(value)
        else IO.sleep(100.millis) *> loop(remaining - 1)
      }
    loop(30)
  }

  private def jobFixture(id: JobId, status: JobStatus): Job =
    Job(
      id,
      recruiterId,
      "Senior Scala Developer",
      "Build services",
      List("Scala"),
      Set("Scala", "Cats"),
      Location("Ukraine", "Kyiv", remote = true),
      status,
      now,
      now
    )

  private def explainIndex(
      database: com.mongodb.reactivestreams.client.MongoDatabase,
      field: String,
      id: String,
      page: ApplicationPageRequest
  ): IO[Option[ExplainEvidence]] = {
    val filter = page.status
      .map(status => new Document(field, id).append("status", status.toString))
      .getOrElse(new Document(field, id))
    val command = new Document("explain",
      new Document("find", "applications")
        .append("filter", filter)
        .append("sort", new Document("createdAt", -1).append("_id", -1))
        .append("limit", page.pageSize.value)
    ).append("verbosity", "executionStats")
    PublisherBridge.first(database.runCommand(command)).map(_.flatMap(explainEvidence(_, fixtureSize = 1)))
  }

  private def explainJobSearchIndex(
      database: com.mongodb.reactivestreams.client.MongoDatabase,
      city: Option[String]
  ): IO[Option[ExplainEvidence]] = {
    val filter = city
      .map(value => new Document("status", JobStatus.Open.toString).append("location.city", value))
      .getOrElse(new Document("status", JobStatus.Open.toString))
    val command = new Document("explain",
      new Document("find", "jobs")
        .append("filter", filter)
        .append("sort", new Document("createdAt", -1).append("_id", -1))
        .append("limit", 10)
    ).append("verbosity", "executionStats")
    PublisherBridge.first(database.runCommand(command)).map(_.flatMap(explainEvidence(_, fixtureSize = 2)))
  }

  private def explainRecruiterJobListingIndex(
      database: com.mongodb.reactivestreams.client.MongoDatabase,
      status: Option[JobStatus]
  ): IO[Option[ExplainEvidence]] = {
    val filter = status
      .map(value => new Document("recruiterId", recruiterId.value.toString).append("status", value.toString))
      .getOrElse(new Document("recruiterId", recruiterId.value.toString))
    val command = new Document("explain",
      new Document("find", "jobs")
        .append("filter", filter)
        .append("sort", new Document("createdAt", -1).append("_id", -1))
        .append("limit", 10)
    ).append("verbosity", "executionStats")
    PublisherBridge.first(database.runCommand(command)).map(_.flatMap(explainEvidence(_, fixtureSize = 2)))
  }

  private def explainJobListingIndex(database: com.mongodb.reactivestreams.client.MongoDatabase): IO[Option[ExplainEvidence]] = {
    val command = new Document("explain",
      new Document("find", "jobs")
        .append("filter", new Document())
        .append("sort", new Document("createdAt", -1).append("_id", -1))
        .append("limit", 10)
    ).append("verbosity", "executionStats")
    PublisherBridge.first(database.runCommand(command)).map(_.flatMap(explainEvidence(_, fixtureSize = 2)))
  }

  private def explainHistoryIndex(database: com.mongodb.reactivestreams.client.MongoDatabase): IO[Option[ExplainEvidence]] = {
    val command = new Document("explain",
      new Document("find", "application_events")
        .append("filter", new Document("applicationId", applicationId.value.toString))
        .append("sort", new Document("occurredAt", -1).append("_id", -1))
        .append("limit", 10)
    ).append("verbosity", "executionStats")
    PublisherBridge.first(database.runCommand(command)).map(_.flatMap(explainEvidence(_, fixtureSize = 1)))
  }

  private def assertExplain(actual: Option[ExplainEvidence], expectedIndex: String, fixtureSize: Int): Unit = {
    val evidence = actual.getOrElse(fail(s"Missing explain evidence for $expectedIndex"))
    assertEquals(evidence.indexName, expectedIndex)
    assertEquals(evidence.fixtureSize, fixtureSize)
    assert(evidence.executionTimeMillis >= 0)
    assert(evidence.totalDocsExamined >= 0)
    assert(evidence.totalKeysExamined >= 0)
    assert(evidence.nReturned >= 0)
    assert(evidence.nReturned <= fixtureSize)
  }

  private def explainEvidence(document: Document, fixtureSize: Int): Option[ExplainEvidence] =
    for {
      stats <- Option(document.get("executionStats", classOf[Document]))
      indexName <- findIndexName(document)
      executionTimeMillis <- Option(stats.get("executionTimeMillis", classOf[Number])).map(_.intValue)
      totalDocsExamined <- Option(stats.get("totalDocsExamined", classOf[Number])).map(_.intValue)
      totalKeysExamined <- Option(stats.get("totalKeysExamined", classOf[Number])).map(_.intValue)
      nReturned <- Option(stats.get("nReturned", classOf[Number])).map(_.intValue)
    } yield ExplainEvidence(indexName, executionTimeMillis, totalDocsExamined, totalKeysExamined, nReturned, fixtureSize)

  private def findIndexName(document: Document): Option[String] =
    Option(document.getString("indexName")).orElse(
      document.values().toArray.toList.view.flatMap(value => findNestedIndexName(value).toList).headOption
    )

  private def findNestedIndexName(value: Any): Option[String] =
    value match {
      case nested: Document => findIndexName(nested)
      case values: java.util.List[?] =>
        values.toArray.toList.view.flatMap(value => findNestedIndexName(value).toList).headOption
      case _ => None
    }
}
