package com.example.graphQL.cats.infrastructure.mongo

import cats.effect.{Deferred, IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.application.port.{
  ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest, JobSearchFilter, PageSize, RepositoryError
}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{
  Application, ApplicationEvent, ApplicationStatus, CandidateProfile, Job, JobStatus, Location, User, UserRole
}
import com.mongodb.client.model.Filters
import munit.CatsEffectSuite
import org.bson.Document
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.{Duration, Instant}
import java.util.UUID
import scala.concurrent.duration.*

class MongoHiringRepositoriesIntegrationSpec extends CatsEffectSuite {
  override val munitIOTimeout: FiniteDuration = 5.minutes

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

  test("setup is idempotent and creates named indexes plus migration record") {
    container.use { uri =>
      MongoDatabaseProbe.clientResource(uri).use { client =>
        val database = client.getDatabase("phase2_setup")
        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- MongoHiringSetup.initialize(database)
          users <- indexes(database.getCollection("users"))
          jobs <- indexes(database.getCollection("jobs"))
          applications <- indexes(database.getCollection("applications"))
          events <- indexes(database.getCollection("application_events"))
          migration <- PublisherBridge.first(database.getCollection("schema_migrations")
            .find(new Document("_id", MongoHiringSetup.MigrationId)))
          phase3Migration <- PublisherBridge.first(database.getCollection("schema_migrations")
            .find(new Document("_id", MongoHiringSetup.Phase3MigrationId)))
        } yield {
          assertIndex(users, MongoHiringSetup.UsersEmailIndex, new Document("emailCanonical", 1), unique = Some(true), partial = None)
          assertIndex(users, MongoHiringSetup.UsersAdminSingletonIndex, new Document("adminSingletonKey", 1), unique = Some(true),
            partial = Some(new Document("role", "Admin")))
          assertIndex(jobs, MongoHiringSetup.JobsRecruiterStatusCreatedIndex,
            new Document("recruiterId", 1).append("status", 1).append("createdAt", -1).append("_id", -1), None, None)
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
          assert(migration.exists(_.getInteger("schemaVersion") == 1))
          assert(migration.exists(_.containsKey("appliedAt")))
          assert(migration.exists(_.getString("description").nonEmpty))
          assert(migration.exists(_.getString("checksum") == MongoHiringSetup.MigrationId))
          assert(phase3Migration.exists(_.getString("checksum") == MongoHiringSetup.Phase3MigrationId))
        }
      }
    }
  }

  test("repositories round-trip users, jobs, applications, and status history") {
    container.use { uri =>
      MongoDatabaseProbe.clientResource(uri).use { client =>
        val database = client.getDatabase("phase2_roundtrip")
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
        val candidate = User(candidateId, "candidate@example.com", "Candidate", UserRole.Candidate, Some(candidateProfile), now)
        val recruiter = User(recruiterId, "recruiter@example.com", "Recruiter", UserRole.Recruiter, None, now)
        val admin = User(adminId, "admin@example.com", "Admin", UserRole.Admin, None, now, adminSingleton = true)
        val unseededAdmin =
          User(UserId(UUID.fromString("00000000-0000-0000-0000-000000000111")), "admin2@example.com", "Admin 2", UserRole.Admin, None, now)
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
          recruiterJobs <- jobs.findByRecruiter(recruiterId, jobPageRequest)
          candidatePage <- applications.findByCandidate(candidateId, page)
          jobPage <- applications.findByJob(jobId, page)
          eventHistory <- applications.history(applicationId, eventPage)
          history <- PublisherBridge.all(database.getCollection("application_events").find())
        } yield {
          assertEquals(rejectedAdmin, Left(RepositoryError.Conflict))
          assertEquals(staleJob, Left(RepositoryError.Conflict))
          assertEquals(staleStatus, Left(RepositoryError.Conflict))
          assertEquals(foundCandidate.map(_.email), Some("candidate@example.com"))
          assertEquals(foundCandidate.flatMap(_.profile), Some(candidateProfile))
          assertEquals(foundAdmin.map(_.adminSingleton), Some(true))
          assertEquals(updatedJob.map(_.version), Right(1L))
          assertEquals(foundJob.map(_.title), Some("Principal Scala Developer"))
          assertEquals(foundJob.map(_.version), Some(2L))
          assertEquals(foundApplication.map(_.status), Some(ApplicationStatus.Accepted))
          assertEquals(foundManyUsers.map(_.id).toSet, Set(candidateId, recruiterId))
          assertEquals(foundManyJobs.map(_.id), List(jobId))
          assertEquals(openJobs.map(_.id), List(jobId))
          assertEquals(recruiterJobs.map(_.id), List(jobId))
          assertEquals(candidatePage.map(_.id), List(applicationId))
          assertEquals(jobPage.map(_.id), List(applicationId))
          assertEquals(eventHistory.map(_.newStatus).toSet, Set(ApplicationStatus.Created, ApplicationStatus.Accepted))
          assertEquals(history.size, 2)
        }
      }
    }
  }

  test("transactional application repository rejects closed-job submissions without partial writes") {
    replicaSetContainer.use { uri =>
      MongoDatabaseProbe.clientResource(uri).use { client =>
        val database = client.getDatabase("phase2_closed_submit")
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
        val database = client.getDatabase("phase2_submit_rollback")
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
        val database = client.getDatabase("phase2_status_rollback")
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
        val database = client.getDatabase("phase2_duplicate_race")
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
        val database = client.getDatabase("phase2_explain")
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
          openJobSearch <- explainJobSearchIndex(database)
          historyIndex <- explainHistoryIndex(database)
        } yield {
          assertEquals(inconsistent, Left(RepositoryError.Conflict))
          assertEquals(candidateNoStatus, Some(MongoHiringSetup.ApplicationsCandidateCreatedIndex))
          assertEquals(candidateStatus, Some(MongoHiringSetup.ApplicationsCandidateStatusCreatedIndex))
          assertEquals(jobNoStatus, Some(MongoHiringSetup.ApplicationsJobCreatedIndex))
          assertEquals(jobStatus, Some(MongoHiringSetup.ApplicationsJobStatusCreatedIndex))
          assertEquals(openJobSearch, Some(MongoHiringSetup.JobsOpenCityCreatedIndex))
          assertEquals(historyIndex, Some(MongoHiringSetup.ApplicationEventsApplicationCreatedIndex))
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
  ): IO[Option[String]] = {
    val filter = page.status
      .map(status => new Document(field, id).append("status", status.toString))
      .getOrElse(new Document(field, id))
    val command = new Document("explain",
      new Document("find", "applications")
        .append("filter", filter)
        .append("sort", new Document("createdAt", -1).append("_id", -1))
        .append("limit", page.pageSize.value)
    ).append("verbosity", "executionStats")
    PublisherBridge.first(database.runCommand(command)).map(_.flatMap(findIndexName))
  }

  private def explainJobSearchIndex(database: com.mongodb.reactivestreams.client.MongoDatabase): IO[Option[String]] = {
    val command = new Document("explain",
      new Document("find", "jobs")
        .append("filter", new Document("status", JobStatus.Open.toString).append("location.city", "Kyiv"))
        .append("sort", new Document("createdAt", -1).append("_id", -1))
        .append("limit", 10)
    ).append("verbosity", "executionStats")
    PublisherBridge.first(database.runCommand(command)).map(_.flatMap(findIndexName))
  }

  private def explainHistoryIndex(database: com.mongodb.reactivestreams.client.MongoDatabase): IO[Option[String]] = {
    val command = new Document("explain",
      new Document("find", "application_events")
        .append("filter", new Document("applicationId", applicationId.value.toString))
        .append("sort", new Document("occurredAt", -1).append("_id", -1))
        .append("limit", 10)
    ).append("verbosity", "executionStats")
    PublisherBridge.first(database.runCommand(command)).map(_.flatMap(findIndexName))
  }

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
