package com.example.graphQL.cats.infrastructure.mongo

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.application.port.{ApplicationPageRequest, PageSize, RepositoryError}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, Job, JobStatus, Location, User, UserRole}
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

  test("setup is idempotent and creates named indexes plus migration record") {
    container.use { uri =>
      MongoDatabaseProbe.clientResource(uri).use { client =>
        val database = client.getDatabase("phase2_setup")
        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- MongoHiringSetup.initialize(database)
          users <- indexNames(database.getCollection("users"))
          jobs <- indexNames(database.getCollection("jobs"))
          applications <- indexNames(database.getCollection("applications"))
          events <- indexNames(database.getCollection("application_events"))
          migration <- PublisherBridge.first(database.getCollection("schema_migrations")
            .find(new Document("_id", MongoHiringSetup.MigrationId)))
        } yield {
          assert(users.contains(MongoHiringSetup.UsersEmailIndex))
          assert(users.contains(MongoHiringSetup.UsersAdminSingletonIndex))
          assert(jobs.contains(MongoHiringSetup.JobsRecruiterStatusCreatedIndex))
          assert(applications.contains(MongoHiringSetup.ApplicationsCandidateJobIndex))
          assert(applications.contains(MongoHiringSetup.ApplicationsCandidateStatusCreatedIndex))
          assert(applications.contains(MongoHiringSetup.ApplicationsJobStatusCreatedIndex))
          assert(events.contains(MongoHiringSetup.ApplicationEventsApplicationCreatedIndex))
          assert(migration.exists(_.getInteger("schemaVersion") == 1))
        }
      }
    }
  }

  test("repositories round-trip users, jobs, applications, status history, and duplicate application errors") {
    container.use { uri =>
      MongoDatabaseProbe.clientResource(uri).use { client =>
        val database = client.getDatabase("phase2_roundtrip")
        val users = MongoUserRepository(database)
        val jobs = MongoJobRepository(database)
        val applications = MongoApplicationRepository(database)
        val page = ApplicationPageRequest(None, None, PageSize.fromInt(10).toOption.get)
        val candidate = User(candidateId, "candidate@example.com", "Candidate", UserRole.Candidate, None, now)
        val recruiter = User(recruiterId, "recruiter@example.com", "Recruiter", UserRole.Recruiter, None, now)
        val admin = User(adminId, "admin@example.com", "Admin", UserRole.Admin, None, now, adminSingleton = true)
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
        val duplicateApplication = application.copy(id = duplicateApplicationId)
        for {
          _ <- MongoHiringSetup.initialize(database)
          _ <- users.insert(candidate)
          _ <- users.insert(recruiter)
          _ <- users.insert(admin)
          _ <- jobs.create(job)
          _ <- applications.create(application, initialEvent)
          duplicate <- applications.create(duplicateApplication, initialEvent.copy(id = secondEventId))
          _ <- applications.updateStatus(updatedApplication, acceptedEvent)
          foundCandidate <- users.find(candidateId)
          foundAdmin <- users.find(adminId)
          foundJob <- jobs.find(jobId)
          foundApplication <- applications.find(applicationId)
          candidatePage <- applications.findByCandidate(candidateId, page)
          jobPage <- applications.findByJob(jobId, page)
          history <- PublisherBridge.all(database.getCollection("application_events").find())
        } yield {
          assertEquals(duplicate, Left(RepositoryError.DuplicateApplication))
          assertEquals(foundCandidate.map(_.email), Some("candidate@example.com"))
          assertEquals(foundAdmin.map(_.adminSingleton), Some(true))
          assertEquals(foundJob.map(_.skills), Some(Set("Cats", "Scala")))
          assertEquals(foundApplication.map(_.status), Some(ApplicationStatus.Accepted))
          assertEquals(candidatePage.map(_.id), List(applicationId))
          assertEquals(jobPage.map(_.id), List(applicationId))
          assertEquals(history.size, 2)
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

  private def indexNames(collection: com.mongodb.reactivestreams.client.MongoCollection[Document]): IO[Set[String]] =
    PublisherBridge.all(collection.listIndexes()).map(_.map(_.getString("name")).toSet)
}
