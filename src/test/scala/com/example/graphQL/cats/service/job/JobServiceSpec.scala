package com.example.graphQL.cats.service.job

import cats.effect.{Clock, IO}
import cats.effect.Ref
import cats.effect.std.UUIDGen
import com.example.graphQL.cats.service.{
  ActorContext,
  AuthenticationError,
  Diagnostics,
  LogEvent,
  LogField,
  TestHiringServices,
  UseCaseError
}
import com.example.graphQL.cats.service.protocol.IdempotencyRequest
import com.example.graphQL.cats.domain.pagination.{JobPageRequest, PageSize}
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.service.search.EmbeddingWorkPublisher
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{GeoPoint, Job, JobStatus, Location, User, UserRole}
import com.example.graphQL.cats.service.events.OperationalEventType
import java.util.UUID
import scala.concurrent.duration.*
import munit.CatsEffectSuite

class JobServiceSpec extends CatsEffectSuite {
  private val page = JobPageRequest(None, None, PageSize.fromInt(10).toOption.get)
  private val requestId = UUID.fromString("00000000-0000-0000-0000-000000000008")

  private def request(operation: String): IdempotencyRequest =
    IdempotencyRequest.fromCanonicalInput(requestId, operation)

  test("createJob derives owner from ActorContext and rejects candidates") {
    for {
      users <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.UserId, User]](Map.empty)
      jobs <- Ref.of[IO, Map[JobId, Job]](Map.empty)
      _ <- users.set(Map(candidateId -> candidate, recruiterId -> recruiter))
      service <- deterministicService(InMemoryUsers(users), InMemoryJobs(jobs), List(now), List(jobId.value))
      input = CreateJobInput(
        " New role ",
        " Build services ",
        List(" Scala "),
        Set(" Cats Effect "),
        Location("Cyprus", "Nicosia", remote = true),
        JobStatus.Draft
      )
      created <- service
        .createJob(request("create-recruiter"), ActorContext(recruiterId, UserRole.Recruiter), input)
        .value
      rejected <- service
        .createJob(request("create-candidate"), ActorContext(candidateId, UserRole.Candidate), input)
        .value
    } yield {
      assertEquals(created.map(_.recruiterId), Right(recruiterId))
      assertEquals(created.map(_.title), Right("New role"))
      assertEquals(rejected, Left(UseCaseError.Domain(DomainError.Forbidden)))
    }
  }

  test("createJob rejects a Closed initial status through JobLifecycle") {
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(recruiterId -> recruiter))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map.empty)
      service <- deterministicService(InMemoryUsers(users), InMemoryJobs(jobs), List(now), List(jobId.value))
      result <- service
        .createJob(
          request("create-closed"),
          ActorContext(recruiterId, UserRole.Recruiter),
          CreateJobInput(
            "New role",
            "Build services",
            List("Scala"),
            Set("Scala"),
            Location("Cyprus", "Nicosia", remote = true),
            JobStatus.Closed
          )
        )
        .value
      stored <- jobs.get
    } yield {
      assertEquals(result, Left(UseCaseError.Domain(DomainError.InvalidInitialJobStatus(JobStatus.Closed))))
      assertEquals(stored, Map.empty)
    }
  }

  test("createJob and updateJob preserve their write results for durable repository handoff") {
    val updatedInput = UpdateJobInput(
      "Updated role",
      "Build updated services",
      List("Scala", "Cats Effect"),
      Set("Scala", "MongoDB"),
      Location("Cyprus", "Nicosia", remote = true)
    )
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(recruiterId -> recruiter, candidateId -> candidate))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map.empty)
      service <- deterministicService(InMemoryUsers(users), InMemoryJobs(jobs), List(now, later), List(jobId.value))
      input = CreateJobInput(
        "New role",
        "Build services",
        List("Scala"),
        Set("Cats Effect"),
        Location("Cyprus", "Nicosia", remote = true),
        JobStatus.Open
      )
      created <- service.createJob(request("create"), ActorContext(recruiterId, UserRole.Recruiter), input).value
      rejected <- service
        .createJob(request("create-forbidden"), ActorContext(candidateId, UserRole.Candidate), input)
        .value
      updated <- service
        .updateJob(request("update"), ActorContext(recruiterId, UserRole.Recruiter), jobId, updatedInput)
        .value
    } yield {
      assertEquals(created.map(_.id), Right(jobId))
      assertEquals(rejected, Left(UseCaseError.Domain(DomainError.Forbidden)))
      assertEquals(updated.map(_.id), Right(jobId))
    }
  }

  test("createJob and updateJob preserve caller coordinates in their results and persisted jobs") {
    val initialPoint = GeoPoint(35.2, 33.4)
    val updatedPoint = GeoPoint(34.7, 33.0)
    val input = CreateJobInput(
      "New role",
      "Build services",
      List("Scala"),
      Set("Cats Effect"),
      Location("Cyprus", "Nicosia", remote = false, Some(initialPoint)),
      JobStatus.Open
    )
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(recruiterId -> recruiter))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map.empty)
      service <- deterministicService(InMemoryUsers(users), InMemoryJobs(jobs), List(now, later), List(jobId.value))
      created <- service
        .createJob(request("create-with-coordinates"), ActorContext(recruiterId, UserRole.Recruiter), input)
        .value
      storedAfterCreate <- jobs.get
      updated <- service
        .updateJob(
          request("update-with-coordinates"),
          ActorContext(recruiterId, UserRole.Recruiter),
          jobId,
          UpdateJobInput(
            input.title,
            input.description,
            input.requirements,
            input.skills,
            input.location.copy(coordinates = Some(updatedPoint))
          )
        )
        .value
      storedAfterUpdate <- jobs.get
    } yield {
      assertEquals(created.map(_.location.coordinates), Right(Some(initialPoint)))
      assertEquals(storedAfterCreate.get(jobId).flatMap(_.location.coordinates), Some(initialPoint))
      assertEquals(updated.map(_.location.coordinates), Right(Some(updatedPoint)))
      assertEquals(storedAfterUpdate.get(jobId).flatMap(_.location.coordinates), Some(updatedPoint))
    }
  }

  test("createJob and updateJob reject invalid coordinates without changing persisted jobs") {
    val initialPoint = GeoPoint(35.2, 33.4)
    val existing = openJob.copy(location = Location("Cyprus", "Nicosia", remote = false, Some(initialPoint)))
    val invalidLocation = existing.location.copy(coordinates = Some(GeoPoint(35.2, 181d)))
    val expectedError =
      UseCaseError.ValidationFailed(cats.data.NonEmptyList.one(DomainValidationError.InvalidCoordinates))
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(recruiterId -> recruiter))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> existing))
      service <- deterministicService(
        InMemoryUsers(users),
        InMemoryJobs(jobs),
        List(now, later),
        List(new UUID(0L, 99L))
      )
      created <- service
        .createJob(
          request("create-invalid-coordinates"),
          ActorContext(recruiterId, UserRole.Recruiter),
          CreateJobInput(
            existing.title,
            existing.description,
            existing.requirements,
            existing.skills,
            invalidLocation,
            JobStatus.Open
          )
        )
        .value
      storedAfterCreate <- jobs.get
      updated <- service
        .updateJob(
          request("update-invalid-coordinates"),
          ActorContext(recruiterId, UserRole.Recruiter),
          jobId,
          UpdateJobInput(existing.title, existing.description, existing.requirements, existing.skills, invalidLocation)
        )
        .value
      storedAfterUpdate <- jobs.get
    } yield {
      assertEquals(created, Left(expectedError))
      assertEquals(storedAfterCreate, Map(jobId -> existing))
      assertEquals(updated, Left(expectedError))
      assertEquals(storedAfterUpdate, Map(jobId -> existing))
    }
  }

  test("successful writes wake durable embedding work without publishing a second work record") {
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(recruiterId -> recruiter))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map.empty)
      wakes <- Ref.of[IO, Int](0)
      publisher = new EmbeddingWorkPublisher {
        override def wake: IO[Unit] = wakes.update(_ + 1)
      }
      service <- deterministicService(InMemoryUsers(users), InMemoryJobs(jobs), List(now), List(jobId.value), publisher)
      result <- service
        .createJob(
          request("create-with-wake"),
          ActorContext(recruiterId, UserRole.Recruiter),
          CreateJobInput(
            "New role",
            "Build services",
            List("Scala"),
            Set("Scala"),
            Location("Cyprus", "Nicosia", remote = true),
            JobStatus.Open
          )
        )
        .value
      wakeCount <- wakes.get
    } yield {
      assertEquals(result.map(_.id), Right(jobId))
      assertEquals(wakeCount, 1)
    }
  }

  test("failed embedding wake records safe diagnostics without failing the committed job") {
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(recruiterId -> recruiter))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map.empty)
      captured <- Ref.of[IO, List[(LogEvent, Map[LogField, String])]](Nil)
      diagnostics = new Diagnostics {
        override def event(
            event: LogEvent,
            requestId: Option[String],
            fields: => Map[LogField, String]
        ): IO[Unit] = captured.update((event, fields) :: _)
      }
      publisher = new EmbeddingWorkPublisher {
        override def wake: IO[Unit] = IO.raiseError(new IllegalStateException("sensitive wake detail"))
      }
      service <- deterministicService(
        InMemoryUsers(users),
        InMemoryJobs(jobs),
        List(now),
        List(jobId.value),
        publisher,
        diagnostics
      )
      result <- service
        .createJob(
          request("create-with-failed-wake"),
          ActorContext(recruiterId, UserRole.Recruiter),
          CreateJobInput(
            "New role",
            "Build services",
            List("Scala"),
            Set("Scala"),
            Location("Cyprus", "Nicosia", true),
            JobStatus.Draft
          )
        )
        .value
      entries <- captured.get
      stored <- jobs.get
    } yield {
      assertEquals(result.map(_.id), Right(jobId))
      assert(stored.contains(jobId))
      assertEquals(entries.map(_._1), List(LogEvent.EmbeddingWakeFailed))
      assertEquals(entries.head._2.get(LogField.ErrorType), Some("java.lang.IllegalStateException"))
      assert(!entries.head._2.values.exists(_.contains("sensitive wake detail")))
    }
  }

  test("createJob hands a JOB_CREATED fact to the durable repository boundary") {
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(recruiterId -> recruiter))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map.empty)
      outbox <- Ref.of[IO, Vector[com.example.graphQL.cats.service.events.OperationalEventEnvelope]](Vector.empty)
      jobRepository = InMemoryJobs(jobs, Some(outbox))
      service <- deterministicService(InMemoryUsers(users), jobRepository, List(now), List(jobId.value))
      result <- service
        .createJob(
          request("create-with-event"),
          ActorContext(recruiterId, UserRole.Recruiter),
          CreateJobInput(
            "New role",
            "Build services",
            List("Scala"),
            Set("Scala"),
            Location("Cyprus", "Nicosia", remote = true),
            JobStatus.Open
          )
        )
        .value
      events <- jobRepository.allOperationalEvents
    } yield {
      assertEquals(result.map(_.id), Right(jobId))
      assertEquals(events.map(_.eventType), Vector(OperationalEventType.JOB_CREATED))
      assertEquals(events.map(_.aggregateId), Vector(jobId.value.toString))
      assert(!events.head.payload.noSpaces.contains("recruiter@example.com"))
    }
  }

  test("forged admin context is rejected unless the stored actor is an admin") {
    for {
      users <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.UserId, User]](
        Map(recruiterId -> recruiter)
      )
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob))
      service <- deterministicService(InMemoryUsers(users), InMemoryJobs(jobs), List(later), Nil)
      result <- service.closeJob(request("close-forged-admin"), ActorContext(recruiterId, UserRole.Admin), jobId).value
    } yield assertEquals(result, Left(UseCaseError.Domain(DomainError.Forbidden)))
  }

  test("stored admin without singleton marker is rejected") {
    val unseededAdmin = admin.copy(adminSingleton = false)
    for {
      users <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.UserId, User]](
        Map(adminId -> unseededAdmin)
      )
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob))
      service <- deterministicService(InMemoryUsers(users), InMemoryJobs(jobs), List(later), Nil)
      result <- service.closeJob(request("close-unseeded-admin"), ActorContext(adminId, UserRole.Admin), jobId).value
    } yield assertEquals(result, Left(UseCaseError.Authentication(AuthenticationError.SingletonAdminViolation)))
  }

  test("myJobs resolves stored actor before listing recruiter jobs") {
    val otherRecruiterId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000006"))
    val otherJobId = JobId(UUID.fromString("00000000-0000-0000-0000-000000000007"))
    val otherJob = openJob.copy(id = otherJobId, recruiterId = otherRecruiterId)
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(recruiterId -> recruiter))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob, otherJobId -> otherJob))
      service = TestHiringServices.job(InMemoryUsers(users), InMemoryJobs(jobs))
      result <- service.myJobs(ActorContext(recruiterId, UserRole.Recruiter), page).value
    } yield assertEquals(result.map(_.map(_.id)), Right(List(jobId)))
  }

  test("myJobs rejects mismatched actor role before repository ownership lookup") {
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(recruiterId -> recruiter))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob))
      service = TestHiringServices.job(InMemoryUsers(users), InMemoryJobs(jobs))
      result <- service.myJobs(ActorContext(recruiterId, UserRole.Candidate), page).value
    } yield assertEquals(result, Left(UseCaseError.Domain(DomainError.Forbidden)))
  }

  test("myJobs lists all manageable jobs for singleton admin") {
    val otherRecruiterId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000006"))
    val otherJobId = JobId(UUID.fromString("00000000-0000-0000-0000-000000000007"))
    val otherJob = openJob.copy(id = otherJobId, recruiterId = otherRecruiterId)
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(adminId -> admin))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob, otherJobId -> otherJob))
      service = TestHiringServices.job(InMemoryUsers(users), InMemoryJobs(jobs))
      result <- service.myJobs(ActorContext(adminId, UserRole.Admin), page).value
    } yield assertEquals(result.map(_.map(_.id).toSet), Right(Set(jobId, otherJobId)))
  }

  test("closeJob records explicit close timestamp") {
    for {
      users <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.UserId, User]](
        Map(recruiterId -> recruiter)
      )
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob))
      service <- deterministicService(InMemoryUsers(users), InMemoryJobs(jobs), List(later), Nil)
      result <- service.closeJob(request("close"), ActorContext(recruiterId, UserRole.Recruiter), jobId).value
      stored <- jobs.get.map(_.get(jobId))
    } yield {
      assertEquals(result.map(_.status), Right(JobStatus.Closed))
      assertEquals(result.map(_.closedAt), Right(Some(later)))
      assertEquals(stored.map(_.closedAt), Some(Some(later)))
    }
  }

  test("publishJob opens a draft and records a job update event") {
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(recruiterId -> recruiter))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob.copy(status = JobStatus.Draft)))
      outbox <- Ref.of[IO, Vector[com.example.graphQL.cats.service.events.OperationalEventEnvelope]](Vector.empty)
      jobRepository = InMemoryJobs(jobs, Some(outbox))
      service <- deterministicService(InMemoryUsers(users), jobRepository, List(later), Nil)
      result <- service.publishJob(request("publish"), ActorContext(recruiterId, UserRole.Recruiter), jobId).value
      stored <- jobs.get.map(_.get(jobId))
      events <- jobRepository.allOperationalEvents
    } yield {
      assertEquals(result.map(_.status), Right(JobStatus.Open))
      assertEquals(result.map(_.updatedAt), Right(later))
      assertEquals(stored.map(_.status), Some(JobStatus.Open))
      assertEquals(events.map(_.eventType), Vector(OperationalEventType.JOB_UPDATED))
    }
  }

  test("candidate can view only open jobs") {
    for {
      users <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.UserId, User]](
        Map(candidateId -> candidate)
      )
      jobs <- Ref.of[IO, Map[JobId, Job]](
        Map(jobId -> openJob.copy(status = JobStatus.Closed))
      )
      service = TestHiringServices.job(InMemoryUsers(users), InMemoryJobs(jobs))
      result <- service.viewJob(ActorContext(candidateId, UserRole.Candidate), jobId).value
    } yield assertEquals(result, Left(UseCaseError.Domain(DomainError.Forbidden)))
  }

  private def deterministicService(
      users: InMemoryUsers,
      jobs: InMemoryJobs,
      times: List[java.time.Instant],
      ids: List[UUID],
      embeddingWork: EmbeddingWorkPublisher = com.example.graphQL.cats.service.search.TestEmbeddingWorkPublisher.noop,
      diagnostics: Diagnostics = Diagnostics.noop
  ): IO[JobService] =
    for {
      timeValues <- Ref.of[IO, List[java.time.Instant]](times)
      idValues <- Ref.of[IO, List[UUID]](ids)
    } yield new JobService(
      users,
      jobs,
      embeddingWork,
      com.example.graphQL.cats.service.mutation.TestIdempotency.noop,
      diagnostics,
      clock = new Clock[IO] {
        override val applicative: cats.Applicative[IO] = IO.asyncForIO
        override def realTime: IO[FiniteDuration] = nextValue(timeValues, "timestamp").map(_.toEpochMilli.millis)
        override def monotonic: IO[FiniteDuration] = IO.pure(Duration.Zero)
      },
      uuidGen = new UUIDGen[IO] {
        override def randomUUID: IO[UUID] = nextValue(idValues, "UUID")
      }
    )

  private def nextValue[A](values: Ref[IO, List[A]], label: String): IO[A] =
    values
      .modify {
        case head :: tail => (tail, Right(head))
        case Nil          => (Nil, Left(new AssertionError(s"No deterministic $label remains")))
      }
      .flatMap(IO.fromEither)
}
