package com.example.graphQL.cats.service.job

import cats.effect.IO
import cats.effect.Ref
import com.example.graphQL.cats.service.{ActorContext, AuthenticationError, UseCaseError}
import com.example.graphQL.cats.shared.pagination.{JobPageRequest, PageSize}
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location, User, UserRole}
import java.util.UUID
import munit.CatsEffectSuite

class JobServiceSpec extends CatsEffectSuite {
  private val page = JobPageRequest(None, None, PageSize.fromInt(10).toOption.get)

  test("createJob derives owner from ActorContext and rejects candidates") {
    for {
      users <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.UserId, User]](Map.empty)
      jobs <- Ref.of[IO, Map[JobId, Job]](Map.empty)
      _ <- users.set(Map(candidateId -> candidate, recruiterId -> recruiter))
      service = JobService[IO](InMemoryUsers(users), InMemoryJobs(jobs))
      input = CreateJobInput(
        " New role ",
        " Build services ",
        List(" Scala "),
        Set(" Cats Effect "),
        Location("Cyprus", "Nicosia", remote = true),
        JobStatus.Draft
      )
      created <- service.createJob(ActorContext(recruiterId, UserRole.Recruiter), input, now, jobId)
      rejected <- service.createJob(ActorContext(candidateId, UserRole.Candidate), input, now, JobId(UUID.randomUUID()))
    } yield {
      assertEquals(created.map(_.recruiterId), Right(recruiterId))
      assertEquals(created.map(_.title), Right("New role"))
      assertEquals(rejected, Left(UseCaseError.domain(DomainError.Forbidden)))
    }
  }

  test("createJob rejects a Closed initial status through JobLifecycle") {
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(recruiterId -> recruiter))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map.empty)
      service = JobService[IO](InMemoryUsers(users), InMemoryJobs(jobs))
      result <- service.createJob(
        ActorContext(recruiterId, UserRole.Recruiter),
        CreateJobInput("New role", "Build services", List("Scala"), Set("Scala"), Location("Cyprus", "Nicosia", remote = true), JobStatus.Closed),
        now,
        jobId
      )
      stored <- jobs.get
    } yield {
      assertEquals(result, Left(UseCaseError.domain(DomainError.InvalidInitialJobStatus(JobStatus.Closed))))
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
      service = JobService[IO](InMemoryUsers(users), InMemoryJobs(jobs))
      input = CreateJobInput(
        "New role",
        "Build services",
        List("Scala"),
        Set("Cats Effect"),
        Location("Cyprus", "Nicosia", remote = true),
        JobStatus.Open
      )
      created <- service.createJob(ActorContext(recruiterId, UserRole.Recruiter), input, now, jobId)
      rejected <- service.createJob(ActorContext(candidateId, UserRole.Candidate), input, now, JobId(UUID.randomUUID()))
      updated <- service.updateJob(ActorContext(recruiterId, UserRole.Recruiter), jobId, updatedInput, later)
    } yield {
      assertEquals(created.map(_.id), Right(jobId))
      assertEquals(rejected, Left(UseCaseError.domain(DomainError.Forbidden)))
      assertEquals(updated.map(_.id), Right(jobId))
    }
  }

  test("forged admin context is rejected unless the stored actor is an admin") {
    for {
      users <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.UserId, User]](
        Map(recruiterId -> recruiter)
      )
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob))
      service = JobService[IO](InMemoryUsers(users), InMemoryJobs(jobs))
      result <- service.closeJob(ActorContext(recruiterId, UserRole.Admin), jobId, later)
    } yield assertEquals(result, Left(UseCaseError.domain(DomainError.Forbidden)))
  }

  test("stored admin without singleton marker is rejected") {
    val unseededAdmin = admin.copy(adminSingleton = false)
    for {
      users <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.UserId, User]](
        Map(adminId -> unseededAdmin)
      )
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob))
      service = JobService[IO](InMemoryUsers(users), InMemoryJobs(jobs))
      result <- service.closeJob(ActorContext(adminId, UserRole.Admin), jobId, later)
    } yield assertEquals(result, Left(UseCaseError.authentication(AuthenticationError.SingletonAdminViolation)))
  }

  test("myJobs resolves stored actor before listing recruiter jobs") {
    val otherRecruiterId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000006"))
    val otherJobId = JobId(UUID.fromString("00000000-0000-0000-0000-000000000007"))
    val otherJob = openJob.copy(id = otherJobId, recruiterId = otherRecruiterId)
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(recruiterId -> recruiter))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob, otherJobId -> otherJob))
      service = JobService[IO](InMemoryUsers(users), InMemoryJobs(jobs))
      result <- service.myJobs(ActorContext(recruiterId, UserRole.Recruiter), page)
    } yield assertEquals(result.map(_.map(_.id)), Right(List(jobId)))
  }

  test("myJobs rejects mismatched actor role before repository ownership lookup") {
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(recruiterId -> recruiter))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob))
      service = JobService[IO](InMemoryUsers(users), InMemoryJobs(jobs))
      result <- service.myJobs(ActorContext(recruiterId, UserRole.Candidate), page)
    } yield assertEquals(result, Left(UseCaseError.domain(DomainError.Forbidden)))
  }

  test("myJobs lists all manageable jobs for singleton admin") {
    val otherRecruiterId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000006"))
    val otherJobId = JobId(UUID.fromString("00000000-0000-0000-0000-000000000007"))
    val otherJob = openJob.copy(id = otherJobId, recruiterId = otherRecruiterId)
    for {
      users <- Ref.of[IO, Map[UserId, User]](Map(adminId -> admin))
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob, otherJobId -> otherJob))
      service = JobService[IO](InMemoryUsers(users), InMemoryJobs(jobs))
      result <- service.myJobs(ActorContext(adminId, UserRole.Admin), page)
    } yield assertEquals(result.map(_.map(_.id).toSet), Right(Set(jobId, otherJobId)))
  }

  test("closeJob records explicit close timestamp") {
    for {
      users <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.UserId, User]](
        Map(recruiterId -> recruiter)
      )
      jobs <- Ref.of[IO, Map[JobId, Job]](Map(jobId -> openJob))
      service = JobService[IO](InMemoryUsers(users), InMemoryJobs(jobs))
      result <- service.closeJob(ActorContext(recruiterId, UserRole.Recruiter), jobId, later)
      stored <- jobs.get.map(_.get(jobId))
    } yield {
      assertEquals(result.map(_.status), Right(JobStatus.Closed))
      assertEquals(result.map(_.closedAt), Right(Some(later)))
      assertEquals(stored.map(_.closedAt), Some(Some(later)))
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
      service = JobService[IO](InMemoryUsers(users), InMemoryJobs(jobs))
      result <- service.viewJob(ActorContext(candidateId, UserRole.Candidate), jobId)
    } yield assertEquals(result, Left(UseCaseError.domain(DomainError.Forbidden)))
  }
}
