package com.example.graphQL.cats.application.service

import cats.effect.IO
import cats.effect.Ref
import com.example.graphQL.cats.application.ActorContext
import com.example.graphQL.cats.application.AuthenticationError
import com.example.graphQL.cats.application.service.ServiceFixtures.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.JobId
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location, User, UserRole}
import java.util.UUID
import munit.CatsEffectSuite

class JobServiceSpec extends CatsEffectSuite {
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
      assertEquals(rejected, Left(DomainError.Forbidden))
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
    } yield assertEquals(result, Left(DomainError.Forbidden))
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
    } yield assertEquals(result, Left(AuthenticationError.SingletonAdminViolation))
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
    } yield assertEquals(result, Left(DomainError.Forbidden))
  }
}
