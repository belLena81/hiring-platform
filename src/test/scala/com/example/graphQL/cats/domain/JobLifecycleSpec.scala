package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location}
import com.example.graphQL.cats.domain.policy.JobLifecycle
import java.time.Instant
import java.util.UUID
import munit.FunSuite

class JobLifecycleSpec extends FunSuite {
  private val createdAt = Instant.parse("2026-09-16T10:15:30Z")
  private val updatedAt = Instant.parse("2026-09-16T11:15:30Z")
  private val recruiterId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000002"))
  private val jobId = JobId(UUID.fromString("00000000-0000-0000-0000-000000000003"))
  private val draftJob = Job(
    jobId,
    recruiterId,
    "Senior Scala Developer",
    "Build backend services",
    List("Scala"),
    Set("Scala"),
    Location("Ukraine", "Kyiv", remote = true),
    JobStatus.Draft,
    createdAt,
    createdAt
  )

  test("publish moves Draft to Open") {
    val result = JobLifecycle.publish(updatedAt).run(draftJob)

    assertEquals(result.map(_._1.status), Right(JobStatus.Open))
    assertEquals(result.map(_._1.updatedAt), Right(updatedAt))
  }

  test("create rejects Closed as an initial status") {
    val closed = draftJob.copy(status = JobStatus.Closed)
    val result = JobLifecycle.create(closed)

    assertEquals(result, Left(DomainError.InvalidInitialJobStatus(JobStatus.Closed)))
  }

  test("rejected publish retains the original aggregate value") {
    val closed = draftJob.copy(status = JobStatus.Closed)
    val result = JobLifecycle.publish(updatedAt).run(closed)

    assertEquals(result, Left(DomainError.InvalidJobTransition(JobStatus.Closed, JobStatus.Open)))
    assertEquals(closed.status, JobStatus.Closed)
  }

  test("update replaces content without changing owner or status") {
    val input = JobLifecycle.Update(
      title = "Lead Scala Developer",
      description = "Own backend services",
      requirements = List("Cats Effect", "MongoDB"),
      skills = Set("Scala", "Cats"),
      location = Location("Poland", "Warsaw", remote = true),
      updatedAt = updatedAt
    )
    val updated = JobLifecycle.update(input).run(draftJob).map(_._1).toOption.getOrElse(fail("expected job update"))

    assertEquals(updated.title, "Lead Scala Developer")
    assertEquals(updated.recruiterId, recruiterId)
    assertEquals(updated.status, JobStatus.Draft)
    assertEquals(updated.updatedAt, updatedAt)
  }

  test("close moves Draft or Open to Closed and rejects already closed jobs") {
    val closedResult = JobLifecycle.close(updatedAt).run(draftJob.copy(status = JobStatus.Open))
    val closedState = closedResult.map(_._1).toOption.getOrElse(fail("expected job to close"))
    val rejectedResult = JobLifecycle.close(updatedAt).run(closedState)

    assertEquals(closedResult.map(_._1.status), Right(JobStatus.Closed))
    assertEquals(closedState.status, JobStatus.Closed)
    assertEquals(closedState.closedAt, Some(updatedAt))
    assertEquals(rejectedResult, Left(DomainError.InvalidJobTransition(JobStatus.Closed, JobStatus.Closed)))
  }

  test("composed transitions thread Job state through publish and close") {
    val program = for {
      _ <- JobLifecycle.publish(updatedAt)
      _ <- JobLifecycle.close(updatedAt.plusSeconds(60))
    } yield ()

    val result = program.run(draftJob).map(_._1)
    assertEquals(result.map(_.status), Right(JobStatus.Closed))
    assertEquals(result.map(_.closedAt), Right(Some(updatedAt.plusSeconds(60))))
  }

  test("a failed later Job transition short-circuits without returning partial state") {
    val program = for {
      _ <- JobLifecycle.publish(updatedAt)
      _ <- JobLifecycle.publish(updatedAt.plusSeconds(60))
    } yield ()

    assertEquals(program.run(draftJob), Left(DomainError.InvalidJobTransition(JobStatus.Open, JobStatus.Open)))
  }
}
