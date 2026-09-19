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
    val result = JobLifecycle.publish(draftJob, updatedAt)

    assertEquals(result.map(_.status), Right(JobStatus.Open))
    assertEquals(result.map(_.updatedAt), Right(updatedAt))
  }

  test("create rejects Closed as an initial status") {
    val closed = draftJob.copy(status = JobStatus.Closed)
    val result = JobLifecycle.create(closed)

    assertEquals(result, Left(DomainError.InvalidInitialJobStatus(JobStatus.Closed)))
  }

  test("rejected publish retains the original aggregate value") {
    val closed = draftJob.copy(status = JobStatus.Closed)
    val result = JobLifecycle.publish(closed, updatedAt)

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
    val updated = JobLifecycle.update(draftJob, input)

    assertEquals(updated.title, "Lead Scala Developer")
    assertEquals(updated.recruiterId, recruiterId)
    assertEquals(updated.status, JobStatus.Draft)
    assertEquals(updated.updatedAt, updatedAt)
  }

  test("close moves Draft or Open to Closed and rejects already closed jobs") {
    val closedResult = JobLifecycle.close(draftJob.copy(status = JobStatus.Open), updatedAt)
    val closedState = closedResult.toOption.getOrElse(fail("expected job to close"))
    val rejectedResult = JobLifecycle.close(closedState, updatedAt)

    assertEquals(closedResult.map(_.status), Right(JobStatus.Closed))
    assertEquals(closedState.status, JobStatus.Closed)
    assertEquals(closedState.closedAt, Some(updatedAt))
    assertEquals(rejectedResult, Left(DomainError.InvalidJobTransition(JobStatus.Closed, JobStatus.Closed)))
  }
}
