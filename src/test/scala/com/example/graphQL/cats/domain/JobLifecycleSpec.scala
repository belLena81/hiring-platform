package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location}
import com.example.graphQL.cats.domain.service.JobLifecycle
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

  test("publish moves Draft to Open through State and updates aggregate state") {
    val (state, result) = JobLifecycle.publish(updatedAt).run(draftJob).value

    assertEquals(result.map(_.status), Right(JobStatus.Open))
    assertEquals(state.status, JobStatus.Open)
    assertEquals(state.updatedAt, updatedAt)
  }

  test("rejected publish leaves aggregate state unchanged") {
    val closed = draftJob.copy(status = JobStatus.Closed)
    val (state, result) = JobLifecycle.publish(updatedAt).run(closed).value

    assertEquals(result, Left(DomainError.InvalidJobTransition(JobStatus.Closed, JobStatus.Open)))
    assertEquals(state, closed)
  }

  test("update replaces mutable content through State without changing owner or status") {
    val input = JobLifecycle.Update(
      title = "Lead Scala Developer",
      description = "Own backend services",
      requirements = List("Cats Effect", "MongoDB"),
      skills = Set("Scala", "Cats"),
      location = Location("Poland", "Warsaw", remote = true),
      updatedAt = updatedAt
    )
    val (state, result) = JobLifecycle.update(input).run(draftJob).value

    assertEquals(result.map(_.title), Right("Lead Scala Developer"))
    assertEquals(state.recruiterId, recruiterId)
    assertEquals(state.status, JobStatus.Draft)
    assertEquals(state.updatedAt, updatedAt)
  }

  test("close moves Draft or Open to Closed and rejects already closed jobs") {
    val (closedState, closedResult) = JobLifecycle.close(updatedAt).run(draftJob.copy(status = JobStatus.Open)).value
    val (unchangedState, rejectedResult) = JobLifecycle.close(updatedAt).run(closedState).value

    assertEquals(closedResult.map(_.status), Right(JobStatus.Closed))
    assertEquals(closedState.status, JobStatus.Closed)
    assertEquals(rejectedResult, Left(DomainError.InvalidJobTransition(JobStatus.Closed, JobStatus.Closed)))
    assertEquals(unchangedState, closedState)
  }
}
