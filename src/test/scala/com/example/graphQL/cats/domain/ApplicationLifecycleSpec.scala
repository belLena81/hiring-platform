package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Application
import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.model.ApplicationStatus.{Accepted, Created, Declined, Hired, Interview, Rejected}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.policy.ApplicationLifecycle
import java.time.Instant
import java.util.UUID
import munit.FunSuite

class ApplicationLifecycleSpec extends FunSuite {
  private val now = Instant.parse("2026-09-16T10:15:30Z")
  private val later = Instant.parse("2026-09-16T11:15:30Z")
  private val candidateId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000001"))
  private val recruiterId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000002"))
  private val jobId = JobId(UUID.fromString("00000000-0000-0000-0000-000000000003"))
  private val applicationId = ApplicationId(UUID.fromString("00000000-0000-0000-0000-000000000004"))
  private val application = Application.create(applicationId, candidateId, jobId, now)

  test("permitted status changes update the application and keep change metadata") {
    val result = ApplicationLifecycle.changeStatus(Accepted, recruiterId, later, None, None).run(application)

    assertEquals(result.map(_._1.status), Right(Accepted))
    assertEquals(result.map(_._2.previousStatus), Right(Created))
    assertEquals(result.map(_._2.newStatus), Right(Accepted))
    assertEquals(result.map(_._2.actorId), Right(recruiterId))
  }

  test("transition matrix allows exactly the documented lifecycle edges") {
    val statuses = List(Created, Accepted, Declined, Interview, Hired, Rejected)
    val expected = Set(
      Created -> Accepted,
      Created -> Declined,
      Created -> Rejected,
      Accepted -> Interview,
      Interview -> Hired,
      Interview -> Rejected
    )
    val actual = statuses.flatMap { from =>
      statuses.collect {
        case to if ApplicationLifecycle.canTransition(from, to) => from -> to
      }
    }.toSet

    assertEquals(actual, expected)
  }

  test("invalid status transition is a typed domain error") {
    val result = ApplicationLifecycle.changeStatus(Hired, recruiterId, later, None, None).run(application)

    assertEquals(result, Left(DomainError.InvalidStatusTransition(Created, Hired)))
  }

  test("rejection requires feedback and trims accepted feedback") {
    val missing = ApplicationLifecycle.changeStatus(Rejected, recruiterId, later, Some(" "), None).run(application)
    val accepted =
      ApplicationLifecycle
        .changeStatus(Rejected, recruiterId, later, Some(" Not enough Scala "), None)
        .run(application)

    assertEquals(missing, Left(DomainError.RejectionFeedbackRequired))
    assertEquals(accepted.map(_._2.feedback), Right(Some("Not enough Scala")))
  }

  test("decline requires reason and terminal statuses cannot move again") {
    val missing = ApplicationLifecycle.changeStatus(Declined, recruiterId, later, None, Some("")).run(application)
    val declined =
      ApplicationLifecycle
        .changeStatus(Declined, recruiterId, later, None, Some("Candidate withdrew"))
        .run(application)
    val terminal = ApplicationLifecycle
      .changeStatus(Interview, recruiterId, later, None, None)
      .run(application.copy(status = Declined))

    assertEquals(missing, Left(DomainError.DeclineReasonRequired))
    assertEquals(declined.map(_._2.reason), Right(Some("Candidate withdrew")))
    assertEquals(terminal, Left(DomainError.InvalidStatusTransition(Declined, Interview)))
  }

  test("composed status changes thread application state in order") {
    val result = for {
      accepted <- ApplicationLifecycle.changeStatus(Accepted, recruiterId, now, None, None)
      interview <- ApplicationLifecycle.changeStatus(Interview, recruiterId, later, None, None)
    } yield List(accepted, interview)

    assertEquals(
      result.run(application).map { case (updated, changes) => (updated.status, changes.map(_.previousStatus)) },
      Right((Interview, List(Created, Accepted)))
    )
  }

  test("a failed later status change short-circuits without returning partial state") {
    val result = for {
      _ <- ApplicationLifecycle.changeStatus(Accepted, recruiterId, now, None, None)
      _ <- ApplicationLifecycle.changeStatus(Hired, recruiterId, later, None, None)
    } yield ()

    assertEquals(result.run(application), Left(DomainError.InvalidStatusTransition(Accepted, Hired)))
  }
}
