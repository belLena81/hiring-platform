package com.example.graphQL.cats.service.application

import cats.effect.IO
import cats.effect.Ref
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.shared.pagination.{ApplicationPageRequest, PageSize}
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.IdempotencyRequest
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.ApplicationEventId
import com.example.graphQL.cats.domain.model.{ApplicationStatus, JobStatus, UserRole}
import com.example.graphQL.cats.shared.events.OperationalEventType
import java.util.UUID
import munit.CatsEffectSuite

class ApplicationServiceSpec extends CatsEffectSuite {
  private val eventId = ApplicationEventId(UUID.fromString("00000000-0000-0000-0000-000000000006"))
  private val page = ApplicationPageRequest(None, None, PageSize.fromInt(10).toOption.get)
  private val requestId = UUID.fromString("00000000-0000-0000-0000-000000000008")

  private def request(operation: String): IdempotencyRequest =
    IdempotencyRequest.fromCanonicalInput(requestId, operation)

  test("submitApplication derives candidate from ActorContext and writes initial history") {
    withServices(Map(jobId -> openJob), Map.empty, now, List(applicationId.value, eventId.value)).flatMap {
      case (_, applications, service) =>
        for {
          result <- service
            .submitApplication(request("submit"), ActorContext(candidateId, UserRole.Candidate), jobId)
            .value
          events <- applications.allEvents
        } yield {
          assertEquals(result.map(_.candidateId), Right(candidateId))
          assertEquals(events.map(_.previousStatus), Vector(None))
          assertEquals(events.map(_.newStatus), Vector(ApplicationStatus.Created))
        }
    }
  }

  test("closed jobs reject submission without writing application history") {
    withServices(
      Map(jobId -> openJob.copy(status = JobStatus.Closed)),
      Map.empty,
      now,
      List(applicationId.value, eventId.value)
    ).flatMap { case (_, applications, service) =>
      for {
        result <- service
          .submitApplication(request("submit-closed"), ActorContext(candidateId, UserRole.Candidate), jobId)
          .value
        events <- applications.allEvents
      } yield {
        assertEquals(result, Left(UseCaseError.Domain(DomainError.JobMustBeOpen)))
        assertEquals(events, Vector.empty)
      }
    }
  }

  test("submitApplication preserves repository conflict when job closes before transactional write") {
    withServices(Map(jobId -> openJob), Map.empty, now, List(applicationId.value, eventId.value)).flatMap {
      case (_, applications, service) =>
        for {
          _ <- applications.rejectNextCreateWith(RepositoryError.Conflict)
          result <- service
            .submitApplication(request("submit-conflict"), ActorContext(candidateId, UserRole.Candidate), jobId)
            .value
          events <- applications.allEvents
        } yield {
          assertEquals(result, Left(UseCaseError.Repository(RepositoryError.Conflict)))
          assertEquals(events, Vector.empty)
        }
    }
  }

  test("owned recruiter status change updates application and appends immutable history") {
    withServices(Map(jobId -> openJob), Map(applicationId -> createdApplication), later, List(eventId.value)).flatMap {
      case (_, applications, service) =>
        for {
          result <- service
            .changeStatus(
              request("accept"),
              ActorContext(recruiterId, UserRole.Recruiter),
              applicationId,
              ApplicationStatus.Accepted,
              None,
              None
            )
            .value
          events <- applications.allEvents
        } yield {
          assertEquals(result.map(_.status), Right(ApplicationStatus.Accepted))
          assertEquals(events.map(_.previousStatus), Vector(Some(ApplicationStatus.Created)))
          assertEquals(events.map(_.newStatus), Vector(ApplicationStatus.Accepted))
        }
    }
  }

  test("hireApplication emits status-change before candidate-hired with distinct event IDs") {
    val interviewApplication = createdApplication.copy(status = ApplicationStatus.Interview)
    withServices(Map(jobId -> openJob), Map(applicationId -> interviewApplication), later, List(eventId.value))
      .flatMap { case (_, applications, service) =>
        for {
          result <- service
            .changeStatus(
              request("hire"),
              ActorContext(recruiterId, UserRole.Recruiter),
              applicationId,
              ApplicationStatus.Hired,
              None,
              None
            )
            .value
          outbox <- applications.allOperationalEvents
        } yield {
          assertEquals(result.map(_.status), Right(ApplicationStatus.Hired))
          assertEquals(
            outbox.map(_.eventType),
            Vector(OperationalEventType.APPLICATION_STATUS_CHANGED, OperationalEventType.CANDIDATE_HIRED)
          )
          assertEquals(
            outbox.map(_.eventType),
            Vector(OperationalEventType.APPLICATION_STATUS_CHANGED, OperationalEventType.CANDIDATE_HIRED)
          )
          assert(outbox.map(_.eventId).distinct.size == 2)
        }
      }
  }

  test("outbox failure rolls back application submission at the repository boundary") {
    withServices(Map(jobId -> openJob), Map.empty, now, List(applicationId.value, eventId.value)).flatMap {
      case (_, applications, service) =>
        for {
          _ <- applications.rejectNextOperationalEventWith(RepositoryError.Unavailable)
          result <- service
            .submitApplication(request("submit-outbox-failure"), ActorContext(candidateId, UserRole.Candidate), jobId)
            .value
          events <- applications.allEvents
          outbox <- applications.allOperationalEvents
        } yield {
          assertEquals(result, Left(UseCaseError.Repository(RepositoryError.Unavailable)))
          assertEquals(events, Vector.empty)
          assertEquals(outbox, Vector.empty)
        }
    }
  }

  test("invalid status change returns typed error and does not append history") {
    withServices(Map(jobId -> openJob), Map(applicationId -> createdApplication), later, List(eventId.value)).flatMap {
      case (_, applications, service) =>
        for {
          result <- service
            .changeStatus(
              request("invalid-hire"),
              ActorContext(recruiterId, UserRole.Recruiter),
              applicationId,
              ApplicationStatus.Hired,
              None,
              None
            )
            .value
          events <- applications.allEvents
        } yield {
          assertEquals(
            result,
            Left(
              UseCaseError.Domain(
                DomainError.InvalidStatusTransition(ApplicationStatus.Created, ApplicationStatus.Hired)
              )
            )
          )
          assertEquals(events, Vector.empty)
        }
    }
  }

  test("rejection without feedback returns typed error and does not append history") {
    withServices(Map(jobId -> openJob), Map(applicationId -> createdApplication), later, List(eventId.value)).flatMap {
      case (_, applications, service) =>
        for {
          result <- service
            .changeStatus(
              request("reject-without-feedback"),
              ActorContext(recruiterId, UserRole.Recruiter),
              applicationId,
              ApplicationStatus.Rejected,
              Some(" "),
              None
            )
            .value
          events <- applications.allEvents
        } yield {
          assertEquals(result, Left(UseCaseError.Domain(DomainError.RejectionFeedbackRequired)))
          assertEquals(events, Vector.empty)
        }
    }
  }

  test("myApplications scopes list access to the actor candidate") {
    withServices(Map(jobId -> openJob), Map(applicationId -> createdApplication), now, Nil).flatMap {
      case (_, _, service) =>
        service.myApplications(ActorContext(candidateId, UserRole.Candidate), page).value.map { result =>
          assertEquals(result.map(_.map(_.candidateId)), Right(List(candidateId)))
        }
    }
  }

  private def withServices(
      jobs: Map[com.example.graphQL.cats.domain.model.Identifiers.JobId, com.example.graphQL.cats.domain.model.Job],
      applications: Map[
        com.example.graphQL.cats.domain.model.Identifiers.ApplicationId,
        com.example.graphQL.cats.domain.model.Application
      ],
      currentTime: java.time.Instant,
      randomIds: List[UUID]
  ): IO[(InMemoryJobs, InMemoryApplications, ApplicationService)] =
    for {
      usersRef <- Ref.of[IO, Map[
        com.example.graphQL.cats.domain.model.Identifiers.UserId,
        com.example.graphQL.cats.domain.model.User
      ]](
        Map(candidateId -> candidate, recruiterId -> recruiter, adminId -> admin)
      )
      jobsRef <- Ref.of[IO, Map[
        com.example.graphQL.cats.domain.model.Identifiers.JobId,
        com.example.graphQL.cats.domain.model.Job
      ]](jobs)
      applicationsRef <- Ref.of[IO, Map[
        com.example.graphQL.cats.domain.model.Identifiers.ApplicationId,
        com.example.graphQL.cats.domain.model.Application
      ]](applications)
      eventsRef <- Ref.of[IO, Vector[com.example.graphQL.cats.domain.model.ApplicationEvent]](Vector.empty)
      operationalEvents <- Ref
        .of[IO, Vector[com.example.graphQL.cats.shared.events.OperationalEventEnvelope]](Vector.empty)
      nextCreateError <- Ref.of[IO, Option[com.example.graphQL.cats.repository.protocol.RepositoryError]](None)
      nextOperationalEventError <- Ref
        .of[IO, Option[com.example.graphQL.cats.repository.protocol.RepositoryError]](None)
      idValues <- Ref.of[IO, List[UUID]](randomIds)
      jobRepository = InMemoryJobs(jobsRef)
      applicationRepository = InMemoryApplications(
        applicationsRef,
        eventsRef,
        nextCreateError,
        Some(operationalEvents),
        Some(nextOperationalEventError)
      )
      service = new ApplicationService(
        InMemoryUsers(usersRef),
        jobRepository,
        applicationRepository,
        Idempotent.noop,
        IO.pure(currentTime),
        nextId(idValues)
      )
    } yield (jobRepository, applicationRepository, service)

  private def nextId(ids: Ref[IO, List[UUID]]): IO[UUID] =
    ids
      .modify {
        case head :: tail => (tail, Right(head))
        case Nil          => (Nil, Left(new AssertionError("No deterministic UUID remains")))
      }
      .flatMap(IO.fromEither)
}
