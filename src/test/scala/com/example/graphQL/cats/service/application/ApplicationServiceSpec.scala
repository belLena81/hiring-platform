package com.example.graphQL.cats.service.application

import cats.effect.IO
import cats.effect.Ref
import com.example.graphQL.cats.service.ActorContext
import com.example.graphQL.cats.shared.pagination.{ApplicationPageRequest, PageSize}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.ApplicationEventId
import com.example.graphQL.cats.domain.model.{ApplicationStatus, JobStatus, UserRole}
import java.util.UUID
import munit.CatsEffectSuite

class ApplicationServiceSpec extends CatsEffectSuite {
  private val eventId = ApplicationEventId(UUID.fromString("00000000-0000-0000-0000-000000000006"))
  private val page = ApplicationPageRequest(None, None, PageSize.fromInt(10).toOption.get)

  test("submitApplication derives candidate from ActorContext and writes initial history") {
    withServices(Map(jobId -> openJob), Map.empty).flatMap { case (_, applications, service) =>
      for {
        result <- service.submitApplication(ActorContext(candidateId, UserRole.Candidate), jobId, applicationId, eventId, now)
        events <- applications.allEvents
      } yield {
        assertEquals(result.map(_.candidateId), Right(candidateId))
        assertEquals(events.map(_.previousStatus), Vector(None))
        assertEquals(events.map(_.newStatus), Vector(ApplicationStatus.Created))
      }
    }
  }

  test("closed jobs reject submission without writing application history") {
    withServices(Map(jobId -> openJob.copy(status = JobStatus.Closed)), Map.empty).flatMap { case (_, applications, service) =>
      for {
        result <- service.submitApplication(ActorContext(candidateId, UserRole.Candidate), jobId, applicationId, eventId, now)
        events <- applications.allEvents
      } yield {
        assertEquals(result, Left(DomainError.JobMustBeOpen))
        assertEquals(events, Vector.empty)
      }
    }
  }

  test("submitApplication preserves repository conflict when job closes before transactional write") {
    withServices(Map(jobId -> openJob), Map.empty).flatMap { case (_, applications, service) =>
      for {
        _ <- applications.rejectNextCreateWith(RepositoryError.Conflict)
        result <- service.submitApplication(ActorContext(candidateId, UserRole.Candidate), jobId, applicationId, eventId, now)
        events <- applications.allEvents
      } yield {
        assertEquals(result, Left(RepositoryError.Conflict))
        assertEquals(events, Vector.empty)
      }
    }
  }

  test("owned recruiter status change updates application and appends immutable history") {
    withServices(Map(jobId -> openJob), Map(applicationId -> createdApplication)).flatMap { case (_, applications, service) =>
      for {
        result <- service.changeStatus(
          ActorContext(recruiterId, UserRole.Recruiter),
          applicationId,
          ApplicationStatus.Accepted,
          None,
          None,
          eventId,
          later
        )
        events <- applications.allEvents
      } yield {
        assertEquals(result.map(_.status), Right(ApplicationStatus.Accepted))
        assertEquals(events.map(_.previousStatus), Vector(Some(ApplicationStatus.Created)))
        assertEquals(events.map(_.newStatus), Vector(ApplicationStatus.Accepted))
      }
    }
  }

  test("invalid status change returns typed error and does not append history") {
    withServices(Map(jobId -> openJob), Map(applicationId -> createdApplication)).flatMap { case (_, applications, service) =>
      for {
        result <- service.changeStatus(
          ActorContext(recruiterId, UserRole.Recruiter),
          applicationId,
          ApplicationStatus.Hired,
          None,
          None,
          eventId,
          later
        )
        events <- applications.allEvents
      } yield {
        assertEquals(result, Left(DomainError.InvalidStatusTransition(ApplicationStatus.Created, ApplicationStatus.Hired)))
        assertEquals(events, Vector.empty)
      }
    }
  }

  test("rejection without feedback returns typed error and does not append history") {
    withServices(Map(jobId -> openJob), Map(applicationId -> createdApplication)).flatMap { case (_, applications, service) =>
      for {
        result <- service.changeStatus(
          ActorContext(recruiterId, UserRole.Recruiter),
          applicationId,
          ApplicationStatus.Rejected,
          Some(" "),
          None,
          eventId,
          later
        )
        events <- applications.allEvents
      } yield {
        assertEquals(result, Left(DomainError.RejectionFeedbackRequired))
        assertEquals(events, Vector.empty)
      }
    }
  }

  test("myApplications scopes list access to the actor candidate") {
    withServices(Map(jobId -> openJob), Map(applicationId -> createdApplication)).flatMap { case (_, _, service) =>
      service.myApplications(ActorContext(candidateId, UserRole.Candidate), page).map { result =>
        assertEquals(result.map(_.map(_.candidateId)), Right(List(candidateId)))
      }
    }
  }

  private def withServices(
      jobs: Map[com.example.graphQL.cats.domain.model.Identifiers.JobId, com.example.graphQL.cats.domain.model.Job],
      applications: Map[com.example.graphQL.cats.domain.model.Identifiers.ApplicationId, com.example.graphQL.cats.domain.model.Application]
  ): IO[(InMemoryJobs, InMemoryApplications, ApplicationService[IO])] =
    for {
      usersRef <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.UserId, com.example.graphQL.cats.domain.model.User]](
        Map(candidateId -> candidate, recruiterId -> recruiter, adminId -> admin)
      )
      jobsRef <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.JobId, com.example.graphQL.cats.domain.model.Job]](jobs)
      applicationsRef <- Ref.of[IO, Map[com.example.graphQL.cats.domain.model.Identifiers.ApplicationId, com.example.graphQL.cats.domain.model.Application]](applications)
      eventsRef <- Ref.of[IO, Vector[com.example.graphQL.cats.domain.model.ApplicationEvent]](Vector.empty)
      nextCreateError <- Ref.of[IO, Option[com.example.graphQL.cats.service.RepositoryError]](None)
      jobRepository = InMemoryJobs(jobsRef)
      applicationRepository = InMemoryApplications(applicationsRef, eventsRef, nextCreateError)
    } yield (jobRepository, applicationRepository, ApplicationService[IO](InMemoryUsers(usersRef), jobRepository, applicationRepository))
}
