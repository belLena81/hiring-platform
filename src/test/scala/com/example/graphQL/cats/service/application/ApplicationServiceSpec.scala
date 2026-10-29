package com.example.graphQL.cats.service.application

import cats.effect.IO
import cats.effect.Ref
import cats.effect.std.UUIDGen
import com.example.graphQL.cats.FixedTestClock
import com.example.graphQL.cats.service.{ActorContext, HiringReadService, UseCaseError}
import com.example.graphQL.cats.domain.pagination.{ApplicationPageRequest, PageSize}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.ServiceFixtures.*
import com.example.graphQL.cats.service.port.ApplicationWriteActor
import com.example.graphQL.cats.service.protocol.IdempotencyRequest
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, UserId}
import com.example.graphQL.cats.domain.model.{ApplicationStatus, UserRole, JobState}
import com.example.graphQL.cats.service.events.OperationalEventType
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

  test("ARR-02.1 submission hands the store the trusted candidate as its write actor") {
    withServices(Map(jobId -> openJob), Map.empty, now, List(applicationId.value, eventId.value)).flatMap {
      case (_, applications, service) =>
        for {
          _ <- service
            .submitApplication(request("submit-actor"), ActorContext(candidateId, UserRole.Candidate), jobId)
            .value
          actors <- applications.allWriteActors
        } yield assertEquals(actors, Vector(ApplicationWriteActor(candidateId, UserRole.Candidate)))
    }
  }

  List(
    "recruiter" -> ApplicationWriteActor(recruiterId, UserRole.Recruiter),
    "admin" -> ApplicationWriteActor(adminId, UserRole.Admin)
  ).foreach { case (label, writeActor) =>
    test(s"ARR-02.2 status change hands the store the trusted $label as its write actor") {
      withServices(Map(jobId -> openJob), Map(applicationId -> createdApplication), later, List(eventId.value))
        .flatMap { case (_, applications, service) =>
          for {
            result <- service
              .changeStatus(
                request(s"accept-$label"),
                ActorContext(writeActor.id, writeActor.role),
                applicationId,
                ApplicationStatus.Accepted,
                None,
                None
              )
              .value
            actors <- applications.allWriteActors
          } yield {
            assertEquals(result.map(_.status), Right(ApplicationStatus.Accepted))
            assertEquals(actors, Vector(writeActor))
          }
        }
    }
  }

  test("ARR-02.3 a status change refused by the service never reaches the store") {
    withServices(Map(jobId -> openJob), Map(applicationId -> createdApplication), later, List(eventId.value)).flatMap {
      case (_, applications, service) =>
        for {
          result <- service
            .changeStatus(
              request("candidate-accept"),
              ActorContext(candidateId, UserRole.Candidate),
              applicationId,
              ApplicationStatus.Accepted,
              None,
              None
            )
            .value
          actors <- applications.allWriteActors
        } yield {
          assertEquals(result, Left(UseCaseError.Domain(DomainError.Forbidden)))
          assertEquals(actors, Vector.empty)
        }
    }
  }

  test("closed jobs reject submission without writing application history") {
    withServices(
      Map(jobId -> openJob.copy(state = JobState.Closed(java.time.Instant.EPOCH))),
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

  private val otherCandidate = candidate.copy(id = UserId(UUID.fromString("00000000-0000-0000-0000-000000000011")))
  private val otherRecruiter = recruiter.copy(id = UserId(UUID.fromString("00000000-0000-0000-0000-000000000012")))
  private val unknownApplicationId = ApplicationId(UUID.fromString("00000000-0000-0000-0000-0000000000ff"))

  private def decline(service: ApplicationService[IO], actor: ActorContext, id: ApplicationId, reason: Option[String]) =
    service
      .changeStatus(request("decline"), actor, id, ApplicationStatus.Declined, None, reason)
      .value

  test("AC-F4.1 the candidate who owns the application declines it with a reason, recorded under their own identity") {
    withServices(Map(jobId -> openJob), Map(applicationId -> createdApplication), later, List(eventId.value)).flatMap {
      case (_, applications, service) =>
        for {
          result <- decline(
            service,
            ActorContext(candidateId, UserRole.Candidate),
            applicationId,
            Some("Accepted another offer")
          )
          events <- applications.allEvents
          actors <- applications.allWriteActors
        } yield {
          assertEquals(result.map(_.status), Right(ApplicationStatus.Declined))
          assertEquals(
            events.map(e => (e.actorId, e.actorRole, e.newStatus)),
            Vector((candidateId, UserRole.Candidate, ApplicationStatus.Declined))
          )
          assertEquals(events.flatMap(_.reason), Vector("Accepted another offer"))
          assertEquals(actors, Vector(ApplicationWriteActor(candidateId, UserRole.Candidate)))
        }
    }
  }

  test("AC-F4.2 a candidate's decline without a reason is refused and writes nothing") {
    withServices(Map(jobId -> openJob), Map(applicationId -> createdApplication), later, List(eventId.value)).flatMap {
      case (_, applications, service) =>
        for {
          result <- decline(service, ActorContext(candidateId, UserRole.Candidate), applicationId, Some(" "))
          events <- applications.allEvents
        } yield {
          assertEquals(result, Left(UseCaseError.Domain(DomainError.DeclineReasonRequired)))
          assertEquals(events, Vector.empty)
        }
    }
  }

  test("AC-F4.3 another candidate declining gets the answer for an unknown application and changes nothing") {
    withServices(
      Map(jobId -> openJob),
      Map(applicationId -> createdApplication),
      later,
      List(eventId.value),
      List(otherCandidate)
    ).flatMap { case (_, applications, service) =>
      val other = ActorContext(otherCandidate.id, UserRole.Candidate)
      for {
        hidden <- decline(service, other, applicationId, Some("reason"))
        unknown <- decline(service, other, unknownApplicationId, Some("reason"))
        actors <- applications.allWriteActors
        events <- applications.allEvents
      } yield {
        assertEquals(hidden.map(_ => ()), Left(UseCaseError.Domain(DomainError.NotFound("application"))))
        assertEquals(hidden, unknown)
        assertEquals((actors, events), (Vector.empty, Vector.empty))
      }
    }
  }

  test(
    "AC-F4.4 a recruiter who does not own the job gets the answer for an unknown application; the owner and Admin still decline"
  ) {
    withServices(
      Map(jobId -> openJob),
      Map(applicationId -> createdApplication),
      later,
      List(eventId.value),
      List(otherRecruiter)
    ).flatMap { case (_, applications, service) =>
      for {
        hidden <- decline(service, ActorContext(otherRecruiter.id, UserRole.Recruiter), applicationId, Some("reason"))
        unknown <- decline(
          service,
          ActorContext(otherRecruiter.id, UserRole.Recruiter),
          unknownApplicationId,
          Some("reason")
        )
        unchanged <- applications.allEvents
        owner <- decline(service, ActorContext(recruiterId, UserRole.Recruiter), applicationId, Some("reason"))
      } yield {
        assertEquals(hidden, unknown)
        assertEquals(hidden.map(_ => ()), Left(UseCaseError.Domain(DomainError.NotFound("application"))))
        assertEquals(unchanged, Vector.empty)
        assertEquals(owner.map(_.status), Right(ApplicationStatus.Declined))
      }
    }
  }

  test("AC-F4.5 a candidate can decline only: accepting or rejecting their own application stays forbidden") {
    withServices(
      Map(jobId -> openJob),
      Map(applicationId -> createdApplication),
      later,
      List(eventId.value, eventId.value)
    ).flatMap { case (_, applications, service) =>
      val own = ActorContext(candidateId, UserRole.Candidate)
      for {
        accepted <- service
          .changeStatus(request("a"), own, applicationId, ApplicationStatus.Accepted, None, None)
          .value
        rejected <- service
          .changeStatus(request("r"), own, applicationId, ApplicationStatus.Rejected, Some("feedback"), None)
          .value
        actors <- applications.allWriteActors
      } yield {
        assertEquals(accepted, Left(UseCaseError.Domain(DomainError.Forbidden)))
        assertEquals(rejected, Left(UseCaseError.Domain(DomainError.Forbidden)))
        assertEquals(actors, Vector.empty)
      }
    }
  }

  test("AC-F3.5 submitting to a Draft answers as for an unknown job and writes nothing") {
    withServices(
      Map(jobId -> openJob.copy(state = JobState.Draft)),
      Map.empty,
      now,
      List(applicationId.value, eventId.value)
    ).flatMap { case (_, applications, service) =>
      val actor = ActorContext(candidateId, UserRole.Candidate)
      for {
        draft <- service.submitApplication(request("submit-draft"), actor, jobId).value
        unknown <- service
          .submitApplication(
            request("submit-unknown"),
            actor,
            com.example.graphQL.cats.domain.model.Identifiers
              .JobId(UUID.fromString("00000000-0000-0000-0000-0000000000fe"))
          )
          .value
        events <- applications.allEvents
      } yield {
        assertEquals(draft, Left(UseCaseError.Domain(DomainError.NotFound("job"))))
        assertEquals(draft, unknown)
        assertEquals(events, Vector.empty)
      }
    }
  }

  test("AC-F1.4 viewing an application of another candidate answers as for an unknown application") {
    for {
      users <- Ref.of[IO, Map[UserId, com.example.graphQL.cats.domain.model.User]](
        List(candidate, recruiter, otherCandidate, otherRecruiter).map(user => user.id -> user).toMap
      )
      jobs <- Ref.of[IO, Map[
        com.example.graphQL.cats.domain.model.Identifiers.JobId,
        com.example.graphQL.cats.domain.model.Job
      ]](
        Map(jobId -> openJob)
      )
      stored <- Ref.of[IO, Map[ApplicationId, com.example.graphQL.cats.domain.model.Application]](
        Map(applicationId -> createdApplication)
      )
      events <- Ref.of[IO, Vector[com.example.graphQL.cats.domain.model.ApplicationEvent]](Vector.empty)
      nextCreate <- Ref.of[IO, Option[RepositoryError]](None)
      read = HiringReadService(
        InMemoryUsers(users),
        InMemoryJobs(jobs),
        InMemoryApplications(stored, events, nextCreate)
      )
      view = (user: com.example.graphQL.cats.domain.model.User, id: ApplicationId) =>
        read.canViewApplication(ActorContext(user.id, user.role), id).value.map(_.map(_.id))
      otherHidden <- view(otherCandidate, applicationId)
      otherUnknown <- view(otherCandidate, unknownApplicationId)
      recruiterHidden <- view(otherRecruiter, applicationId)
      recruiterUnknown <- view(otherRecruiter, unknownApplicationId)
      owner <- view(candidate, applicationId)
      managing <- view(recruiter, applicationId)
    } yield {
      assertEquals(otherHidden, Left(UseCaseError.Domain(DomainError.NotFound("application"))))
      assertEquals(otherHidden, otherUnknown)
      assertEquals(recruiterHidden, recruiterUnknown)
      assertEquals((owner, managing), (Right(applicationId), Right(applicationId)))
    }
  }

  private def withServices(
      jobs: Map[com.example.graphQL.cats.domain.model.Identifiers.JobId, com.example.graphQL.cats.domain.model.Job],
      applications: Map[
        com.example.graphQL.cats.domain.model.Identifiers.ApplicationId,
        com.example.graphQL.cats.domain.model.Application
      ],
      currentTime: java.time.Instant,
      randomIds: List[UUID],
      extraUsers: List[com.example.graphQL.cats.domain.model.User] = Nil
  ): IO[(InMemoryJobs, InMemoryApplications, ApplicationService[IO])] =
    for {
      usersRef <- Ref.of[IO, Map[
        com.example.graphQL.cats.domain.model.Identifiers.UserId,
        com.example.graphQL.cats.domain.model.User
      ]](
        Map(candidateId -> candidate, recruiterId -> recruiter, adminId -> admin) ++
          extraUsers.map(user => user.id -> user)
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
        .of[IO, Vector[com.example.graphQL.cats.service.events.OperationalEventEnvelope]](Vector.empty)
      writeActors <- Ref.of[IO, Vector[ApplicationWriteActor]](Vector.empty)
      nextCreateError <- Ref.of[IO, Option[com.example.graphQL.cats.service.RepositoryError]](None)
      nextOperationalEventError <- Ref
        .of[IO, Option[com.example.graphQL.cats.service.RepositoryError]](None)
      idValues <- Ref.of[IO, List[UUID]](randomIds)
      jobRepository = InMemoryJobs(jobsRef)
      applicationRepository = InMemoryApplications(
        applicationsRef,
        eventsRef,
        nextCreateError,
        Some(operationalEvents),
        Some(nextOperationalEventError),
        Some(writeActors)
      )
      userRepository = InMemoryUsers(usersRef)
      service = new ApplicationService(
        userRepository,
        jobRepository,
        applicationRepository,
        HiringReadService(userRepository, jobRepository, applicationRepository),
        com.example.graphQL.cats.service.mutation.TestIdempotency.noop,
        clock = FixedTestClock.at(currentTime),
        uuidGen = new UUIDGen[IO] {
          override def randomUUID: IO[UUID] = nextId(idValues)
        }
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
