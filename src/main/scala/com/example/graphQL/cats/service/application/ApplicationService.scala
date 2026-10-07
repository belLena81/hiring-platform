package com.example.graphQL.cats.service.application

import cats.effect.{Clock, IO}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.service.HiringReadService
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.UseCaseError.*
import com.example.graphQL.cats.service.port.{
  ApplicationRepository,
  JobRepository,
  MutationEntityReference,
  RepositoryError,
  UserRepository
}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{
  ApplicationEventId,
  ApplicationId,
  JobId,
  parse as parseIdentifier
}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, UserRole}
import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import com.example.graphQL.cats.service.events.OperationalEvents
import com.example.graphQL.cats.domain.policy.{ApplicationLifecycle, ApplicationSubmission, StatusChange}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.job.AuthorizedJobAccess
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.{
  ApplicationUseCases,
  IdempotencyRequest,
  UseCaseIO,
  UseCaseIO as UseCase
}
import com.example.graphQL.cats.domain.pagination.ApplicationPageRequest
import java.nio.charset.StandardCharsets
import java.util.UUID

final class ApplicationService(
    users: UserRepository,
    jobs: JobRepository,
    applications: ApplicationRepository,
    idempotent: Idempotent,
    clock: Clock[IO] = Clock[IO],
    uuidGen: UUIDGen[IO] = UUIDGen[IO]
) extends ApplicationUseCases {
  private val authorization = ActorAuthorization(users)
  private val authorizedJobs = AuthorizedJobAccess(authorization, jobs)
  private val readModel = HiringReadService(users, jobs, applications)

  override def submitApplication(
      request: IdempotencyRequest,
      actor: ActorContext,
      jobId: JobId
  ): UseCaseIO[Application] =
    idempotent.execute(
      "submitApplication",
      Idempotent.actorScope(actor),
      request,
      applicationReference,
      replayApplication(actor)
    ) { context =>
      for {
        candidate <- authorization.resolve(actor)
        _ <- UseCase.fromEither(
          Either.cond(candidate.role == UserRole.Candidate, (), UseCaseError.Domain(DomainError.Forbidden))
        )
        job <- UseCase
          .repository(jobs.findSubmissionSnapshot(jobId))
          .subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
        now <- UseCase.liftIO(clock.realTimeInstant)
        applicationId <- UseCase.liftIO(uuidGen.randomUUID.map(uuid => ApplicationId(uuid)))
        eventId <- UseCase.liftIO(uuidGen.randomUUID.map(uuid => ApplicationEventId(uuid)))
        application <- UseCase.fromEither(
          ApplicationSubmission.create(candidate, job, applicationId, now).widenUseCase
        )
        initialEvent = ApplicationEvent(
          eventId,
          application.id,
          None,
          application.status,
          candidate.id,
          now,
          None,
          None
        )
        event = OperationalEvents.applicationCreated(eventId.value, application, candidate.id, now)
        _ <- UseCase.repository(
          applications.createForOpenJobWithEvents(job, application, initialEvent, List(event), context)
        )
      } yield application
    }

  def myApplications(
      actor: ActorContext,
      page: ApplicationPageRequest
  ): UseCaseIO[List[Application]] =
    for {
      user <- authorization.resolve(actor)
      _ <- UseCase.fromEither(
        Either.cond(user.role == UserRole.Candidate, (), UseCaseError.Domain(DomainError.Forbidden))
      )
      persisted <- UseCase
        .repository(users.find(actor.userId))
        .subflatMap(
          _.toRight(UseCaseError.Authentication(com.example.graphQL.cats.service.AuthenticationError.Unauthorized))
        )
      scope <- UseCase.fromEither(
        com.example.graphQL.cats.service.read.HiringReadScope.validated(actor, persisted, authorization)
      )
      applications <- UseCase.repository(this.applications.findByCandidate(scope, page))
    } yield applications

  def jobApplications(
      actor: ActorContext,
      jobId: JobId,
      page: ApplicationPageRequest
  ): UseCaseIO[List[Application]] =
    authorizedJobs.manage(actor, jobId) { job =>
      for {
        user <- UseCase
          .repository(users.find(actor.userId))
          .subflatMap(
            _.toRight(UseCaseError.Authentication(com.example.graphQL.cats.service.AuthenticationError.Unauthorized))
          )
        scope <- UseCase.fromEither(
          com.example.graphQL.cats.service.read.HiringReadScope.validated(actor, user, authorization)
        )
        values <- UseCase.repository(applications.findByJob(scope, job.id, page))
      } yield values
    }

  override def changeStatus(
      request: IdempotencyRequest,
      actor: ActorContext,
      applicationId: ApplicationId,
      target: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String]
  ): UseCaseIO[Application] =
    idempotent.execute(
      statusOperation(target),
      Idempotent.actorScope(actor),
      request,
      applicationReference,
      replayApplication(actor)
    ) { context =>
      for {
        actorUser <- authorization.resolve(actor)
        application <- UseCase
          .repository(applications.find(applicationId))
          .subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("application"))))
        job <- UseCase
          .repository(jobs.find(application.jobId))
          .subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
        _ <- UseCase.fromEither(
          Either.cond(
            authorization.canManage(actorUser, job),
            (),
            UseCaseError.Domain(DomainError.Forbidden)
          )
        )
        now <- UseCase.liftIO(clock.realTimeInstant)
        eventId <- UseCase.liftIO(uuidGen.randomUUID.map(uuid => ApplicationEventId(uuid)))
        (persistedApplication, change) <- UseCase.fromEither(
          ApplicationLifecycle
            .changeStatus(target, actorUser.id, now, feedback, reason)
            .run(application)
            .widenUseCase
        )
        (event, events) <- UseCase.fromEither(statusEvents(persistedApplication, change, eventId))
        _ <- UseCase.repository(applications.updateStatusWithEvents(persistedApplication, event, events, context))
      } yield persistedApplication
    }

  private def replayApplication(
      actor: ActorContext
  )(reference: MutationEntityReference): UseCaseIO[Application] =
    parseIdentifier(reference.entityId)(ApplicationId.apply)
      .fold(
        _ =>
          UseCase
            .left(UseCaseError.Repository(RepositoryError.Unavailable)),
        id =>
          readModel.canViewApplication(actor, id) *> readModel
            .application(id)
            .subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("application"))))
      )

  private def applicationReference(
      application: Application
  ): MutationEntityReference =
    MutationEntityReference("application", application.id.value.toString)

  private def statusOperation(status: ApplicationStatus): String =
    status match {
      case ApplicationStatus.Accepted  => "acceptApplication"
      case ApplicationStatus.Interview => "moveApplicationToInterview"
      case ApplicationStatus.Hired     => "hireApplication"
      case ApplicationStatus.Rejected  => "rejectApplication"
      case ApplicationStatus.Declined  => "declineApplication"
      case ApplicationStatus.Created   => "changeApplicationStatus"
    }

  private def applicationEvents(application: Application, event: ApplicationEvent): List[OperationalEventEnvelope] = {
    val statusChanged = OperationalEvents.statusChanged(event.id.value, application, event)
    if (event.newStatus == ApplicationStatus.Hired)
      List(statusChanged, OperationalEvents.candidateHired(candidateHiredEventId(event), application, event))
    else List(statusChanged)
  }

  private def statusEvents(
      application: Application,
      change: StatusChange,
      eventId: ApplicationEventId
  ): Either[UseCaseError, (ApplicationEvent, List[OperationalEventEnvelope])] =
    ApplicationEvent
      .validate(
        eventId,
        application.id,
        Some(change.previousStatus),
        change.newStatus,
        change.actorId,
        change.occurredAt,
        change.feedback,
        change.reason
      )
      .toEither
      .widenUseCase
      .map(event => event -> applicationEvents(application, event))

  private def candidateHiredEventId(event: ApplicationEvent): UUID =
    UUID.nameUUIDFromBytes(s"candidate-hired:${event.id.value}".getBytes(StandardCharsets.UTF_8))
}

object ApplicationService {
  def live(
      users: UserRepository,
      jobs: JobRepository,
      applications: ApplicationRepository,
      idempotent: Idempotent
  ): ApplicationService =
    new ApplicationService(users, jobs, applications, idempotent)
}
