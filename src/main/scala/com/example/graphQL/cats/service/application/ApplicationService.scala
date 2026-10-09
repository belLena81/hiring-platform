package com.example.graphQL.cats.service.application

import cats.data.EitherT
import cats.effect.{Clock, IO}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.service.HiringReadService
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.port.{
  ApplicationRepository,
  JobRepository,
  MutationEntityReference,
  UserRepository
}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId}
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
    idempotent.executeFor(actor, "submitApplication", request, applicationReference, replayApplication(actor)) {
      context =>
        for {
          candidate <- authorization.resolve(actor)
          _ <- UseCase.ensure(candidate.role == UserRole.Candidate, UseCaseError.Domain(DomainError.Forbidden))
          job <- UseCase.found(jobs.findSubmissionSnapshot(jobId), "job")
          now <- EitherT.liftF(clock.realTimeInstant)
          applicationId <- EitherT.liftF(uuidGen.randomUUID.map(uuid => ApplicationId(uuid)))
          eventId <- EitherT.liftF(uuidGen.randomUUID.map(uuid => ApplicationEventId(uuid)))
          application <- EitherT.fromEither[IO](
            ApplicationSubmission.create(candidate, job, applicationId, now).leftMap(UseCaseError.Domain.apply)
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
      (user, scope) <- authorization.readScope(actor)
      _ <- UseCase.ensure(user.role == UserRole.Candidate, UseCaseError.Domain(DomainError.Forbidden))
      applications <- UseCase.repository(this.applications.findByCandidate(scope, page))
    } yield applications

  def jobApplications(
      actor: ActorContext,
      jobId: JobId,
      page: ApplicationPageRequest
  ): UseCaseIO[List[Application]] =
    authorizedJobs.manage(actor, jobId) { job =>
      authorization
        .readScope(actor)
        .flatMap((_, scope) => UseCase.repository(applications.findByJob(scope, job.id, page)))
    }

  override def changeStatus(
      request: IdempotencyRequest,
      actor: ActorContext,
      applicationId: ApplicationId,
      target: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String]
  ): UseCaseIO[Application] =
    idempotent.executeFor(actor, statusOperation(target), request, applicationReference, replayApplication(actor)) {
      context =>
        for {
          actorUser <- authorization.resolve(actor)
          application <- UseCase.found(applications.find(applicationId), "application")
          job <- UseCase.found(jobs.find(application.jobId), "job")
          _ <- UseCase.ensure(authorization.canManage(actorUser, job), UseCaseError.Domain(DomainError.Forbidden))
          now <- EitherT.liftF(clock.realTimeInstant)
          eventId <- EitherT.liftF(uuidGen.randomUUID.map(uuid => ApplicationEventId(uuid)))
          (persistedApplication, change) <- EitherT.fromEither[IO](
            ApplicationLifecycle
              .changeStatus(target, actorUser.id, now, feedback, reason)
              .run(application)
              .leftMap(UseCaseError.Domain.apply)
          )
          (event, events) <- EitherT.fromEither[IO](statusEvents(persistedApplication, change, eventId))
          _ <- UseCase.repository(applications.updateStatusWithEvents(persistedApplication, event, events, context))
        } yield persistedApplication
    }

  private def replayApplication(actor: ActorContext): MutationEntityReference => UseCaseIO[Application] =
    Idempotent.replayById("application", ApplicationId.apply)(id =>
      readModel.canViewApplication(actor, id) *> UseCase.found(applications.find(id), "application")
    )

  private def applicationReference(application: Application): MutationEntityReference =
    MutationEntityReference.of("application", application.id.value)

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
      .leftMap(UseCaseError.ValidationFailed.apply)
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
