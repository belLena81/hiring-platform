package com.example.graphQL.cats.service.application

import cats.data.EitherT
import cats.Monad
import cats.effect.Clock
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.port.{
  ApplicationRepository,
  ApplicationWriteActor,
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
import com.example.graphQL.cats.service.protocol.{ApplicationUseCases, HiringReadModel, IdempotencyRequest, UseCaseF}
import com.example.graphQL.cats.domain.pagination.ApplicationPageRequest
import com.example.graphQL.cats.shared.StableIds
import java.util.UUID

final class ApplicationService[F[_]: Monad](
    users: UserRepository[F],
    jobs: JobRepository[F],
    applications: ApplicationRepository[F],
    readModel: HiringReadModel[F],
    idempotent: Idempotent[F],
    clock: Clock[F],
    uuidGen: UUIDGen[F]
) extends ApplicationUseCases[F] {
  private val authorization = ActorAuthorization(users)
  private val authorizedJobs = AuthorizedJobAccess(authorization, jobs)

  override def submitApplication(
      request: IdempotencyRequest,
      actor: ActorContext,
      jobId: JobId
  ): UseCaseF[F, Application] =
    idempotent.executeFor(actor, "submitApplication", request, applicationReference, replayApplication(actor)) {
      context =>
        for {
          candidate <- authorization.resolve(actor)
          _ <- UseCaseF.ensure[F](candidate.role == UserRole.Candidate, UseCaseError.Domain(DomainError.Forbidden))
          job <- UseCaseF.found(jobs.findSubmissionSnapshot(jobId), "job")
          now <- EitherT.liftF(clock.realTimeInstant)
          applicationId <- EitherT.liftF(uuidGen.randomUUID.map(uuid => ApplicationId(uuid)))
          eventId <- EitherT.liftF(uuidGen.randomUUID.map(uuid => ApplicationEventId(uuid)))
          application <- EitherT.fromEither[F](
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
            None,
            Some(candidate.role)
          )
          event = OperationalEvents.applicationCreated(eventId.value, application, candidate.id, now)
          _ <- UseCaseF.repository(
            applications.createForOpenJobWithEvents(
              job,
              application,
              initialEvent,
              List(event),
              ApplicationWriteActor(candidate.id, candidate.role),
              context
            )
          )
        } yield application
    }

  def myApplications(
      actor: ActorContext,
      page: ApplicationPageRequest
  ): UseCaseF[F, List[Application]] =
    for {
      (user, scope) <- authorization.readScope(actor)
      _ <- UseCaseF.ensure[F](user.role == UserRole.Candidate, UseCaseError.Domain(DomainError.Forbidden))
      applications <- UseCaseF.repository(this.applications.findByCandidate(scope, page))
    } yield applications

  def jobApplications(
      actor: ActorContext,
      jobId: JobId,
      page: ApplicationPageRequest
  ): UseCaseF[F, List[Application]] =
    authorizedJobs.manage(actor, jobId) { job =>
      authorization
        .readScope(actor)
        .flatMap((_, scope) => UseCaseF.repository(applications.findByJob(scope, job.id, page)))
    }

  override def changeStatus(
      request: IdempotencyRequest,
      actor: ActorContext,
      applicationId: ApplicationId,
      target: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String]
  ): UseCaseF[F, Application] =
    idempotent.executeFor(actor, statusOperation(target), request, applicationReference, replayApplication(actor)) {
      context =>
        for {
          actorUser <- authorization.resolve(actor)
          application <- UseCaseF.found(applications.find(applicationId), "application")
          job <- UseCaseF.found(jobs.find(application.jobId), "job")
          _ <- UseCaseF.ensure[F](authorization.canManage(actorUser, job), UseCaseError.Domain(DomainError.Forbidden))
          now <- EitherT.liftF(clock.realTimeInstant)
          eventId <- EitherT.liftF(uuidGen.randomUUID.map(uuid => ApplicationEventId(uuid)))
          (persistedApplication, change) <- EitherT.fromEither[F](
            ApplicationLifecycle
              .changeStatus(target, actorUser.id, Some(actorUser.role), now, feedback, reason)
              .run(application)
              .leftMap(UseCaseError.Domain.apply)
          )
          (event, events) <- EitherT.fromEither[F](statusEvents(persistedApplication, change, eventId))
          _ <- UseCaseF.repository(
            applications.updateStatusWithEvents(
              persistedApplication,
              event,
              events,
              ApplicationWriteActor(actorUser.id, actorUser.role),
              context
            )
          )
        } yield persistedApplication
    }

  private def replayApplication(actor: ActorContext): MutationEntityReference => UseCaseF[F, Application] =
    Idempotent.replayById[F, ApplicationId, Application]("application", ApplicationId.apply)(id =>
      readModel.canViewApplication(actor, id) *> UseCaseF.found(applications.find(id), "application")
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
        change.reason,
        change.actorRole
      )
      .toEither
      .leftMap(UseCaseError.ValidationFailed.apply)
      .map(event => event -> applicationEvents(application, event))

  private def candidateHiredEventId(event: ApplicationEvent): UUID =
    StableIds.uuid(s"candidate-hired:${event.id.value}")
}
