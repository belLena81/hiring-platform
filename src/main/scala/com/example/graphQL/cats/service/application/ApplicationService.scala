package com.example.graphQL.cats.service.application

import cats.Monad
import cats.data.EitherT
import cats.syntax.all.*
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.UseCaseError.*
import com.example.graphQL.cats.repository.protocol.{ApplicationRepository, JobRepository, UserRepository}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, UserRole}
import com.example.graphQL.cats.shared.events.OperationalEventEnvelope
import com.example.graphQL.cats.shared.events.OperationalEvents
import com.example.graphQL.cats.domain.policy.{ApplicationLifecycle, ApplicationSubmission}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.job.AuthorizedJobAccess
import com.example.graphQL.cats.service.protocol.ApplicationUseCases
import com.example.graphQL.cats.shared.pagination.ApplicationPageRequest
import java.time.Instant
import java.nio.charset.StandardCharsets
import java.util.UUID

final class ApplicationService[F[_]: Monad](
    users: UserRepository[F],
    jobs: JobRepository[F],
    applications: ApplicationRepository[F]
) extends ApplicationUseCases[F] {
  private val authorization = ActorAuthorization(users)
  private val authorizedJobs = AuthorizedJobAccess(authorization, jobs)

  def submitApplication(
      actor: ActorContext,
      jobId: JobId,
      applicationId: ApplicationId,
      eventId: ApplicationEventId,
      now: Instant
  ): F[Either[UseCaseError, Application]] =
    (for {
      candidate <- EitherT(authorization.resolve(actor))
      _ <- EitherT.cond[F](candidate.role == UserRole.Candidate, (), UseCaseError.Domain(DomainError.Forbidden))
      job <- EitherT(jobs.find(jobId).map(_.widenUseCase)).subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
      application <- EitherT.fromEither[F](ApplicationSubmission.create(candidate, job, applicationId, now).widenUseCase)
      initialEvent = ApplicationEvent(eventId, application.id, None, application.status, candidate.id, now, None, None)
      event = OperationalEvents.applicationCreated(eventId.value, application, candidate.id, now)
      _ <- EitherT(applications.createForOpenJobWithEvents(job, application, initialEvent, List(event)).map(_.widenUseCase))
    } yield application).value

  def myApplications(
      actor: ActorContext,
      page: ApplicationPageRequest
  ): F[Either[UseCaseError, List[Application]]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      _ <- EitherT.cond[F](user.role == UserRole.Candidate, (), UseCaseError.Domain(DomainError.Forbidden))
      applications <- EitherT(this.applications.findByCandidate(user.id, page).map(_.widenUseCase))
    } yield applications).value

  def jobApplications(
      actor: ActorContext,
      jobId: JobId,
      page: ApplicationPageRequest
  ): F[Either[UseCaseError, List[Application]]] =
    authorizedJobs.manage(actor, jobId) { job =>
      applications.findByJob(job.id, page).map(_.widenUseCase)
    }

  def changeStatus(
      actor: ActorContext,
      applicationId: ApplicationId,
      target: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String],
      eventId: ApplicationEventId,
      now: Instant
  ): F[Either[UseCaseError, Application]] =
    (for {
      actorUser <- EitherT(authorization.resolve(actor))
      application <- EitherT(applications.find(applicationId).map(_.widenUseCase)).subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("application"))))
      job <- EitherT(jobs.find(application.jobId).map(_.widenUseCase)).subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
      _ <- EitherT.cond[F](authorization.canManage(actorUser, job), (), UseCaseError.Domain(DomainError.Forbidden))
      change <- EitherT.fromEither[F](
        ApplicationLifecycle.changeStatus(application, target, actorUser.id, now, feedback, reason).widenUseCase
      )
      persistedApplication = change.application.copy(version = application.version + 1L)
      event <- EitherT.fromEither[F](
        ApplicationEvent
          .validate(eventId, application.id, Some(change.previousStatus), change.newStatus, actorUser.id, now, change.feedback, change.reason)
          .toEither
          .widenUseCase
      )
      events = applicationEvents(persistedApplication, event)
      _ <- EitherT(applications.updateStatusWithEvents(persistedApplication, event, events).map(_.widenUseCase))
    } yield persistedApplication).value

  private def applicationEvents(application: Application, event: ApplicationEvent): List[OperationalEventEnvelope] = {
    val statusChanged = OperationalEvents.statusChanged(event.id.value, application, event)
    if (event.newStatus == ApplicationStatus.Hired)
      List(statusChanged, OperationalEvents.candidateHired(candidateHiredEventId(event), application, event))
    else List(statusChanged)
  }

  private def candidateHiredEventId(event: ApplicationEvent): UUID =
    UUID.nameUUIDFromBytes(s"candidate-hired:${event.id.value}".getBytes(StandardCharsets.UTF_8))
}

object ApplicationService {
  def apply[F[_]: Monad](
      users: UserRepository[F],
      jobs: JobRepository[F],
      applications: ApplicationRepository[F]
  ): ApplicationService[F] =
    new ApplicationService(users, jobs, applications)
}
