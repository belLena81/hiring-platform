package com.example.graphQL.cats.service.application

import cats.data.EitherT
import cats.effect.IO
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.UseCaseError.*
import com.example.graphQL.cats.repository.protocol.{ApplicationRepository, JobRepository, MutationWriteContext, UserRepository}
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

final class ApplicationService(
    users: UserRepository,
    jobs: JobRepository,
    applications: ApplicationRepository
) extends ApplicationUseCases {
  private val authorization = ActorAuthorization(users)
  private val authorizedJobs = AuthorizedJobAccess(authorization, jobs)

  override def submitApplication(
      actor: ActorContext,
      jobId: JobId,
      applicationId: ApplicationId,
      eventId: ApplicationEventId,
      now: Instant,
      context: MutationWriteContext
  ): IO[Either[UseCaseError, Application]] =
    (for {
      candidate <- EitherT(authorization.resolve(actor))
      _ <- EitherT.cond[IO](candidate.role == UserRole.Candidate, (), UseCaseError.Domain(DomainError.Forbidden))
      job <- EitherT(jobs.find(jobId).map(_.widenUseCase)).subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
      application <- EitherT.fromEither[IO](ApplicationSubmission.create(candidate, job, applicationId, now).widenUseCase)
      initialEvent = ApplicationEvent(eventId, application.id, None, application.status, candidate.id, now, None, None)
      event = OperationalEvents.applicationCreated(eventId.value, application, candidate.id, now)
      _ <- EitherT(applications.createForOpenJobWithEvents(job, application, initialEvent, List(event), context).map(_.widenUseCase))
    } yield application).value

  def myApplications(
      actor: ActorContext,
      page: ApplicationPageRequest
  ): IO[Either[UseCaseError, List[Application]]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      _ <- EitherT.cond[IO](user.role == UserRole.Candidate, (), UseCaseError.Domain(DomainError.Forbidden))
      applications <- EitherT(this.applications.findByCandidate(user.id, page).map(_.widenUseCase))
    } yield applications).value

  def jobApplications(
      actor: ActorContext,
      jobId: JobId,
      page: ApplicationPageRequest
  ): IO[Either[UseCaseError, List[Application]]] =
    authorizedJobs.manage(actor, jobId) { job =>
      applications.findByJob(job.id, page).map(_.widenUseCase)
    }

  override def changeStatus(
      actor: ActorContext,
      applicationId: ApplicationId,
      target: ApplicationStatus,
      feedback: Option[String],
      reason: Option[String],
      eventId: ApplicationEventId,
      now: Instant,
      context: MutationWriteContext
  ): IO[Either[UseCaseError, Application]] =
    (for {
      actorUser <- EitherT(authorization.resolve(actor))
      application <- EitherT(applications.find(applicationId).map(_.widenUseCase)).subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("application"))))
      job <- EitherT(jobs.find(application.jobId).map(_.widenUseCase)).subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
      _ <- EitherT.cond[IO](authorization.canManage(actorUser, job), (), UseCaseError.Domain(DomainError.Forbidden))
      change <- EitherT.fromEither[IO](
        ApplicationLifecycle.changeStatus(application, target, actorUser.id, now, feedback, reason).widenUseCase
      )
      persistedApplication = change.application
      event <- EitherT.fromEither[IO](
        ApplicationEvent
          .validate(eventId, application.id, Some(change.previousStatus), change.newStatus, actorUser.id, now, change.feedback, change.reason)
          .toEither
          .widenUseCase
      )
      events = applicationEvents(persistedApplication, event)
      _ <- EitherT(applications.updateStatusWithEvents(persistedApplication, event, events, context).map(_.widenUseCase))
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
  def apply(
      users: UserRepository,
      jobs: JobRepository,
      applications: ApplicationRepository
  ): ApplicationService =
    new ApplicationService(users, jobs, applications)
}
