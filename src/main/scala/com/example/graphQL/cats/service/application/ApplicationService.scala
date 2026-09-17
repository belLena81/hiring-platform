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
import com.example.graphQL.cats.domain.policy.{ApplicationLifecycle, ApplicationSubmission}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.job.AuthorizedJobAccess
import com.example.graphQL.cats.service.protocol.ApplicationUseCases
import com.example.graphQL.cats.shared.pagination.ApplicationPageRequest
import java.time.Instant

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
      _ <- EitherT.cond[F](candidate.role == UserRole.Candidate, (), DomainError.Forbidden: UseCaseError)
      job <- EitherT.fromOptionF(jobs.find(jobId), DomainError.NotFound("job"): UseCaseError)
      application <- EitherT.fromEither[F](ApplicationSubmission.create(candidate, job, applicationId, now).widenUseCase)
      initialEvent = ApplicationEvent(eventId, application.id, None, application.status, candidate.id, now, None, None)
      _ <- EitherT(applications.createForOpenJob(job, application, initialEvent).map(_.widenUseCase))
    } yield application).value

  def myApplications(
      actor: ActorContext,
      page: ApplicationPageRequest
  ): F[Either[UseCaseError, List[Application]]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      _ <- EitherT.cond[F](user.role == UserRole.Candidate, (), DomainError.Forbidden: UseCaseError)
      applications <- EitherT.liftF(this.applications.findByCandidate(user.id, page))
    } yield applications).value

  def jobApplications(
      actor: ActorContext,
      jobId: JobId,
      page: ApplicationPageRequest
  ): F[Either[UseCaseError, List[Application]]] =
    authorizedJobs.manage(actor, jobId) { job =>
      applications.findByJob(job.id, page).map(_.asRight[UseCaseError])
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
      application <- EitherT.fromOptionF(applications.find(applicationId), DomainError.NotFound("application"): UseCaseError)
      job <- EitherT.fromOptionF(jobs.find(application.jobId), DomainError.NotFound("job"): UseCaseError)
      _ <- EitherT.cond[F](authorization.canManage(actorUser, job), (), DomainError.Forbidden: UseCaseError)
      change <- EitherT.fromEither[F](
        ApplicationLifecycle.changeStatus(application, target, actorUser.id, now, feedback, reason).widenUseCase
      )
      event <- EitherT.fromEither[F](
        ApplicationEvent
          .validate(eventId, application.id, Some(change.previousStatus), change.newStatus, actorUser.id, now, change.feedback, change.reason)
          .toEither
          .widenUseCase
      )
      _ <- EitherT(applications.updateStatus(change.application, event).map(_.widenUseCase))
    } yield change.application).value
}

object ApplicationService {
  def apply[F[_]: Monad](
      users: UserRepository[F],
      jobs: JobRepository[F],
      applications: ApplicationRepository[F]
  ): ApplicationService[F] =
    new ApplicationService(users, jobs, applications)
}
