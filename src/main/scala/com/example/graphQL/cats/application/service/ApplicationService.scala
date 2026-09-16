package com.example.graphQL.cats.application.service

import cats.Monad
import cats.syntax.all.*
import com.example.graphQL.cats.application.port.{ApplicationPageRequest, ApplicationRepository, JobRepository, UserRepository}
import com.example.graphQL.cats.application.{ActorContext, AuthenticationError, UseCaseError}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, Job, User, UserRole}
import com.example.graphQL.cats.domain.service.{ApplicationLifecycle, ApplicationSubmission}
import java.time.Instant

final class ApplicationService[F[_]: Monad](
    users: UserRepository[F],
    jobs: JobRepository[F],
    applications: ApplicationRepository[F]
) {
  def submitApplication(
      actor: ActorContext,
      jobId: JobId,
      applicationId: ApplicationId,
      eventId: ApplicationEventId,
      now: Instant
  ): F[Either[UseCaseError, Application]] =
    resolveActor(actor).flatMap {
      case Left(error) => error.asLeft[Application].pure[F]
      case Right(user) if user.role != UserRole.Candidate => DomainError.Forbidden.asLeft[Application].pure[F]
      case Right(candidate) =>
        jobs.find(jobId).flatMap {
          case None => DomainError.NotFound("job").asLeft[Application].pure[F]
          case Some(job) =>
            ApplicationSubmission.create(candidate, job, applicationId, now).leftMap(error => error: UseCaseError) match {
              case Left(error) => error.asLeft[Application].pure[F]
              case Right(application) =>
                val initialEvent = ApplicationEvent(eventId, application.id, None, application.status, candidate.id, now, None, None)
                applications.create(application, initialEvent).map(_.leftMap(error => error: UseCaseError).as(application))
            }
        }
    }

  def myApplications(
      actor: ActorContext,
      page: ApplicationPageRequest
  ): F[Either[UseCaseError, List[Application]]] =
    resolveActor(actor).flatMap {
      case Left(error) => error.asLeft[List[Application]].pure[F]
      case Right(user) if user.role == UserRole.Candidate =>
        applications.findByCandidate(user.id, page).map(_.asRight[UseCaseError])
      case Right(_) => DomainError.Forbidden.asLeft[List[Application]].pure[F]
    }

  def jobApplications(
      actor: ActorContext,
      jobId: JobId,
      page: ApplicationPageRequest
  ): F[Either[UseCaseError, List[Application]]] =
    withAuthorizedJob(actor, jobId) { job =>
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
    resolveActor(actor).flatMap {
      case Left(error) => error.asLeft[Application].pure[F]
      case Right(actorUser) =>
        applications.find(applicationId).flatMap {
          case None => DomainError.NotFound("application").asLeft[Application].pure[F]
          case Some(application) =>
            jobs.find(application.jobId).flatMap {
              case None => DomainError.NotFound("job").asLeft[Application].pure[F]
              case Some(job) if canManage(actorUser, job) =>
                ApplicationLifecycle.changeStatus(application, target, actorUser.id, now, feedback, reason).leftMap(error => error: UseCaseError) match {
                  case Left(error) => error.asLeft[Application].pure[F]
                  case Right(change) =>
                    ApplicationEvent
                      .validate(eventId, application.id, Some(change.previousStatus), change.newStatus, actorUser.id, now, change.feedback, change.reason)
                      .toEither
                      .leftMap(errors => errors: UseCaseError)
                      .fold(
                        _.asLeft[Application].pure[F],
                        event => applications.updateStatus(change.application, event).map(_.leftMap(error => error: UseCaseError).as(change.application))
                      )
                }
              case Some(_) => DomainError.Forbidden.asLeft[Application].pure[F]
            }
        }
    }

  private def withAuthorizedJob[A](
      actor: ActorContext,
      jobId: JobId
  )(operation: Job => F[Either[UseCaseError, A]]): F[Either[UseCaseError, A]] =
    resolveActor(actor).flatMap {
      case Left(error) => error.asLeft[A].pure[F]
      case Right(user) =>
        jobs.find(jobId).flatMap {
          case None => DomainError.NotFound("job").asLeft[A].pure[F]
          case Some(job) if canManage(user, job) => operation(job)
          case Some(_) => DomainError.Forbidden.asLeft[A].pure[F]
        }
    }

  private def resolveActor(actor: ActorContext): F[Either[UseCaseError, User]] =
    users.find(actor.userId).map {
      case None => AuthenticationError.Unauthorized.asLeft[User]
      case Some(user) if user.role != actor.role => DomainError.Forbidden.asLeft[User]
      case Some(user) if user.role == UserRole.Admin && !user.adminSingleton =>
        AuthenticationError.SingletonAdminViolation.asLeft[User]
      case Some(user) => user.asRight[UseCaseError]
    }

  private def canManage(user: User, job: Job): Boolean =
    (user.role == UserRole.Admin && user.adminSingleton) || (user.role == UserRole.Recruiter && job.recruiterId == user.id)

}

object ApplicationService {
  def apply[F[_]: Monad](
      users: UserRepository[F],
      jobs: JobRepository[F],
      applications: ApplicationRepository[F]
  ): ApplicationService[F] =
    new ApplicationService(users, jobs, applications)
}
