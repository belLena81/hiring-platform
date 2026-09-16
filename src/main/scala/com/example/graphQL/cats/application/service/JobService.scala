package com.example.graphQL.cats.application.service

import cats.Monad
import cats.syntax.all.*
import com.example.graphQL.cats.application.port.{JobRepository, RepositoryError, UserRepository}
import com.example.graphQL.cats.application.{ActorContext, AuthenticationError, UseCaseError}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location, User, UserRole}
import com.example.graphQL.cats.domain.service.JobLifecycle
import java.time.Instant

final case class CreateJobInput(
    title: String,
    description: String,
    requirements: List[String],
    skills: Set[String],
    location: Location,
    status: JobStatus
)

final case class UpdateJobInput(
    title: String,
    description: String,
    requirements: List[String],
    skills: Set[String],
    location: Location
)

final class JobService[F[_]: Monad](users: UserRepository[F], jobs: JobRepository[F]) {
  def createJob(
      actor: ActorContext,
      input: CreateJobInput,
      now: Instant,
      jobId: JobId
  ): F[Either[UseCaseError, Job]] =
    resolveActor(actor).flatMap {
      case Left(error) => error.asLeft[Job].pure[F]
      case Right(user) if canManageJobs(user) =>
        validateNewJob(user.id, input, now, jobId).fold(
          _.asLeft[Job].pure[F],
          job => persistJob(JobLifecycle.create(job).runA(job).value.leftMap(error => error: UseCaseError), jobs.create)
        )
      case Right(_) => DomainError.Forbidden.asLeft[Job].pure[F]
    }

  def updateJob(
      actor: ActorContext,
      jobId: JobId,
      input: UpdateJobInput,
      now: Instant
  ): F[Either[UseCaseError, Job]] =
    withAuthorizedJob(actor, jobId) { job =>
      validateUpdatedJob(job, input, now).fold(
        _.asLeft[Job].pure[F],
        validated => persistJob(JobLifecycle.update(validated).runA(job).value.leftMap(error => error: UseCaseError), jobs.update)
      )
    }

  def publishJob(actor: ActorContext, jobId: JobId, now: Instant): F[Either[UseCaseError, Job]] =
    withAuthorizedJob(actor, jobId) { job =>
      persistJob(JobLifecycle.publish(now).runA(job).value.leftMap(error => error: UseCaseError), jobs.update)
    }

  def closeJob(actor: ActorContext, jobId: JobId, now: Instant): F[Either[UseCaseError, Job]] =
    withAuthorizedJob(actor, jobId) { job =>
      persistJob(JobLifecycle.close(now).runA(job).value.leftMap(error => error: UseCaseError), jobs.update)
    }

  def viewJob(actor: ActorContext, jobId: JobId): F[Either[UseCaseError, Job]] =
    resolveActor(actor).flatMap {
      case Left(error) => error.asLeft[Job].pure[F]
      case Right(user) =>
        jobs.find(jobId).map {
          case None => DomainError.NotFound("job").asLeft[Job]
          case Some(job) if canView(user, job) => job.asRight[UseCaseError]
          case Some(_) => DomainError.Forbidden.asLeft[Job]
        }
    }

  private def withAuthorizedJob(
      actor: ActorContext,
      jobId: JobId
  )(operation: Job => F[Either[UseCaseError, Job]]): F[Either[UseCaseError, Job]] =
    resolveActor(actor).flatMap {
      case Left(error) => error.asLeft[Job].pure[F]
      case Right(user) =>
        jobs.find(jobId).flatMap {
          case None => DomainError.NotFound("job").asLeft[Job].pure[F]
          case Some(job) if canManage(user, job) => operation(job)
          case Some(_) => DomainError.Forbidden.asLeft[Job].pure[F]
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

  private def validateNewJob(
      recruiterId: UserId,
      input: CreateJobInput,
      now: Instant,
      jobId: JobId
  ): Either[UseCaseError, Job] =
    if (input.status == JobStatus.Closed) DomainError.InvalidJobTransition(JobStatus.Closed, JobStatus.Closed).asLeft
    else {
      Job
        .validate(
          jobId,
          recruiterId,
          input.title,
          input.description,
          input.requirements,
          input.skills,
          input.location,
          input.status,
          now,
          now
        )
        .toEither
        .leftMap(errors => errors: UseCaseError)
    }

  private def validateUpdatedJob(job: Job, input: UpdateJobInput, now: Instant): Either[UseCaseError, JobLifecycle.Update] =
    Job
      .validate(
        job.id,
        job.recruiterId,
        input.title,
        input.description,
        input.requirements,
        input.skills,
        input.location,
        job.status,
        job.createdAt,
        now
      )
      .toEither
      .leftMap(errors => errors: UseCaseError)
      .map(validated =>
        JobLifecycle.Update(
          validated.title,
          validated.description,
          validated.requirements,
          validated.skills,
          validated.location,
          validated.updatedAt
        )
      )

  private def persistJob(
      result: Either[UseCaseError, Job],
      persist: Job => F[Either[RepositoryError, Unit]]
  ): F[Either[UseCaseError, Job]] =
    result match {
      case Left(error) => error.asLeft[Job].pure[F]
      case Right(job) => persist(job).map(_.leftMap(error => error: UseCaseError).as(job))
    }

  private def canManageJobs(user: User): Boolean =
    user.role == UserRole.Recruiter || (user.role == UserRole.Admin && user.adminSingleton)

  private def canManage(user: User, job: Job): Boolean =
    (user.role == UserRole.Admin && user.adminSingleton) || (user.role == UserRole.Recruiter && job.recruiterId == user.id)

  private def canView(user: User, job: Job): Boolean =
    job.status == JobStatus.Open || canManage(user, job)
}

object JobService {
  def apply[F[_]: Monad](users: UserRepository[F], jobs: JobRepository[F]): JobService[F] =
    new JobService(users, jobs)
}
