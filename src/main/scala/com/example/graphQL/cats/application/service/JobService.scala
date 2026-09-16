package com.example.graphQL.cats.application.service

import cats.Monad
import cats.data.EitherT
import cats.syntax.all.*
import com.example.graphQL.cats.application.ActorContext
import com.example.graphQL.cats.application.UseCaseError
import com.example.graphQL.cats.application.UseCaseError.*
import com.example.graphQL.cats.application.port.{JobRepository, UserRepository}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location}
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
  private val authorization = ActorAuthorization(users)

  def createJob(
      actor: ActorContext,
      input: CreateJobInput,
      now: Instant,
      jobId: JobId
  ): F[Either[UseCaseError, Job]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      _ <- EitherT.cond[F](authorization.canManageJobs(user), (), DomainError.Forbidden: UseCaseError)
      job <- EitherT.fromEither[F](validateNewJob(user.id, input, now, jobId))
      created <- EitherT(persistCreatedJob(JobLifecycle.create(job).runA(job).value.widenUseCase))
    } yield created).value

  def updateJob(
      actor: ActorContext,
      jobId: JobId,
      input: UpdateJobInput,
      now: Instant
  ): F[Either[UseCaseError, Job]] =
    withAuthorizedJob(actor, jobId) { job =>
      (for {
        update <- EitherT.fromEither[F](validateUpdatedJob(job, input, now))
        updated <- EitherT(persistJob(JobLifecycle.update(update).runA(job).value.widenUseCase))
      } yield updated).value
    }

  def publishJob(actor: ActorContext, jobId: JobId, now: Instant): F[Either[UseCaseError, Job]] =
    withAuthorizedJob(actor, jobId) { job =>
      persistJob(JobLifecycle.publish(now).runA(job).value.widenUseCase)
    }

  def closeJob(actor: ActorContext, jobId: JobId, now: Instant): F[Either[UseCaseError, Job]] =
    withAuthorizedJob(actor, jobId) { job =>
      persistJob(JobLifecycle.close(now).runA(job).value.widenUseCase)
    }

  def viewJob(actor: ActorContext, jobId: JobId): F[Either[UseCaseError, Job]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      job <- EitherT.fromOptionF(jobs.find(jobId), DomainError.NotFound("job"): UseCaseError)
      _ <- EitherT.cond[F](authorization.canView(user, job), (), DomainError.Forbidden: UseCaseError)
    } yield job).value

  private def withAuthorizedJob(
      actor: ActorContext,
      jobId: JobId
  )(operation: Job => F[Either[UseCaseError, Job]]): F[Either[UseCaseError, Job]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      job <- EitherT.fromOptionF(jobs.find(jobId), DomainError.NotFound("job"): UseCaseError)
      _ <- EitherT.cond[F](authorization.canManage(user, job), (), DomainError.Forbidden: UseCaseError)
      result <- EitherT(operation(job))
    } yield result).value

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
        .widenUseCase
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
        now,
        job.version
      )
      .toEither
      .widenUseCase
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

  private def persistCreatedJob(result: Either[UseCaseError, Job]): F[Either[UseCaseError, Job]] =
    result match {
      case Left(error) => error.asLeft[Job].pure[F]
      case Right(job) => jobs.create(job).map(_.widenUseCase.as(job))
    }

  private def persistJob(result: Either[UseCaseError, Job]): F[Either[UseCaseError, Job]] =
    result match {
      case Left(error) => error.asLeft[Job].pure[F]
      case Right(job) => jobs.update(job).map(_.widenUseCase)
    }
}

object JobService {
  def apply[F[_]: Monad](users: UserRepository[F], jobs: JobRepository[F]): JobService[F] =
    new JobService(users, jobs)
}
