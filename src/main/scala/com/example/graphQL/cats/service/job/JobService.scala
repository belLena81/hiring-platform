package com.example.graphQL.cats.service.job

import cats.Monad
import cats.data.EitherT
import cats.syntax.all.*
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.UseCaseError.*
import com.example.graphQL.cats.repository.protocol.{JobRepository, UserRepository}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location, UserRole}
import com.example.graphQL.cats.domain.policy.JobLifecycle
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.protocol.JobUseCases
import com.example.graphQL.cats.service.search.{EmbeddingWork, EmbeddingWorkPublisher}
import com.example.graphQL.cats.shared.pagination.JobPageRequest
import com.example.graphQL.cats.shared.search.JobSearchFilter
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

final class JobService[F[_]: Monad](
    users: UserRepository[F],
    jobs: JobRepository[F],
    embeddingWork: EmbeddingWorkPublisher[F]
) extends JobUseCases[F] {
  private val authorization = ActorAuthorization(users)
  private val authorizedJobs = AuthorizedJobAccess(authorization, jobs)

  def createJob(
      actor: ActorContext,
      input: CreateJobInput,
      now: Instant,
      jobId: JobId
  ): F[Either[UseCaseError, Job]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      _ <- EitherT.cond[F](authorization.canManageJobs(user), (), UseCaseError.domain(DomainError.Forbidden))
      job <- EitherT.fromEither[F](validateNewJob(user.id, input, now, jobId))
      created <- EitherT(persistCreatedJob(JobLifecycle.create(job).runA(job).value.widenUseCase))
    } yield created).value

  def updateJob(
      actor: ActorContext,
      jobId: JobId,
      input: UpdateJobInput,
      now: Instant
  ): F[Either[UseCaseError, Job]] =
    authorizedJobs.manage(actor, jobId) { job =>
      (for {
        update <- EitherT.fromEither[F](validateUpdatedJob(job, input, now))
        updated <- EitherT(persistUpdatedJob(JobLifecycle.update(update).runA(job).value.widenUseCase))
      } yield updated).value
    }

  def publishJob(actor: ActorContext, jobId: JobId, now: Instant): F[Either[UseCaseError, Job]] =
    authorizedJobs.manage(actor, jobId) { job =>
      persistJob(JobLifecycle.publish(now).runA(job).value.widenUseCase)
    }

  def closeJob(actor: ActorContext, jobId: JobId, now: Instant): F[Either[UseCaseError, Job]] =
    authorizedJobs.manage(actor, jobId) { job =>
      persistJob(JobLifecycle.close(now).runA(job).value.widenUseCase)
    }

  def viewJob(actor: ActorContext, jobId: JobId): F[Either[UseCaseError, Job]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      job <- EitherT.fromOptionF(jobs.find(jobId), UseCaseError.domain(DomainError.NotFound("job")))
      _ <- EitherT.cond[F](authorization.canView(user, job), (), UseCaseError.domain(DomainError.Forbidden))
    } yield job).value

  def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest): F[Either[UseCaseError, List[Job]]] =
    (for {
      _ <- EitherT(authorization.resolve(actor))
      openJobs <- EitherT.liftF(jobs.findOpen(filter, page))
    } yield openJobs).value

  def myJobs(actor: ActorContext, page: JobPageRequest): F[Either[UseCaseError, List[Job]]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      manageableJobs <- {
        user.role match {
          case UserRole.Admin => EitherT.liftF(jobs.findAll(page))
          case UserRole.Recruiter => EitherT.liftF(jobs.findByRecruiter(user.id, page))
          case UserRole.Candidate => EitherT.leftT[F, List[Job]](UseCaseError.domain(DomainError.Forbidden))
        }
      }
    } yield manageableJobs).value

  private def validateNewJob(
      recruiterId: UserId,
      input: CreateJobInput,
      now: Instant,
      jobId: JobId
  ): Either[UseCaseError, Job] =
    if (input.status == JobStatus.Closed) UseCaseError.domain(DomainError.InvalidJobTransition(JobStatus.Closed, JobStatus.Closed)).asLeft
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
        job.closedAt,
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
      case Right(job) =>
        jobs.create(job).map(_.widenUseCase.as(job)).flatTap {
          case Right(created) => embeddingWork.publish(EmbeddingWork.JobChanged(created.id))
          case Left(_) => ().pure[F]
        }
    }

  private def persistUpdatedJob(result: Either[UseCaseError, Job]): F[Either[UseCaseError, Job]] =
    persistJob(result).flatTap {
      case Right(updated) => embeddingWork.publish(EmbeddingWork.JobChanged(updated.id))
      case Left(_) => ().pure[F]
    }

  private def persistJob(result: Either[UseCaseError, Job]): F[Either[UseCaseError, Job]] =
    result match {
      case Left(error) => error.asLeft[Job].pure[F]
      case Right(job) => jobs.update(job).map(_.widenUseCase)
    }
}

object JobService {
  def apply[F[_]: Monad](users: UserRepository[F], jobs: JobRepository[F]): JobService[F] =
    new JobService(users, jobs, EmbeddingWorkPublisher.noop[F])

  def apply[F[_]: Monad](
      users: UserRepository[F],
      jobs: JobRepository[F],
      embeddingWork: EmbeddingWorkPublisher[F]
  ): JobService[F] =
    new JobService(users, jobs, embeddingWork)
}
