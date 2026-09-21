package com.example.graphQL.cats.service.job

import cats.MonadError
import cats.data.EitherT
import cats.syntax.all.*
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.UseCaseError.*
import com.example.graphQL.cats.repository.protocol.{JobRepository, UserRepository}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location, UserRole}
import com.example.graphQL.cats.shared.events.OperationalEvents
import com.example.graphQL.cats.shared.events.OperationalEventType
import com.example.graphQL.cats.domain.policy.JobLifecycle
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.protocol.JobUseCases
import com.example.graphQL.cats.service.search.EmbeddingWorkPublisher
import com.example.graphQL.cats.shared.pagination.JobPageRequest
import com.example.graphQL.cats.shared.search.JobSearchFilter
import java.time.Instant
import java.nio.charset.StandardCharsets
import java.util.UUID

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

final class JobService[F[_]](
    users: UserRepository[F],
    jobs: JobRepository[F],
    embeddingWork: EmbeddingWorkPublisher[F]
)(using MonadError[F, Throwable]) extends JobUseCases[F] {
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
      _ <- EitherT.cond[F](authorization.canManageJobs(user), (), UseCaseError.Domain(DomainError.Forbidden))
      job <- EitherT.fromEither[F](validateNewJob(user.id, input, now, jobId))
      created <- EitherT(persistCreatedJob(JobLifecycle.create(job).widenUseCase, user.id))
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
        updated <- EitherT(persistUpdatedJob(Right[UseCaseError, Job](JobLifecycle.update(job, update)), actor.userId))
      } yield updated).value
    }

  def publishJob(actor: ActorContext, jobId: JobId, now: Instant): F[Either[UseCaseError, Job]] =
    authorizedJobs.manage(actor, jobId) { job =>
      persistJob(JobLifecycle.publish(job, now).widenUseCase, actor.userId, OperationalEventType.JOB_UPDATED)
    }

  def closeJob(actor: ActorContext, jobId: JobId, now: Instant): F[Either[UseCaseError, Job]] =
    authorizedJobs.manage(actor, jobId) { job =>
      persistJob(JobLifecycle.close(job, now).widenUseCase, actor.userId, OperationalEventType.JOB_CLOSED)
    }

  def viewJob(actor: ActorContext, jobId: JobId): F[Either[UseCaseError, Job]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      job <- EitherT(jobs.find(jobId).map(_.widenUseCase)).subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
      _ <- EitherT.cond[F](authorization.canView(user, job), (), UseCaseError.Domain(DomainError.Forbidden))
    } yield job).value

  def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest): F[Either[UseCaseError, List[Job]]] =
    (for {
      _ <- EitherT(authorization.resolve(actor))
      openJobs <- EitherT(jobs.findOpen(filter, page).map(_.widenUseCase))
    } yield openJobs).value

  def myJobs(actor: ActorContext, page: JobPageRequest): F[Either[UseCaseError, List[Job]]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      manageableJobs <- {
        user.role match {
          case UserRole.Admin => EitherT(jobs.findAll(page).map(_.widenUseCase))
          case UserRole.Recruiter => EitherT(jobs.findByRecruiter(user.id, page).map(_.widenUseCase))
          case UserRole.Candidate => EitherT.leftT[F, List[Job]](UseCaseError.Domain(DomainError.Forbidden))
        }
      }
    } yield manageableJobs).value

  private def validateNewJob(
      recruiterId: UserId,
      input: CreateJobInput,
      now: Instant,
      jobId: JobId
  ): Either[UseCaseError, Job] =
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

  private def persistCreatedJob(result: Either[UseCaseError, Job], actorId: UserId): F[Either[UseCaseError, Job]] =
    result.fold(
      _.asLeft[Job].pure[F],
      job => {
        val event = OperationalEvents.jobEvent(
          OperationalEventType.JOB_CREATED,
          eventId(job, OperationalEventType.JOB_CREATED, job.version),
          job,
          actorId,
          job.createdAt
        )
        notifyAfterCommit(jobs.createWithEvents(job, job.createdAt, List(event)).map(_.widenUseCase.as(job)))
      }
    )

  private def persistUpdatedJob(result: Either[UseCaseError, Job], actorId: UserId): F[Either[UseCaseError, Job]] =
    persistJob(result, actorId, OperationalEventType.JOB_UPDATED)

  private def persistJob(result: Either[UseCaseError, Job], actorId: UserId, eventType: OperationalEventType): F[Either[UseCaseError, Job]] =
    result.fold(
      _.asLeft[Job].pure[F],
      job => {
        val persisted = job.copy(version = job.version + 1L)
        val event = OperationalEvents.jobEvent(eventType, eventId(job, eventType, persisted.version), persisted, actorId, job.updatedAt)
        notifyAfterCommit(jobs.updateWithEvents(job, job.updatedAt, List(event)).map(_.widenUseCase))
      }
    )

  private def notifyAfterCommit(result: F[Either[UseCaseError, Job]]): F[Either[UseCaseError, Job]] =
    result.flatTap(_.fold(_ => MonadError[F, Throwable].unit, _ => embeddingWork.wake.handleError(_ => ())))

  private def eventId(job: Job, eventType: OperationalEventType, version: Long): UUID =
    UUID.nameUUIDFromBytes(s"job:${job.id.value}:$eventType:$version".getBytes(StandardCharsets.UTF_8))
}

object JobService {
  def apply[F[_]](users: UserRepository[F], jobs: JobRepository[F])(using MonadError[F, Throwable]): JobService[F] =
    new JobService(users, jobs, EmbeddingWorkPublisher.noop[F])

  def apply[F[_]](
      users: UserRepository[F],
      jobs: JobRepository[F],
      embeddingWork: EmbeddingWorkPublisher[F]
  )(using MonadError[F, Throwable]): JobService[F] =
    new JobService(users, jobs, embeddingWork)
}
