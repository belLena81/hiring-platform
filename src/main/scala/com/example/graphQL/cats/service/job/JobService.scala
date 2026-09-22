package com.example.graphQL.cats.service.job

import cats.data.EitherT
import cats.effect.IO
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

final class JobService(
    users: UserRepository,
    jobs: JobRepository,
    embeddingWork: EmbeddingWorkPublisher
) extends JobUseCases {
  private val authorization = ActorAuthorization(users)
  private val authorizedJobs = AuthorizedJobAccess(authorization, jobs)

  def createJob(
      actor: ActorContext,
      input: CreateJobInput,
      now: Instant,
      jobId: JobId
  ): IO[Either[UseCaseError, Job]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      _ <- EitherT.cond[IO](authorization.canManageJobs(user), (), UseCaseError.Domain(DomainError.Forbidden))
      job <- EitherT.fromEither[IO](validateNewJob(user.id, input, now, jobId))
      created <- EitherT(persistCreatedJob(JobLifecycle.create(job).widenUseCase, user.id))
    } yield created).value

  def updateJob(
      actor: ActorContext,
      jobId: JobId,
      input: UpdateJobInput,
      now: Instant
  ): IO[Either[UseCaseError, Job]] =
    authorizedJobs.manage(actor, jobId) { job =>
      (for {
        update <- EitherT.fromEither[IO](validateUpdatedJob(job, input, now))
        updated <- EitherT(persistUpdatedJob(Right[UseCaseError, Job](JobLifecycle.update(job, update)), actor.userId))
      } yield updated).value
    }

  def publishJob(actor: ActorContext, jobId: JobId, now: Instant): IO[Either[UseCaseError, Job]] =
    authorizedJobs.manage(actor, jobId) { job =>
      persistJob(JobLifecycle.publish(job, now).widenUseCase, actor.userId, OperationalEventType.JOB_UPDATED)
    }

  def closeJob(actor: ActorContext, jobId: JobId, now: Instant): IO[Either[UseCaseError, Job]] =
    authorizedJobs.manage(actor, jobId) { job =>
      persistJob(JobLifecycle.close(job, now).widenUseCase, actor.userId, OperationalEventType.JOB_CLOSED)
    }

  def viewJob(actor: ActorContext, jobId: JobId): IO[Either[UseCaseError, Job]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      job <- EitherT(jobs.find(jobId).map(_.widenUseCase)).subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
      _ <- EitherT.cond[IO](authorization.canView(user, job), (), UseCaseError.Domain(DomainError.Forbidden))
    } yield job).value

  def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest): IO[Either[UseCaseError, List[Job]]] =
    (for {
      _ <- EitherT(authorization.resolve(actor))
      openJobs <- EitherT(jobs.findOpen(filter, page).map(_.widenUseCase))
    } yield openJobs).value

  def myJobs(actor: ActorContext, page: JobPageRequest): IO[Either[UseCaseError, List[Job]]] =
    (for {
      user <- EitherT(authorization.resolve(actor))
      manageableJobs <- {
        user.role match {
          case UserRole.Admin => EitherT(jobs.findAll(page).map(_.widenUseCase))
          case UserRole.Recruiter => EitherT(jobs.findByRecruiter(user.id, page).map(_.widenUseCase))
          case UserRole.Candidate => EitherT.leftT[IO, List[Job]](UseCaseError.Domain(DomainError.Forbidden))
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
        job.closedAt
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

  private def persistCreatedJob(result: Either[UseCaseError, Job], actorId: UserId): IO[Either[UseCaseError, Job]] =
    result.fold(
      error => IO.pure(error.asLeft[Job]),
      job => {
        val event = OperationalEvents.jobEvent(
          OperationalEventType.JOB_CREATED,
          eventId(job, OperationalEventType.JOB_CREATED, job.createdAt),
          job,
          actorId,
          job.createdAt
        )
        notifyAfterCommit(jobs.createWithEvents(job, job.createdAt, List(event)).map(_.widenUseCase.as(job)))
      }
    )

  private def persistUpdatedJob(result: Either[UseCaseError, Job], actorId: UserId): IO[Either[UseCaseError, Job]] =
    persistJob(result, actorId, OperationalEventType.JOB_UPDATED)

  private def persistJob(result: Either[UseCaseError, Job], actorId: UserId, eventType: OperationalEventType): IO[Either[UseCaseError, Job]] =
    result.fold(
      error => IO.pure(error.asLeft[Job]),
      job => {
        val event = OperationalEvents.jobEvent(eventType, eventId(job, eventType, job.updatedAt), job, actorId, job.updatedAt)
        notifyAfterCommit(jobs.updateWithEvents(job, job.updatedAt, List(event)).map(_.widenUseCase))
      }
    )

  private def notifyAfterCommit(result: IO[Either[UseCaseError, Job]]): IO[Either[UseCaseError, Job]] =
    result.flatTap(_.fold(_ => IO.unit, _ => embeddingWork.wake.handleError(_ => ())))

  private def eventId(job: Job, eventType: OperationalEventType, occurredAt: Instant): UUID =
    UUID.nameUUIDFromBytes(s"job:${job.id.value}:$eventType:$occurredAt".getBytes(StandardCharsets.UTF_8))
}

object JobService {
  def apply(users: UserRepository, jobs: JobRepository): JobService =
    new JobService(users, jobs, EmbeddingWorkPublisher.noop)

  def apply(
      users: UserRepository,
      jobs: JobRepository,
      embeddingWork: EmbeddingWorkPublisher
  ): JobService =
    new JobService(users, jobs, embeddingWork)
}
