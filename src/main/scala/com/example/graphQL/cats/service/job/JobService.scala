package com.example.graphQL.cats.service.job

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.UseCaseError.*
import com.example.graphQL.cats.repository.protocol.{JobRepository, MutationWriteContext, UserRepository}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location, UserRole}
import com.example.graphQL.cats.shared.events.OperationalEvents
import com.example.graphQL.cats.shared.events.OperationalEventType
import com.example.graphQL.cats.domain.policy.JobLifecycle
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, JobUseCases, UseCaseIO, UseCaseIO as UseCase}
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
    embeddingWork: EmbeddingWorkPublisher,
    idempotent: Idempotent = Idempotent.noop,
    currentTime: IO[Instant] = IO.realTimeInstant,
    randomId: IO[UUID] = IO.randomUUID
) extends JobUseCases {
  private val authorization = ActorAuthorization(users)
  private val authorizedJobs = AuthorizedJobAccess(authorization, jobs)

  override def createJob(
      request: IdempotencyRequest,
      actor: ActorContext,
      input: CreateJobInput
  ): UseCaseIO[Job] =
    idempotent.execute("createJob", Idempotent.actorScope(actor), request, jobReference, replayJob(actor)) { context =>
      for {
        user <- authorization.resolve(actor)
        _ <- UseCase.fromEither(
          Either.cond(authorization.canManageJobs(user), (), UseCaseError.Domain(DomainError.Forbidden))
        )
        now <- UseCase.liftIO(currentTime)
        jobId <- UseCase.liftIO(randomId.map(JobId.apply))
        job <- UseCase.fromEither(validateNewJob(user.id, input, now, jobId))
        created <- UseCase.fromIO(persistCreatedJob(JobLifecycle.create(job).widenUseCase, user.id, context))
      } yield created
    }

  override def updateJob(
      request: IdempotencyRequest,
      actor: ActorContext,
      jobId: JobId,
      input: UpdateJobInput
  ): UseCaseIO[Job] =
    idempotent.execute("updateJob", Idempotent.actorScope(actor), request, jobReference, replayJob(actor)) { context =>
      authorizedJobs.manage(actor, jobId) { job =>
        for {
          now <- UseCase.liftIO(currentTime)
          update <- UseCase.fromEither(validateUpdatedJob(job, input, now))
          replacement <- UseCase.fromEither(JobLifecycle.update(update).run(job).map(_._1).widenUseCase)
          updated <- UseCase.fromIO(
            persistUpdatedJob(job, Right(replacement), actor.userId, context)
          )
        } yield updated
      }
    }

  override def publishJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId): UseCaseIO[Job] =
    idempotent.execute("publishJob", Idempotent.actorScope(actor), request, jobReference, replayJob(actor)) { context =>
      authorizedJobs.manage(actor, jobId) { job =>
        UseCase
          .liftIO(currentTime)
          .flatMap(now =>
            UseCase.fromIO(
              persistJob(
                job,
                JobLifecycle.publish(now).run(job).map(_._1).widenUseCase,
                actor.userId,
                OperationalEventType.JOB_UPDATED,
                context
              )
            )
          )
      }
    }

  override def closeJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId): UseCaseIO[Job] =
    idempotent.execute("closeJob", Idempotent.actorScope(actor), request, jobReference, replayJob(actor)) { context =>
      authorizedJobs.manage(actor, jobId) { job =>
        UseCase
          .liftIO(currentTime)
          .flatMap(now =>
            UseCase.fromIO(
              persistJob(
                job,
                JobLifecycle.close(now).run(job).map(_._1).widenUseCase,
                actor.userId,
                OperationalEventType.JOB_CLOSED,
                context
              )
            )
          )
      }
    }

  def viewJob(actor: ActorContext, jobId: JobId): UseCaseIO[Job] =
    for {
      user <- authorization.resolve(actor)
      job <- UseCase
        .repository(jobs.find(jobId))
        .subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("job"))))
      _ <- UseCase.fromEither(
        Either.cond(authorization.canView(user, job), (), UseCaseError.Domain(DomainError.Forbidden))
      )
    } yield job

  def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest): UseCaseIO[List[Job]] =
    authorization.resolve(actor) *> UseCase.repository(jobs.findOpen(filter, page))

  def myJobs(actor: ActorContext, page: JobPageRequest): UseCaseIO[List[Job]] =
    for {
      user <- authorization.resolve(actor)
      manageableJobs <- {
        user.role match {
          case UserRole.Admin     => UseCase.repository(jobs.findAll(page))
          case UserRole.Recruiter => UseCase.repository(jobs.findByRecruiter(user.id, page))
          case UserRole.Candidate => UseCase.left(UseCaseError.Domain(DomainError.Forbidden))
        }
      }
    } yield manageableJobs

  private def replayJob(
      actor: ActorContext
  )(reference: com.example.graphQL.cats.repository.protocol.MutationEntityReference): UseCaseIO[Job] =
    scala.util
      .Try(JobId(UUID.fromString(reference.entityId)))
      .toEither
      .fold(
        _ =>
          UseCase
            .left(UseCaseError.Repository(com.example.graphQL.cats.repository.protocol.RepositoryError.Unavailable)),
        viewJob(actor, _)
      )

  private def jobReference(job: Job): com.example.graphQL.cats.repository.protocol.MutationEntityReference =
    com.example.graphQL.cats.repository.protocol.MutationEntityReference("job", job.id.value.toString)

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

  private def validateUpdatedJob(
      job: Job,
      input: UpdateJobInput,
      now: Instant
  ): Either[UseCaseError, JobLifecycle.Update] =
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

  private def persistCreatedJob(
      result: Either[UseCaseError, Job],
      actorId: UserId,
      context: MutationWriteContext
  ): IO[Either[UseCaseError, Job]] =
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
        notifyAfterCommit(jobs.createWithEvents(job, job.createdAt, List(event), context).map(_.widenUseCase.as(job)))
      }
    )

  private def persistUpdatedJob(
      expected: Job,
      result: Either[UseCaseError, Job],
      actorId: UserId,
      context: MutationWriteContext
  ): IO[Either[UseCaseError, Job]] =
    persistJob(expected, result, actorId, OperationalEventType.JOB_UPDATED, context)

  private def persistJob(
      expected: Job,
      result: Either[UseCaseError, Job],
      actorId: UserId,
      eventType: OperationalEventType,
      context: MutationWriteContext
  ): IO[Either[UseCaseError, Job]] =
    result.fold(
      error => IO.pure(error.asLeft[Job]),
      job => {
        val event =
          OperationalEvents.jobEvent(eventType, eventId(job, eventType, job.updatedAt), job, actorId, job.updatedAt)
        notifyAfterCommit(jobs.updateWithEvents(expected, job, job.updatedAt, List(event), context).map(_.widenUseCase))
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

  def live(
      users: UserRepository,
      jobs: JobRepository,
      embeddingWork: EmbeddingWorkPublisher,
      idempotent: Idempotent
  ): JobService =
    new JobService(users, jobs, embeddingWork, idempotent)
}
