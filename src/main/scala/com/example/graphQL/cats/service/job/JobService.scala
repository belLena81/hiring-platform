package com.example.graphQL.cats.service.job

import cats.effect.{Clock, IO}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, LogEvent, LogFields, UseCaseError}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.service.UseCaseError.*
import com.example.graphQL.cats.service.port.{
  JobRepository,
  MutationEntityReference,
  MutationWriteContext,
  RepositoryError,
  UserRepository,
  Versioned
}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId, parse as parseIdentifier}
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location, UserRole}
import com.example.graphQL.cats.service.events.OperationalEvents
import com.example.graphQL.cats.service.events.OperationalEventType
import com.example.graphQL.cats.domain.policy.JobLifecycle
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, JobUseCases, UseCaseIO, UseCaseIO as UseCase}
import com.example.graphQL.cats.service.search.EmbeddingWorkPublisher
import com.example.graphQL.cats.domain.pagination.JobPageRequest
import com.example.graphQL.cats.service.search.JobSearchFilter
import com.example.graphQL.cats.service.search.{
  NearbyJob,
  NearbyJobsQuery,
  JobDiscoveryFacets,
  JobFacetQuery,
  JobDiscoveryValidation
}
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
    idempotent: Idempotent,
    diagnostics: Diagnostics,
    clock: Clock[IO] = Clock[IO],
    uuidGen: UUIDGen[IO] = UUIDGen[IO]
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
        now <- UseCase.liftIO(clock.realTimeInstant)
        jobId <- UseCase.liftIO(uuidGen.randomUUID.map(JobId.apply))
        job <- UseCase.fromEither(validateNewJob(user.id, input, now, jobId))
        created <- persistCreatedJob(JobLifecycle.create(job).widenUseCase, user.id, context)
      } yield created
    }

  override def updateJob(
      request: IdempotencyRequest,
      actor: ActorContext,
      jobId: JobId,
      input: UpdateJobInput
  ): UseCaseIO[Job] =
    idempotent.execute("updateJob", Idempotent.actorScope(actor), request, jobReference, replayJob(actor)) { context =>
      authorizedJobs.manageVersioned(actor, jobId) { observed =>
        val job = observed.value
        for {
          now <- UseCase.liftIO(clock.realTimeInstant)
          update <- UseCase.fromEither(validateUpdatedJob(job, input, now))
          replacement <- UseCase.fromEither(JobLifecycle.update(update).run(job).map(_._1).widenUseCase)
          updated <- persistUpdatedJob(observed, Right(replacement), actor.userId, context)
        } yield updated
      }
    }

  override def publishJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId): UseCaseIO[Job] =
    idempotent.execute("publishJob", Idempotent.actorScope(actor), request, jobReference, replayJob(actor)) { context =>
      authorizedJobs.manageVersioned(actor, jobId) { observed =>
        val job = observed.value
        UseCase
          .liftIO(clock.realTimeInstant)
          .flatMap(now =>
            persistJob(
              observed,
              JobLifecycle.publish(now).run(job).map(_._1).widenUseCase,
              actor.userId,
              OperationalEventType.JOB_UPDATED,
              context
            )
          )
      }
    }

  override def closeJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId): UseCaseIO[Job] =
    idempotent.execute("closeJob", Idempotent.actorScope(actor), request, jobReference, replayJob(actor)) { context =>
      authorizedJobs.manageVersioned(actor, jobId) { observed =>
        val job = observed.value
        UseCase
          .liftIO(clock.realTimeInstant)
          .flatMap(now =>
            persistJob(
              observed,
              JobLifecycle.close(now).run(job).map(_._1).widenUseCase,
              actor.userId,
              OperationalEventType.JOB_CLOSED,
              context
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

  def nearbyJobs(actor: ActorContext, query: NearbyJobsQuery, limit: Int): UseCaseIO[List[NearbyJob]] =
    for {
      normalized <- UseCase.fromEither(
        JobDiscoveryValidation.nearby(query).toEither.leftMap(errors => UseCaseError.Search(errors.head))
      )
      _ <- UseCase.fromEither(
        Either.cond(
          limit >= 1 && limit <= com.example.graphQL.cats.domain.pagination.PageSize.Max,
          (),
          UseCaseError.Search(com.example.graphQL.cats.service.SearchError.InvalidFilter("pageSize"))
        )
      )
      user <- authorization.resolve(actor)
      _ <- UseCase.fromEither(
        Either.cond(
          user.role == UserRole.Candidate || user.role == UserRole.Admin,
          (),
          UseCaseError.Domain(DomainError.Forbidden)
        )
      )
      results <- UseCase.repository(jobs.nearbyJobs(normalized, limit + 1))
    } yield results

  def jobDiscoveryFacets(actor: ActorContext, query: JobFacetQuery): UseCaseIO[JobDiscoveryFacets] =
    for {
      normalized <- UseCase.fromEither(
        JobDiscoveryValidation.facets(query).toEither.leftMap(errors => UseCaseError.Search(errors.head))
      )
      user <- authorization.resolve(actor)
      _ <- UseCase.fromEither(
        Either.cond(
          user.role == UserRole.Candidate || user.role == UserRole.Admin,
          (),
          UseCaseError.Domain(DomainError.Forbidden)
        )
      )
      results <- UseCase.repository(jobs.jobDiscoveryFacets(normalized))
    } yield results

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
  )(reference: MutationEntityReference): UseCaseIO[Job] =
    parseIdentifier(reference.entityId)(JobId.apply)
      .fold(
        _ =>
          UseCase
            .left(UseCaseError.Repository(RepositoryError.Unavailable)),
        viewJob(actor, _)
      )

  private def jobReference(job: Job): MutationEntityReference =
    MutationEntityReference("job", job.id.value.toString)

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
  ): UseCaseIO[Job] =
    UseCase.fromEither(result).flatMap { job =>
      val event = OperationalEvents.jobEvent(
        OperationalEventType.JOB_CREATED,
        eventId(job, OperationalEventType.JOB_CREATED, job.createdAt),
        job,
        actorId,
        job.createdAt
      )
      notifyAfterCommit(UseCase.repository(jobs.createWithEvents(job, job.createdAt, List(event), context)).as(job))
    }

  private def persistUpdatedJob(
      expected: Versioned[Job],
      result: Either[UseCaseError, Job],
      actorId: UserId,
      context: MutationWriteContext
  ): UseCaseIO[Job] =
    persistJob(expected, result, actorId, OperationalEventType.JOB_UPDATED, context)

  private def persistJob(
      expected: Versioned[Job],
      result: Either[UseCaseError, Job],
      actorId: UserId,
      eventType: OperationalEventType,
      context: MutationWriteContext
  ): UseCaseIO[Job] =
    UseCase.fromEither(result).flatMap { job =>
      val event =
        OperationalEvents.jobEvent(eventType, eventId(job, eventType, job.updatedAt), job, actorId, job.updatedAt)
      notifyAfterCommit(
        UseCase.repository(jobs.updateWithEvents(expected, job, job.updatedAt, List(event), context)).map(_.value)
      )
    }

  private def notifyAfterCommit(result: UseCaseIO[Job]): UseCaseIO[Job] =
    result.semiflatTap(_ =>
      embeddingWork.wake.handleErrorWith(error =>
        diagnostics.emit(LogEvent.EmbeddingWakeFailed, fields = LogFields.failure(error))
      )
    )

  private def eventId(job: Job, eventType: OperationalEventType, occurredAt: Instant): UUID =
    UUID.nameUUIDFromBytes(s"job:${job.id.value}:$eventType:$occurredAt".getBytes(StandardCharsets.UTF_8))
}

object JobService {
  def live(
      users: UserRepository,
      jobs: JobRepository,
      embeddingWork: EmbeddingWorkPublisher,
      idempotent: Idempotent,
      diagnostics: Diagnostics
  ): JobService =
    new JobService(users, jobs, embeddingWork, idempotent, diagnostics)
}
