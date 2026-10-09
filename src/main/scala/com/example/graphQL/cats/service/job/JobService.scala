package com.example.graphQL.cats.service.job

import cats.data.EitherT
import cats.effect.{Clock, IO}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, LogEvent, LogFields, SearchError, UseCaseError}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.service.port.{
  JobRepository,
  MutationEntityReference,
  MutationWriteContext,
  RepositoryIO,
  RepositoryError,
  UserRepository,
  Versioned
}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location, UserRole}
import com.example.graphQL.cats.service.events.OperationalEvents
import com.example.graphQL.cats.service.events.OperationalEventType
import com.example.graphQL.cats.domain.policy.{JobLifecycle, LifecycleProgram}
import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import com.example.graphQL.cats.service.read.HiringReadScope
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
    idempotent.executeFor(actor, "createJob", request, jobReference, replayJob(actor)) { context =>
      for {
        user <- authorization.resolve(actor)
        _ <- UseCase.ensure(authorization.canManageJobs(user), UseCaseError.Domain(DomainError.Forbidden))
        now <- EitherT.liftF(clock.realTimeInstant)
        jobId <- EitherT.liftF(uuidGen.randomUUID.map(JobId.apply))
        job <- EitherT.fromEither[IO](
          validateNewJob(user.id, input, now, jobId).flatMap(JobLifecycle.create(_).leftMap(UseCaseError.Domain.apply))
        )
        created <- persist(job, OperationalEventType.JOB_CREATED, job.createdAt, user.id)(
          jobs.createWithEvents(job, job.createdAt, _, context).as(job)
        )
      } yield created
    }

  override def updateJob(
      request: IdempotencyRequest,
      actor: ActorContext,
      jobId: JobId,
      input: UpdateJobInput
  ): UseCaseIO[Job] =
    idempotent.executeFor(actor, "updateJob", request, jobReference, replayJob(actor)) { context =>
      authorizedJobs.manageVersioned(actor, jobId) { observed =>
        for {
          now <- EitherT.liftF(clock.realTimeInstant)
          update <- EitherT.fromEither[IO](validateUpdatedJob(observed.value, input, now))
          replacement <- EitherT.fromEither[IO](
            JobLifecycle.update(update).runS(observed.value).leftMap(UseCaseError.Domain.apply)
          )
          updated <- persistUpdate(observed, replacement, OperationalEventType.JOB_UPDATED, actor.userId, context)
        } yield updated
      }
    }

  override def publishJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId): UseCaseIO[Job] =
    transition("publishJob", OperationalEventType.JOB_UPDATED, request, actor, jobId)(JobLifecycle.publish)

  override def closeJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId): UseCaseIO[Job] =
    transition("closeJob", OperationalEventType.JOB_CLOSED, request, actor, jobId)(JobLifecycle.close)

  def viewJob(actor: ActorContext, jobId: JobId): UseCaseIO[Job] =
    for {
      user <- authorization.resolve(actor)
      job <- UseCase.found(jobs.find(jobId), "job")
      _ <- UseCase.ensure(authorization.canView(user, job), UseCaseError.Domain(DomainError.Forbidden))
    } yield job

  def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest): UseCaseIO[List[Job]] =
    authorization.resolve(actor) *> UseCase.repository(jobs.findOpen(filter, page))

  def nearbyJobs(actor: ActorContext, query: NearbyJobsQuery, limit: Int): UseCaseIO[List[NearbyJob]] =
    for {
      normalized <- EitherT.fromEither[IO](
        JobDiscoveryValidation
          .nearby(query)
          .toEither
          .leftMap(errors => UseCaseError.Search(SearchError.accumulated(errors)))
      )
      _ <- UseCase.ensure(
        limit >= 1 && limit <= com.example.graphQL.cats.domain.pagination.PageSize.Max,
        UseCaseError.Search(SearchError.InvalidFilter("pageSize"))
      )
      scope <- discoveryScope(actor)
      results <- UseCase.repository(jobs.nearbyJobs(scope, normalized, limit + 1))
    } yield results

  def jobDiscoveryFacets(actor: ActorContext, query: JobFacetQuery): UseCaseIO[JobDiscoveryFacets] =
    for {
      normalized <- EitherT.fromEither[IO](
        JobDiscoveryValidation
          .facets(query)
          .toEither
          .leftMap(errors => UseCaseError.Search(SearchError.accumulated(errors)))
      )
      scope <- discoveryScope(actor)
      results <- UseCase.repository(jobs.jobDiscoveryFacets(scope, normalized))
    } yield results

  /** Candidates and the Admin discover jobs; the read scope comes from the persisted actor. */
  private def discoveryScope(actor: ActorContext): UseCaseIO[HiringReadScope] =
    authorization.readScope(actor).flatMap { (user, scope) =>
      UseCase
        .ensure(
          user.role == UserRole.Candidate || user.role == UserRole.Admin,
          UseCaseError.Domain(DomainError.Forbidden)
        )
        .as(scope)
    }

  def myJobs(actor: ActorContext, page: JobPageRequest): UseCaseIO[List[Job]] =
    for {
      user <- authorization.resolve(actor)
      manageableJobs <- {
        user.role match {
          case UserRole.Admin     => UseCase.repository(jobs.findAll(page))
          case UserRole.Recruiter => UseCase.repository(jobs.findByRecruiter(user.id, page))
          case UserRole.Candidate => EitherT.leftT[IO, List[Job]](UseCaseError.Domain(DomainError.Forbidden))
        }
      }
    } yield manageableJobs

  private def replayJob(actor: ActorContext): MutationEntityReference => UseCaseIO[Job] =
    Idempotent.replayById("job", JobId.apply)(viewJob(actor, _))

  private def jobReference(job: Job): MutationEntityReference =
    MutationEntityReference.of("job", job.id.value)

  private def transition(
      operation: String,
      eventType: OperationalEventType,
      request: IdempotencyRequest,
      actor: ActorContext,
      jobId: JobId
  )(program: Instant => LifecycleProgram[Job, Unit]): UseCaseIO[Job] =
    idempotent.executeFor(actor, operation, request, jobReference, replayJob(actor)) { context =>
      authorizedJobs.manageVersioned(actor, jobId) { observed =>
        for {
          now <- EitherT.liftF(clock.realTimeInstant)
          changed <- EitherT.fromEither[IO](program(now).runS(observed.value).leftMap(UseCaseError.Domain.apply))
          saved <- persistUpdate(observed, changed, eventType, actor.userId, context)
        } yield saved
      }
    }

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
      .leftMap(UseCaseError.ValidationFailed.apply)

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
      .leftMap(UseCaseError.ValidationFailed.apply)
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

  private def persistUpdate(
      expected: Versioned[Job],
      job: Job,
      eventType: OperationalEventType,
      actorId: UserId,
      context: MutationWriteContext
  ): UseCaseIO[Job] =
    persist(job, eventType, job.updatedAt, actorId)(
      jobs.updateWithEvents(expected, job, job.updatedAt, _, context).map(_.value)
    )

  /** Builds the job's operational event, then runs `write` with it and wakes the embedding worker after commit. */
  private def persist(job: Job, eventType: OperationalEventType, occurredAt: Instant, actorId: UserId)(
      write: List[OperationalEventEnvelope] => RepositoryIO[Job]
  ): UseCaseIO[Job] =
    EitherT
      .fromEither[IO](
        OperationalEvents
          .jobEvent(eventType, eventId(job, eventType, occurredAt), job, actorId, occurredAt)
          .leftMap(_ => UseCaseError.Repository(RepositoryError.InvalidEvent))
      )
      .flatMap(event => notifyAfterCommit(UseCase.repository(write(List(event)))))

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
