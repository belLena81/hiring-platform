package com.example.graphQL.cats.service.job

import cats.data.EitherT
import cats.effect.{Clock, IO}
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, SearchError, UseCaseError}
import com.example.graphQL.cats.service.port.{
  JobRepository,
  LexicalJobMatcher,
  MutationEntityReference,
  MutationWriteContext,
  RepositoryIO,
  UserRepository,
  Versioned
}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, JobStatus, Location, UserRole}
import com.example.graphQL.cats.service.events.OperationalEvents
import com.example.graphQL.cats.service.events.OperationalEventPayload.JobKind
import com.example.graphQL.cats.domain.policy.{JobLifecycle, LifecycleProgram}
import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import com.example.graphQL.cats.service.read.HiringReadScope
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.mutation.Idempotent
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, JobUseCases, UseCaseIO}
import com.example.graphQL.cats.service.search.EmbeddingWorkPublisher
import com.example.graphQL.cats.domain.pagination.JobPageRequest
import com.example.graphQL.cats.service.search.JobSearchFilter
import com.example.graphQL.cats.service.search.{
  NearbyJob,
  NearbyJobsQuery,
  JobDiscoveryFacets,
  JobFacetQuery,
  JobDiscoveryValidation,
  LexicalMatchLimit
}
import java.time.Instant
import scala.concurrent.duration.*
import com.example.graphQL.cats.shared.StableIds
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
    uuidGen: UUIDGen[IO] = UUIDGen[IO],
    lexicalMatcher: Option[LexicalJobMatcher] = None,
    lexicalFacetMatchLimit: LexicalMatchLimit = LexicalMatchLimit.Default,
    /** Total time budget shared by the lexical match and the recount; absent means each step keeps its own bound. */
    lexicalFacetDeadline: Option[FiniteDuration] = None
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
        _ <- UseCaseIO.ensure(authorization.canManageJobs(user), UseCaseError.Domain(DomainError.Forbidden))
        now <- EitherT.liftF(clock.realTimeInstant)
        jobId <- EitherT.liftF(uuidGen.randomUUID.map(JobId.apply))
        job <- EitherT.fromEither[IO](
          validateNewJob(user.id, input, now, jobId).flatMap(JobLifecycle.create(_).leftMap(UseCaseError.Domain.apply))
        )
        created <- persist(job, JobKind.Created, job.createdAt, user.id)(
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
          edited <- EitherT.fromEither[IO](validateUpdatedJob(observed.value, input, now))
          replacement <- EitherT.fromEither[IO](
            JobLifecycle.update(edited).runS(observed.value).leftMap(UseCaseError.Domain.apply)
          )
          updated <- persistUpdate(observed, replacement, JobKind.Updated, actor.userId, context)
        } yield updated
      }
    }

  override def publishJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId): UseCaseIO[Job] =
    transition("publishJob", JobKind.Updated, request, actor, jobId)(JobLifecycle.publish)

  override def closeJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId): UseCaseIO[Job] =
    transition("closeJob", JobKind.Closed, request, actor, jobId)(JobLifecycle.close)

  def viewJob(actor: ActorContext, jobId: JobId): UseCaseIO[Job] =
    for {
      user <- authorization.resolve(actor)
      job <- UseCaseIO.found(jobs.find(jobId), "job")
      _ <- UseCaseIO.ensure(authorization.canView(user, job), UseCaseError.Domain(DomainError.Forbidden))
    } yield job

  def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest): UseCaseIO[List[Job]] =
    authorization.resolve(actor) *> UseCaseIO.repository(jobs.findOpen(filter, page))

  def nearbyJobs(actor: ActorContext, query: NearbyJobsQuery, limit: Int): UseCaseIO[List[NearbyJob]] =
    for {
      normalized <- EitherT.fromEither[IO](
        JobDiscoveryValidation
          .nearby(query)
          .toEither
          .leftMap(errors => UseCaseError.Search(SearchError.accumulated(errors)))
      )
      _ <- UseCaseIO.ensure(
        limit >= 1 && limit <= com.example.graphQL.cats.domain.pagination.PageSize.Max,
        UseCaseError.Search(SearchError.InvalidFilter("pageSize"))
      )
      scope <- discoveryScope(actor)
      results <- UseCaseIO.repository(jobs.nearbyJobs(scope, normalized, limit + 1))
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
      results <- normalized.text.fold(UseCaseIO.repository(jobs.jobDiscoveryFacets(scope, normalized, None, None)))(
        lexicalFacets(scope, normalized, _)
      )
    } yield results

  /** Step 1 reads index evidence after authorization; step 2 recounts from operational truth restricted to those IDs.
    * Both steps share one time budget: step 1's elapsed monotonic time is deducted and step 2 receives what remains.
    */
  private def lexicalFacets(scope: HiringReadScope, query: JobFacetQuery, text: String): UseCaseIO[JobDiscoveryFacets] =
    for {
      matcher <- EitherT.fromOption[IO](lexicalMatcher, facetsUnavailable)
      started <- EitherT.liftF[IO, UseCaseError, FiniteDuration](clock.monotonic)
      matched <- UseCaseIO
        .repository(matcher.matches(text, query.filter, lexicalFacetMatchLimit.value))
        .leftMap(_ => facetsUnavailable)
      finished <- EitherT.liftF[IO, UseCaseError, FiniteDuration](clock.monotonic)
      remaining = lexicalFacetDeadline.map(_ - (finished - started))
      counted <-
        if (matched.ids.isEmpty) EitherT.rightT[IO, UseCaseError](JobDiscoveryFacets.fromJobs(Nil))
        else if (remaining.exists(_ < MinimumRecountBudget)) EitherT.leftT[IO, JobDiscoveryFacets](facetsUnavailable)
        else
          UseCaseIO.repository(jobs.jobDiscoveryFacets(scope, query, Some(matched.ids), remaining)).leftMap {
            case UseCaseError.Repository(_) => facetsUnavailable
            case other                      => other
          }
    } yield counted.copy(coverage = matched.coverage)

  // MongoDB treats maxTimeMS 0 as unbounded, so a sub-millisecond remainder counts as exhausted.
  private val MinimumRecountBudget: FiniteDuration = 1.milli

  private val facetsUnavailable: UseCaseError = UseCaseError.Search(SearchError.FacetsUnavailable)

  /** Candidates and the Admin discover jobs; the read scope comes from the persisted actor. */
  private def discoveryScope(actor: ActorContext): UseCaseIO[HiringReadScope] =
    authorization.readScope(actor).flatMap { (user, scope) =>
      UseCaseIO
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
          case UserRole.Admin     => UseCaseIO.repository(jobs.findAll(page))
          case UserRole.Recruiter => UseCaseIO.repository(jobs.findByRecruiter(user.id, page))
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
      kind: JobKind,
      request: IdempotencyRequest,
      actor: ActorContext,
      jobId: JobId
  )(program: Instant => LifecycleProgram[Job, Unit]): UseCaseIO[Job] =
    idempotent.executeFor(actor, operation, request, jobReference, replayJob(actor)) { context =>
      authorizedJobs.manageVersioned(actor, jobId) { observed =>
        for {
          now <- EitherT.liftF(clock.realTimeInstant)
          changed <- EitherT.fromEither[IO](program(now).runS(observed.value).leftMap(UseCaseError.Domain.apply))
          saved <- persistUpdate(observed, changed, kind, actor.userId, context)
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
  ): Either[UseCaseError, Job] =
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

  private def persistUpdate(
      expected: Versioned[Job],
      job: Job,
      kind: JobKind,
      actorId: UserId,
      context: MutationWriteContext
  ): UseCaseIO[Job] =
    persist(job, kind, job.updatedAt, actorId)(
      jobs.updateWithEvents(expected, job, job.updatedAt, _, context).map(_.value)
    )

  /** Builds the job's operational event, then runs `write` with it and wakes the embedding worker after commit. */
  private def persist(job: Job, kind: JobKind, occurredAt: Instant, actorId: UserId)(
      write: List[OperationalEventEnvelope] => RepositoryIO[Job]
  ): UseCaseIO[Job] =
    EitherT
      .fromEither[IO](
        OperationalEvents
          .jobEvent(kind, eventId(job, kind, occurredAt), job, actorId, occurredAt)
          .leftMap(_ => UseCaseError.Repository(RepositoryError.InvalidEvent))
      )
      .flatMap(event => notifyAfterCommit(UseCaseIO.repository(write(List(event)))))

  private def notifyAfterCommit(result: UseCaseIO[Job]): UseCaseIO[Job] =
    result.semiflatTap(_ => embeddingWork.wakeAfterCommit(diagnostics))

  private def eventId(job: Job, kind: JobKind, occurredAt: Instant): UUID =
    StableIds.uuid(s"job:${job.id.value}:${kind.eventType}:$occurredAt")
}
