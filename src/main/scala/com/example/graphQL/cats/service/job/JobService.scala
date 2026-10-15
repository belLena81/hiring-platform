package com.example.graphQL.cats.service.job

import cats.data.EitherT
import cats.MonadThrow
import cats.effect.Clock
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.service.{ActorContext, Diagnostics, SearchError, UseCaseError}
import com.example.graphQL.cats.service.port.{
  JobRepository,
  LexicalJobMatcher,
  MutationEntityReference,
  MutationWriteContext,
  RepositoryF,
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
import com.example.graphQL.cats.service.protocol.{IdempotencyRequest, JobUseCases, UseCaseF}
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

final class JobService[F[_]: MonadThrow](
    users: UserRepository[F],
    jobs: JobRepository[F],
    embeddingWork: EmbeddingWorkPublisher[F],
    idempotent: Idempotent[F],
    diagnostics: Diagnostics[F],
    clock: Clock[F],
    uuidGen: UUIDGen[F],
    lexicalMatcher: Option[LexicalJobMatcher[F]] = None,
    lexicalFacetMatchLimit: LexicalMatchLimit = LexicalMatchLimit.Default,
    /** Total time budget shared by the lexical match and the recount; absent means each step keeps its own bound. */
    lexicalFacetDeadline: Option[FiniteDuration] = None
) extends JobUseCases[F] {
  private val authorization = ActorAuthorization(users)
  private val authorizedJobs = AuthorizedJobAccess(authorization, jobs)

  override def createJob(
      request: IdempotencyRequest,
      actor: ActorContext,
      input: CreateJobInput
  ): UseCaseF[F, Job] =
    idempotent.executeFor(actor, "createJob", request, jobReference, replayJob(actor)) { context =>
      for {
        user <- authorization.resolve(actor)
        _ <- UseCaseF.ensure[F](authorization.canManageJobs(user), UseCaseError.Domain(DomainError.Forbidden))
        now <- EitherT.liftF(clock.realTimeInstant)
        jobId <- EitherT.liftF(uuidGen.randomUUID.map(JobId.apply))
        job <- EitherT.fromEither[F](
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
  ): UseCaseF[F, Job] =
    idempotent.executeFor(actor, "updateJob", request, jobReference, replayJob(actor)) { context =>
      authorizedJobs.manageVersioned(actor, jobId) { observed =>
        for {
          now <- EitherT.liftF(clock.realTimeInstant)
          edited <- EitherT.fromEither[F](validateUpdatedJob(observed.value, input, now))
          replacement <- EitherT.fromEither[F](
            JobLifecycle.update(edited).runS(observed.value).leftMap(UseCaseError.Domain.apply)
          )
          updated <- persistUpdate(observed, replacement, JobKind.Updated, actor.userId, context)
        } yield updated
      }
    }

  override def publishJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId): UseCaseF[F, Job] =
    transition("publishJob", JobKind.Updated, request, actor, jobId)(JobLifecycle.publish)

  override def closeJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId): UseCaseF[F, Job] =
    transition("closeJob", JobKind.Closed, request, actor, jobId)(JobLifecycle.close)

  def viewJob(actor: ActorContext, jobId: JobId): UseCaseF[F, Job] =
    for {
      user <- authorization.resolve(actor)
      job <- UseCaseF.found(jobs.find(jobId), "job")
      _ <- UseCaseF.ensure[F](authorization.canView(user, job), UseCaseError.Domain(DomainError.Forbidden))
    } yield job

  def searchOpenJobs(actor: ActorContext, filter: JobSearchFilter, page: JobPageRequest): UseCaseF[F, List[Job]] =
    authorization.resolve(actor) *> UseCaseF.repository(jobs.findOpen(filter, page))

  def nearbyJobs(actor: ActorContext, query: NearbyJobsQuery, limit: Int): UseCaseF[F, List[NearbyJob]] =
    for {
      normalized <- EitherT.fromEither[F](
        JobDiscoveryValidation
          .nearby(query)
          .toEither
          .leftMap(errors => UseCaseError.Search(SearchError.accumulated(errors)))
      )
      _ <- UseCaseF.ensure[F](
        limit >= 1 && limit <= com.example.graphQL.cats.domain.pagination.PageSize.Max,
        UseCaseError.Search(SearchError.InvalidFilter("pageSize"))
      )
      scope <- discoveryScope(actor)
      results <- UseCaseF.repository(jobs.nearbyJobs(scope, normalized, limit + 1))
    } yield results

  def jobDiscoveryFacets(actor: ActorContext, query: JobFacetQuery): UseCaseF[F, JobDiscoveryFacets] =
    for {
      normalized <- EitherT.fromEither[F](
        JobDiscoveryValidation
          .facets(query)
          .toEither
          .leftMap(errors => UseCaseError.Search(SearchError.accumulated(errors)))
      )
      scope <- discoveryScope(actor)
      results <- normalized.text.fold(UseCaseF.repository(jobs.jobDiscoveryFacets(scope, normalized, None, None)))(
        lexicalFacets(scope, normalized, _)
      )
    } yield results

  /** Step 1 reads index evidence after authorization; step 2 recounts from operational truth restricted to those IDs.
    * Both steps share one time budget: step 1's elapsed monotonic time is deducted and step 2 receives what remains.
    */
  private def lexicalFacets(
      scope: HiringReadScope,
      query: JobFacetQuery,
      text: String
  ): UseCaseF[F, JobDiscoveryFacets] =
    for {
      matcher <- EitherT.fromOption[F](lexicalMatcher, facetsUnavailable)
      started <- EitherT.liftF[F, UseCaseError, FiniteDuration](clock.monotonic)
      matched <- UseCaseF
        .repository(matcher.matches(text, query.filter, lexicalFacetMatchLimit.value))
        .leftMap(_ => facetsUnavailable)
      finished <- EitherT.liftF[F, UseCaseError, FiniteDuration](clock.monotonic)
      remaining = lexicalFacetDeadline.map(_ - (finished - started))
      counted <-
        if (matched.ids.isEmpty) EitherT.rightT[F, UseCaseError](JobDiscoveryFacets.fromJobs(Nil))
        else if (remaining.exists(_ < MinimumRecountBudget)) EitherT.leftT[F, JobDiscoveryFacets](facetsUnavailable)
        else
          UseCaseF.repository(jobs.jobDiscoveryFacets(scope, query, Some(matched.ids), remaining)).leftMap {
            case UseCaseError.Repository(_) => facetsUnavailable
            case other                      => other
          }
    } yield counted.copy(coverage = matched.coverage)

  // MongoDB treats maxTimeMS 0 as unbounded, so a sub-millisecond remainder counts as exhausted.
  private val MinimumRecountBudget: FiniteDuration = 1.milli

  private val facetsUnavailable: UseCaseError = UseCaseError.Search(SearchError.FacetsUnavailable)

  /** Candidates and the Admin discover jobs; the read scope comes from the persisted actor. */
  private def discoveryScope(actor: ActorContext): UseCaseF[F, HiringReadScope] =
    authorization.readScope(actor).flatMap { (user, scope) =>
      UseCaseF
        .ensure(
          user.role == UserRole.Candidate || user.role == UserRole.Admin,
          UseCaseError.Domain(DomainError.Forbidden)
        )
        .as(scope)
    }

  def myJobs(actor: ActorContext, page: JobPageRequest): UseCaseF[F, List[Job]] =
    for {
      user <- authorization.resolve(actor)
      manageableJobs <- {
        user.role match {
          case UserRole.Admin     => UseCaseF.repository(jobs.findAll(page))
          case UserRole.Recruiter => UseCaseF.repository(jobs.findByRecruiter(user.id, page))
          case UserRole.Candidate => EitherT.leftT[F, List[Job]](UseCaseError.Domain(DomainError.Forbidden))
        }
      }
    } yield manageableJobs

  private def replayJob(actor: ActorContext): MutationEntityReference => UseCaseF[F, Job] =
    Idempotent.replayById[F, JobId, Job]("job", JobId.apply)(viewJob(actor, _))

  private def jobReference(job: Job): MutationEntityReference =
    MutationEntityReference.of("job", job.id.value)

  private def transition(
      operation: String,
      kind: JobKind,
      request: IdempotencyRequest,
      actor: ActorContext,
      jobId: JobId
  )(program: Instant => LifecycleProgram[Job, Unit]): UseCaseF[F, Job] =
    idempotent.executeFor(actor, operation, request, jobReference, replayJob(actor)) { context =>
      authorizedJobs.manageVersioned(actor, jobId) { observed =>
        for {
          now <- EitherT.liftF(clock.realTimeInstant)
          changed <- EitherT.fromEither[F](program(now).runS(observed.value).leftMap(UseCaseError.Domain.apply))
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
  ): UseCaseF[F, Job] =
    persist(job, kind, job.updatedAt, actorId)(
      jobs.updateWithEvents(expected, job, job.updatedAt, _, context).map(_.value)
    )

  /** Builds the job's operational event, then runs `write` with it and wakes the embedding worker after commit. */
  private def persist(job: Job, kind: JobKind, occurredAt: Instant, actorId: UserId)(
      write: List[OperationalEventEnvelope] => RepositoryF[F, Job]
  ): UseCaseF[F, Job] =
    EitherT
      .fromEither[F](
        OperationalEvents
          .jobEvent(kind, eventId(job, kind, occurredAt), job, actorId, occurredAt)
          .leftMap(_ => UseCaseError.Repository(RepositoryError.InvalidEvent))
      )
      .flatMap(event => notifyAfterCommit(UseCaseF.repository(write(List(event)))))

  private def notifyAfterCommit(result: UseCaseF[F, Job]): UseCaseF[F, Job] =
    result.semiflatTap(_ => embeddingWork.wakeAfterCommit(diagnostics))

  private def eventId(job: Job, kind: JobKind, occurredAt: Instant): UUID =
    StableIds.uuid(s"job:${job.id.value}:${kind.eventType}:$occurredAt")
}
