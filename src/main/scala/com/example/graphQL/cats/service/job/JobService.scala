package com.example.graphQL.cats.service.job

import com.example.graphQL.cats.service.events.OperationalEventFailure.*
import cats.data.EitherT
import cats.MonadThrow
import cats.effect.Clock
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.service.{
  ActorContext,
  Diagnostics,
  LogEvent,
  LogField,
  LogFields,
  SearchError,
  UseCaseError
}
import com.example.graphQL.cats.service.Diagnostics.emit
import com.example.graphQL.cats.service.port.{
  JobOwner,
  JobRepository,
  LexicalJobMatcher,
  MutationEntityReference,
  MutationWriteContext,
  RepositoryF,
  UserRepository,
  Versioned
}
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Job, JobState, JobStatus, Location, UserRole}
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
  LexicalMatchLimit,
  OpenJobsPage,
  RelaxationAttempt,
  ZeroResultRecovery
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
    location: Location
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
        _ <- UseCaseF.forbidUnless[F](authorization.canManageJobs(user))
        now <- EitherT.liftF(clock.realTimeInstant)
        jobId <- EitherT.liftF(uuidGen.randomUUID.map(JobId.apply))
        job <- EitherT.fromEither[F](validateNewJob(user.id, input, now, jobId))
        created <- persist(job, None, job.createdAt, user.id)(
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
          updated <- persistUpdate(
            observed,
            replacement,
            visibleEvent(observed.value, JobKind.Updated),
            actor.userId,
            context
          )
        } yield updated
      }
    }

  override def publishJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId): UseCaseF[F, Job] =
    transition("publishJob", _ => Some(JobKind.Created), request, actor, jobId)(JobLifecycle.publish)

  override def closeJob(request: IdempotencyRequest, actor: ActorContext, jobId: JobId): UseCaseF[F, Job] =
    transition("closeJob", visibleEvent(_, JobKind.Closed), request, actor, jobId)(JobLifecycle.close)

  def viewJob(actor: ActorContext, jobId: JobId): UseCaseF[F, Job] =
    for {
      user <- authorization.resolve(actor)
      job <- UseCaseF.found(jobs.find(jobId), "job")
      _ <- UseCaseF
        .ensure[F](authorization.canView(user, JobOwner.of(job)), UseCaseError.Domain(DomainError.NotFound("job")))
    } yield job

  // For display only: under `JobProjection.Summary` the jobs carry a blank description and no requirements (the caller
  // proved the client selected neither); never persist or re-derive anything from them.
  // Authorization and the base search come first, so a refused actor or a failed base search never reaches a relaxation.
  def searchOpenJobs(
      actor: ActorContext,
      filter: JobSearchFilter,
      page: JobPageRequest,
      admitRelaxation: F[Boolean]
  ): UseCaseF[F, OpenJobsPage] =
    for {
      _ <- authorization.resolve(actor)
      base <- UseCaseF.repository(jobs.findOpen(filter, page))
      result <-
        if (base.nonEmpty || page.cursor.isDefined) EitherT.pure[F, UseCaseError](OpenJobsPage(base, Nil))
        else EitherT.liftF[F, UseCaseError, OpenJobsPage](recoverEmptyPage(filter, page, admitRelaxation))
    } yield result

  // Optional work: no refusal or failure here may turn the (empty) base answer into an error.
  private def recoverEmptyPage(
      filter: JobSearchFilter,
      page: JobPageRequest,
      admitRelaxation: F[Boolean]
  ): F[OpenJobsPage] = {
    val empty = OpenJobsPage(Nil, Nil)
    def attempts(remaining: List[RelaxationAttempt]): F[OpenJobsPage] =
      remaining match {
        case Nil          => empty.pure[F]
        case next :: rest =>
          admitRelaxation.attempt.flatMap {
            case Right(true) =>
              jobs.findOpen(next.filter, page).value.attempt.flatMap {
                case Right(Right(found)) if found.nonEmpty => OpenJobsPage(found, List(next.relaxed)).pure[F]
                case Right(Right(_))                       => attempts(rest)
                case Right(Left(error))                    =>
                  reportRelaxationFailure(LogFields.repositoryFailure(error)).as(empty)
                case Left(error) =>
                  reportRelaxationFailure(
                    Map(LogField.ErrorCode -> "RELAXATION_SEARCH_FAILED") ++ LogFields.failure(error)
                  ).as(empty)
              }
            case Right(false) => empty.pure[F]
            case Left(error)  =>
              reportRelaxationFailure(
                Map(LogField.ErrorCode -> "RELAXATION_ADMISSION_FAILED") ++ LogFields.failure(error)
              ).as(empty)
          }
      }
    attempts(ZeroResultRecovery.plan(filter))
  }

  // Only the closed code and the failure class and location are logged: never a filter value, skill, city or actor.
  private def reportRelaxationFailure(fields: Map[LogField, String]): F[Unit] =
    diagnostics.emit(LogEvent.JobSearchRelaxationFailed, fields = fields)

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
      UseCaseF.forbidUnless[F](user.role == UserRole.Candidate || user.role == UserRole.Admin).as(scope)
    }

  // For display only: under `JobProjection.Summary` the jobs carry a blank description and no requirements (the caller
  // proved the client selected neither); never persist or re-derive anything from them.
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
      kind: Job => Option[JobKind],
      request: IdempotencyRequest,
      actor: ActorContext,
      jobId: JobId
  )(program: Instant => LifecycleProgram[Job, Unit]): UseCaseF[F, Job] =
    idempotent.executeFor(actor, operation, request, jobReference, replayJob(actor)) { context =>
      authorizedJobs.manageVersioned(actor, jobId) { observed =>
        for {
          now <- EitherT.liftF(clock.realTimeInstant)
          changed <- EitherT.fromEither[F](program(now).runS(observed.value).leftMap(UseCaseError.Domain.apply))
          saved <- persistUpdate(observed, changed, kind(observed.value), actor.userId, context)
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
        JobState.Draft,
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
        job.state,
        job.createdAt,
        now
      )
      .toEither
      .leftMap(UseCaseError.ValidationFailed.apply)

  private def persistUpdate(
      expected: Versioned[Job],
      job: Job,
      kind: Option[JobKind],
      actorId: UserId,
      context: MutationWriteContext
  ): UseCaseF[F, Job] =
    persist(job, kind, job.updatedAt, actorId)(
      jobs.updateWithEvents(expected, job, job.updatedAt, _, context).map(_.value)
    )

  // `createJob` always creates a Draft, which is private to its owner, so it publishes no event: `JOB_CREATED` is
  // emitted once, with the Open snapshot, by `publishJob`.

  /** Updates and closing are announced only for a job that was published; a Draft emits nothing. */
  private def visibleEvent(before: Job, kind: JobKind): Option[JobKind] =
    Option.when(before.status != JobStatus.Draft)(kind)

  /** Builds the job's operational event (none for a Draft), then runs `write` with it and wakes the embedding worker
    * after commit. The embedding work is enqueued by the write for every state, so a published job is already embedded
    * or queued; the work key is the job, so publishing re-queues the same item and never duplicates it.
    */
  private def persist(job: Job, kind: Option[JobKind], occurredAt: Instant, actorId: UserId)(
      write: List[OperationalEventEnvelope] => RepositoryF[F, Job]
  ): UseCaseF[F, Job] =
    EitherT
      .fromEither[F](
        kind.toList.traverse(k =>
          OperationalEvents
            .jobEvent(k, eventId(job, k, occurredAt), job, actorId, occurredAt)
            .leftMap(_.asUseCaseError)
        )
      )
      .flatMap(events => notifyAfterCommit(UseCaseF.repository(write(events))))

  private def notifyAfterCommit(result: UseCaseF[F, Job]): UseCaseF[F, Job] =
    result.semiflatTap(_ => embeddingWork.wakeAfterCommit(diagnostics))

  private def eventId(job: Job, kind: JobKind, occurredAt: Instant): UUID =
    StableIds.uuid(s"job:${job.id.value}:${kind.eventType}:$occurredAt")
}
