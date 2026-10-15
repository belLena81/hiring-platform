package com.example.graphQL.cats.service

import cats.Applicative
import cats.data.StateT
import cats.effect.Clock
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{
  Application,
  ApplicationEvent,
  EntityEmbedding,
  Job,
  JobSubmissionSnapshot,
  User
}
import com.example.graphQL.cats.domain.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest}
import com.example.graphQL.cats.service.protocol.{HiringReadModel, UseCaseF}
import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.read.*
import com.example.graphQL.cats.service.search.{
  EmbeddingWorkPublisher,
  JobDiscoveryFacets,
  JobFacetQuery,
  JobSearchFilter,
  NearbyJobsQuery
}
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.*

/** A deterministic effect that is not `IO`: state threaded through `Either`, with no suspension, threads or clocks. A
  * service that runs here needs only the capabilities its signature names.
  */
private[cats] object StateEffectFixtures {
  final case class World(
      users: Map[UserId, User] = Map.empty,
      jobs: Map[JobId, Job] = Map.empty,
      applications: Map[ApplicationId, Application] = Map.empty,
      applicationEvents: Vector[ApplicationEvent] = Vector.empty,
      events: Vector[OperationalEventEnvelope] = Vector.empty,
      wakes: Int = 0,
      logged: Vector[LogEvent] = Vector.empty,
      receipts: Map[MutationReceiptKey, (MutationReceiptFingerprint, MutationEntityReference)] = Map.empty,
      uuids: List[UUID] = Nil,
      now: Instant = ServiceFixtures.now
  )

  type Outcome[A] = Either[Throwable, A]
  type Program[A] = StateT[Outcome, World, A]

  given Clock[Program] = new Clock[Program] {
    override val applicative: Applicative[Program] = Applicative[Program]
    override def realTime: Program[FiniteDuration] = StateT.inspect(_.now.toEpochMilli.millis)
    override def monotonic: Program[FiniteDuration] = StateT.pure(Duration.Zero)
  }

  given UUIDGen[Program] = new UUIDGen[Program] {
    override def randomUUID: Program[UUID] =
      StateT(world =>
        Right((world.copy(uuids = world.uuids.drop(1)), world.uuids.headOption.getOrElse(new UUID(0L, 0L))))
      )
  }

  private def unavailable[A]: RepositoryF[Program, A] = RepositoryF.raise[Program, A](RepositoryError.Unavailable)

  private def inspect[A](read: World => A): RepositoryF[Program, A] =
    RepositoryF.lift[Program, A](StateT.inspect[Outcome, World, A](read))

  object Users extends UserRepository[Program] {
    override def relatedUsers(scope: HiringReadScope, keys: List[UserRelationKey]) = unavailable
    override def find(id: UserId): RepositoryF[Program, Option[User]] = inspect(_.users.get(id))
    override def findVersioned(id: UserId) = unavailable
    override def updateEmbedding(id: UserId, embedding: EntityEmbedding) = unavailable
    override def updateEmbedding(observed: Versioned[User], embedding: EntityEmbedding) = unavailable
  }

  object Jobs extends JobRepository[Program] {
    override def relatedJobs(scope: HiringReadScope, keys: List[JobRelationKey]) = unavailable
    override def find(id: JobId): RepositoryF[Program, Option[Job]] = inspect(_.jobs.get(id))
    override def findVersioned(id: JobId): RepositoryF[Program, Option[Versioned[Job]]] =
      inspect(_.jobs.get(id).map(Versioned(_, 0L)))
    override def findSubmissionSnapshot(id: JobId): RepositoryF[Program, Option[JobSubmissionSnapshot]] =
      inspect(_.jobs.get(id).map(job => JobSubmissionSnapshot(job.id, job.status, 0L)))
    override def findOpen(filter: JobSearchFilter, page: JobPageRequest) = unavailable
    override def nearbyJobs(scope: HiringReadScope, query: NearbyJobsQuery, limit: Int) = unavailable
    override def jobDiscoveryFacets(
        scope: HiringReadScope,
        query: JobFacetQuery,
        matchedIds: Option[Set[JobId]],
        maxTime: Option[FiniteDuration]
    ): RepositoryF[Program, JobDiscoveryFacets] = unavailable
    override def findAll(page: JobPageRequest) = unavailable
    override def findByRecruiter(recruiterId: UserId, page: JobPageRequest) = unavailable
    override def createWithEvents(
        job: Job,
        now: Instant,
        events: List[OperationalEventEnvelope],
        context: MutationWriteContext
    ): RepositoryF[Program, Unit] =
      RepositoryF.lift[Program, Unit](
        StateT.modify[Outcome, World](world =>
          world.copy(jobs = world.jobs + (job.id -> job), events = world.events ++ events)
        )
      )
    override def updateWithEvents(
        expected: Versioned[Job],
        replacement: Job,
        now: Instant,
        events: List[OperationalEventEnvelope],
        context: MutationWriteContext
    ): RepositoryF[Program, Versioned[Job]] =
      RepositoryF
        .lift[Program, Unit](
          StateT.modify[Outcome, World](world =>
            world.copy(jobs = world.jobs.updated(replacement.id, replacement), events = world.events ++ events)
          )
        )
        .as(Versioned(replacement, expected.version + 1L))
    override def updateEmbedding(id: JobId, embedding: EntityEmbedding) = unavailable
    override def updateEmbedding(observed: Versioned[Job], embedding: EntityEmbedding) = unavailable
  }

  object Applications extends ApplicationRepository[Program] {
    override def find(id: ApplicationId): RepositoryF[Program, Option[Application]] = inspect(_.applications.get(id))
    override def findByCandidate(scope: HiringReadScope, page: ApplicationPageRequest) = unavailable
    override def findByJob(scope: HiringReadScope, jobId: JobId, page: ApplicationPageRequest) = unavailable
    override def history(
        scope: HiringReadScope,
        applicationId: ApplicationId,
        page: ApplicationEventPageRequest
    ) = unavailable
    override def createForOpenJobWithEvents(
        observedJob: JobSubmissionSnapshot,
        application: Application,
        initialEvent: ApplicationEvent,
        events: List[OperationalEventEnvelope],
        context: MutationWriteContext
    ): RepositoryF[Program, Unit] =
      RepositoryF.lift[Program, Unit](
        StateT.modify[Outcome, World](world =>
          world.copy(
            applications = world.applications + (application.id -> application),
            applicationEvents = world.applicationEvents :+ initialEvent,
            events = world.events ++ events
          )
        )
      )
    override def updateStatusWithEvents(
        application: Application,
        event: ApplicationEvent,
        events: List[OperationalEventEnvelope],
        context: MutationWriteContext
    ): RepositoryF[Program, Unit] =
      RepositoryF.lift[Program, Unit](
        StateT.modify[Outcome, World](world =>
          world.copy(
            applications = world.applications.updated(application.id, application),
            applicationEvents = world.applicationEvents :+ event,
            events = world.events ++ events
          )
        )
      )
  }

  /** A read model that admits every viewer: authorization is exercised by the services under test, not here. */
  object OpenReadModel extends HiringReadModel[Program] {
    private def unsupported[A]: UseCaseF[Program, A] = UseCaseF.repository(unavailable[A])
    override def relatedUsers(actor: ActorContext, keys: List[UserRelationKey]) = unsupported
    override def relatedJobs(actor: ActorContext, keys: List[JobRelationKey]) = unsupported
    override def viewer(actor: ActorContext) = unsupported
    override def canViewUserEmails(actor: ActorContext, userIds: List[UserId]) = unsupported
    override def application(id: ApplicationId) = unsupported
    override def canViewApplication(actor: ActorContext, applicationId: ApplicationId) =
      UseCaseF.ensure[Program](true, UseCaseError.Repository(RepositoryError.Unavailable))
    override def applicationHistory(
        actor: ActorContext,
        applicationId: ApplicationId,
        page: ApplicationEventPageRequest
    ) = unsupported
  }

  /** Runs the write directly: this world has no transactions to share with a receipt. */
  object Receipts extends MutationReceiptRepository[Program] {
    override def execute[A, E](
        key: MutationReceiptKey,
        fingerprint: MutationReceiptFingerprint,
        now: Instant,
        expiresAt: Instant
    )(
        write: MutationWriteContext => RepositoryF[Program, MutationWriteOutcome[A, E]]
    ): RepositoryF[Program, MutationReceiptExecution[A, E]] =
      write(MutationWriteContext.directWrite).map {
        case MutationWriteOutcome.Rejected(error) => MutationReceiptExecution.Rejected(error)
        case MutationWriteOutcome.Applied(value)  => MutationReceiptExecution.Applied(value.value, value.entity)
      }
  }

  /** Remembers applied writes by key, so a repeated request replays instead of writing again. */
  object RememberingReceipts extends MutationReceiptRepository[Program] {
    override def execute[A, E](
        key: MutationReceiptKey,
        fingerprint: MutationReceiptFingerprint,
        now: Instant,
        expiresAt: Instant
    )(
        write: MutationWriteContext => RepositoryF[Program, MutationWriteOutcome[A, E]]
    ): RepositoryF[Program, MutationReceiptExecution[A, E]] =
      RepositoryF
        .lift[Program, Option[(MutationReceiptFingerprint, MutationEntityReference)]](
          StateT.inspect[Outcome, World, Option[(MutationReceiptFingerprint, MutationEntityReference)]](
            _.receipts.get(key)
          )
        )
        .flatMap {
          case Some((stored, entity)) if stored == fingerprint =>
            RepositoryF.pure(MutationReceiptExecution.Replay(entity))
          case Some(_) => RepositoryF.pure(MutationReceiptExecution.FingerprintMismatch)
          case None    =>
            write(MutationWriteContext.directWrite).flatMap {
              case MutationWriteOutcome.Rejected(error) => RepositoryF.pure(MutationReceiptExecution.Rejected(error))
              case MutationWriteOutcome.Applied(value)  =>
                RepositoryF
                  .lift[Program, Unit](
                    StateT.modify[Outcome, World](world =>
                      world.copy(receipts = world.receipts + (key -> (fingerprint -> value.entity)))
                    )
                  )
                  .as(MutationReceiptExecution.Applied(value.value, value.entity))
            }
        }
  }

  val countingPublisher: EmbeddingWorkPublisher[Program] = new EmbeddingWorkPublisher[Program] {
    override def wake: Program[Unit] = StateT.modify(world => world.copy(wakes = world.wakes + 1))
  }

  val failingPublisher: EmbeddingWorkPublisher[Program] = new EmbeddingWorkPublisher[Program] {
    override def wake: Program[Unit] = StateT.liftF[Outcome, World, Unit](Left(new IllegalStateException("wake")))
  }
}
