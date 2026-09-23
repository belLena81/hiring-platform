package com.example.graphQL.cats.repository.protocol

import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{
  AccountCredentials,
  Application,
  ApplicationEvent,
  EntityEmbedding,
  Job,
  User,
  UserPageRequest,
  UserProfile
}
import com.example.graphQL.cats.shared.events.{OperationalEventEnvelope, SearchSession}
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.shared.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest}
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob, VectorSearchQuery}
import java.time.Instant
import java.util.UUID

/** A stable, caller-scoped key for replaying a state-changing operation. */
final case class MutationReceiptKey(operation: String, actorScope: String, idempotencyKey: UUID)

/** A one-way fingerprint of the canonical mutation input. Never persist source input here. */
final case class MutationReceiptFingerprint private (value: String)

object MutationReceiptFingerprint {
  def fromCanonicalInput(input: String): MutationReceiptFingerprint =
    MutationReceiptFingerprint(SourceHash.sha256(input))

  private[repository] def stored(value: String): MutationReceiptFingerprint = MutationReceiptFingerprint(value)
}

/** A non-sensitive reference from a completed receipt to its authoritative result. */
final case class MutationEntityReference(entityType: String, entityId: String)

enum MutationReceiptState {
  case InProgress, Completed
}

final case class MutationReceipt(
    key: MutationReceiptKey,
    fingerprint: MutationReceiptFingerprint,
    state: MutationReceiptState,
    entity: Option[MutationEntityReference],
    createdAt: Instant,
    completedAt: Option[Instant],
    expiresAt: Instant
)

/** Opaque transaction capability supplied only by a persistence adapter. */
trait MutationWriteContext

object MutationWriteContext {
  val noop: MutationWriteContext = new MutationWriteContext {}
}

final case class MutationReceiptWrite[+A](value: A, entity: MutationEntityReference)

enum MutationReceiptExecution[+A, +E] {
  case Applied(value: A, entity: MutationEntityReference)
  case Replay(entity: MutationEntityReference)
  case Rejected(error: E)
  case FingerprintMismatch
  case InProgress
}

/** Runs a state write and its idempotency receipt atomically when the adapter supports transactions. The callback must
  * use the supplied context for every participating write.
  */
trait MutationReceiptRepository {
  def execute[A, E](
      key: MutationReceiptKey,
      fingerprint: MutationReceiptFingerprint,
      now: Instant,
      expiresAt: Instant
  )(
      write: MutationWriteContext => IO[Either[RepositoryError, Either[E, MutationReceiptWrite[A]]]]
  ): IO[Either[RepositoryError, MutationReceiptExecution[A, E]]]
}

object MutationReceiptRepository {
  val noop: MutationReceiptRepository = new MutationReceiptRepository {
    override def execute[A, E](
        key: MutationReceiptKey,
        fingerprint: MutationReceiptFingerprint,
        now: Instant,
        expiresAt: Instant
    )(
        write: MutationWriteContext => IO[Either[RepositoryError, Either[E, MutationReceiptWrite[A]]]]
    ): IO[Either[RepositoryError, MutationReceiptExecution[A, E]]] =
      write(MutationWriteContext.noop).map(
        _.map(
          _.fold(
            MutationReceiptExecution.Rejected(_),
            value => MutationReceiptExecution.Applied(value.value, value.entity)
          )
        )
      )
  }
}

trait UserRepository {
  def find(id: UserId): IO[Either[RepositoryError, Option[User]]]
  def findMany(ids: List[UserId]): IO[Either[RepositoryError, List[User]]]
  def updateEmbedding(id: UserId, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]]
  def updateEmbedding(observed: User, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]] =
    updateEmbedding(observed.id, embedding)
}

trait UserAccountRepository {
  def bootstrap(
      user: User,
      passwordHash: String,
      context: MutationWriteContext = MutationWriteContext.noop
  ): IO[Either[RepositoryError, Unit]]
  def initialized: IO[Either[RepositoryError, Boolean]]
  def createAccount(
      user: User,
      passwordHash: String,
      now: Instant,
      context: MutationWriteContext = MutationWriteContext.noop
  ): IO[Either[RepositoryError, Unit]]
  def findByCanonicalName(nameCanonical: String): IO[Either[RepositoryError, Option[AccountCredentials]]]
  def updateProfile(
      userId: UserId,
      profile: UserProfile,
      now: Instant,
      context: MutationWriteContext = MutationWriteContext.noop
  ): IO[Either[RepositoryError, User]]
  def listAccounts(page: UserPageRequest): IO[Either[RepositoryError, List[User]]]
  def deleteAccount(
      userId: UserId,
      now: Instant,
      tombstone: String,
      context: MutationWriteContext = MutationWriteContext.noop
  ): IO[Either[RepositoryError, Unit]]
}

/** A durable request for removing a deleted subject from analytical projections. */
final case class AnalyticsErasureRequest(userId: UserId, requestedAt: Instant)

trait AnalyticsErasureRequestRepository {

  /** Records the request in the caller's mutation transaction. Repeating the same request is intentionally idempotent
    * so receipt replay cannot create work twice.
    */
  def enqueue(userId: UserId, now: Instant, context: MutationWriteContext): IO[Either[RepositoryError, Unit]]
}

object AnalyticsErasureRequestRepository {
  val unavailable: AnalyticsErasureRequestRepository = new AnalyticsErasureRequestRepository {
    override def enqueue(
        userId: UserId,
        now: Instant,
        context: MutationWriteContext
    ): IO[Either[RepositoryError, Unit]] =
      IO.pure(Left(RepositoryError.Unavailable))
  }
}

trait AnalyticsReportRepository {
  def latest: IO[Either[RepositoryError, Option[AnalyticsReportSnapshot]]]
}

/** Publishes a complete, immutable analytical snapshot. The analytics batch owns expiry calculation; the operational
  * API only owns the read model contract.
  */
trait AnalyticsReportSnapshotPublisher {
  def publish(snapshot: AnalyticsReportSnapshot, expiresAt: Instant): IO[Either[RepositoryError, Unit]]
}

object AnalyticsReportSnapshotPublisher {
  val unavailable: AnalyticsReportSnapshotPublisher = new AnalyticsReportSnapshotPublisher {
    override def publish(snapshot: AnalyticsReportSnapshot, expiresAt: Instant): IO[Either[RepositoryError, Unit]] =
      IO.pure(Left(RepositoryError.Unavailable))
  }
}

final case class AnalyticsReportSnapshot(
    asOf: Instant,
    funnel: List[AnalyticsFunnelDay],
    timeToHire: Option[AnalyticsTimeToHire],
    skillPostingActivity: List[AnalyticsSkillPostingDay]
)

final case class AnalyticsFunnelDay(
    day: Instant,
    created: Long,
    accepted: Long,
    declined: Long,
    interview: Long,
    hired: Long,
    rejected: Long
)
final case class AnalyticsTimeToHire(
    p50Hours: Double,
    p75Hours: Double,
    p90Hours: Double,
    p95Hours: Double,
    eligibleCount: Long,
    excludedCount: Long
)
final case class AnalyticsSkillPostingDay(day: Instant, skill: String, postings: Long)

trait JobRepository {
  def find(id: JobId): IO[Either[RepositoryError, Option[Job]]]
  def findMany(ids: List[JobId]): IO[Either[RepositoryError, List[Job]]]
  def findOpen(filter: JobSearchFilter, page: JobPageRequest): IO[Either[RepositoryError, List[Job]]]
  def findAll(page: JobPageRequest): IO[Either[RepositoryError, List[Job]]]
  def findByRecruiter(recruiterId: UserId, page: JobPageRequest): IO[Either[RepositoryError, List[Job]]]
  def create(job: Job, now: Instant): IO[Either[RepositoryError, Unit]]
  def createWithEvents(
      job: Job,
      now: Instant,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext = MutationWriteContext.noop
  ): IO[Either[RepositoryError, Unit]]
  def update(expected: Job, replacement: Job, now: Instant): IO[Either[RepositoryError, Job]]
  def updateWithEvents(
      expected: Job,
      replacement: Job,
      now: Instant,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext = MutationWriteContext.noop
  ): IO[Either[RepositoryError, Job]]
  def updateEmbedding(id: JobId, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]]
  def updateEmbedding(observed: Job, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]] =
    updateEmbedding(observed.id, embedding)
}

trait EmbeddingService {
  def embed(input: EmbeddingInput): IO[Either[EmbeddingError, EmbeddingVector]]
}

enum EmbeddingWorkKind {
  case Job, CandidateProfile
}

final case class EmbeddingWorkKey(kind: EmbeddingWorkKind, entityId: String) {
  val value: String = s"${kind.toString}:$entityId"
}

final case class ClaimedEmbeddingWork(
    key: EmbeddingWorkKey,
    generation: Long,
    attempts: Int,
    leaseToken: String
)

enum EmbeddingWorkFailure {
  case RetryExhausted, DocumentTooLarge, InvalidWorkKey
}

trait EmbeddingWorkRepository {
  def enqueue(key: EmbeddingWorkKey, now: Instant): IO[Either[RepositoryError, Unit]]
  def claim(
      workerId: String,
      now: Instant,
      leaseUntil: Instant
  ): IO[Either[RepositoryError, Option[ClaimedEmbeddingWork]]]
  def complete(claim: ClaimedEmbeddingWork): IO[Either[RepositoryError, Unit]]
  def retry(claim: ClaimedEmbeddingWork, availableAt: Instant): IO[Either[RepositoryError, Unit]]
  def fail(claim: ClaimedEmbeddingWork, failure: EmbeddingWorkFailure, now: Instant): IO[Either[RepositoryError, Unit]]
}

enum EmbeddingInputType {
  case Query, Document
}

final case class EmbeddingInput(text: String, inputType: EmbeddingInputType)
final case class EmbeddingVector(values: List[Float], model: String, dimension: Int)

enum EmbeddingError {
  case ProviderUnavailable
  case InvalidResponse
}

trait SemanticSearchRepository {
  def searchJobs(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedJob]]]
  def recommendedJobs(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedJob]]]
  def candidateMatches(query: VectorSearchQuery): IO[Either[RepositoryError, List[RankedCandidate]]]
}

trait SearchSessionRepository {
  def save(session: SearchSession, event: OperationalEventEnvelope): IO[Either[RepositoryError, Unit]]
  def find(id: java.util.UUID): IO[Either[RepositoryError, Option[SearchSession]]]
  def recordInteraction(
      event: OperationalEventEnvelope,
      context: MutationWriteContext = MutationWriteContext.noop
  ): IO[Either[RepositoryError, Boolean]]
}

final case class ClaimedOperationalEvent(
    event: OperationalEventEnvelope,
    envelopeBytes: Array[Byte],
    partitionKey: String,
    leaseToken: String,
    attempts: Int
)

enum OperationalEventFailureCategory {
  case MalformedEnvelope, UnsupportedVersion, InvalidOrdering, ConsumerFailure
}

trait OperationalEventOutboxRepository {
  def claim(
      workerId: String,
      now: Instant,
      leaseUntil: Instant,
      limit: Int
  ): IO[Either[RepositoryError, List[ClaimedOperationalEvent]]]
  def markPublished(
      eventId: java.util.UUID,
      leaseToken: String,
      now: Instant,
      retentionExpiresAt: Instant
  ): IO[Either[RepositoryError, Unit]]
  def releaseForRetry(
      eventId: java.util.UUID,
      leaseToken: String,
      now: Instant,
      availableAt: Instant
  ): IO[Either[RepositoryError, Unit]]
  def markFailed(
      eventId: java.util.UUID,
      leaseToken: String,
      now: Instant,
      reason: String
  ): IO[Either[RepositoryError, Unit]]
}

trait ConsumerReceiptRepository {
  def exists(consumerGroup: String, eventId: java.util.UUID): IO[Either[RepositoryError, Boolean]]
  def record(
      consumerGroup: String,
      event: OperationalEventEnvelope,
      now: Instant,
      expiresAt: Instant
  ): IO[Either[RepositoryError, Boolean]]
}

final case class EventQuarantineRecord(
    topic: String,
    partition: Int,
    offset: Long,
    category: OperationalEventFailureCategory,
    reason: String,
    rawBytes: Array[Byte],
    occurredAt: Instant,
    expiresAt: Instant
)

trait EventQuarantineRepository {
  def save(record: EventQuarantineRecord): IO[Either[RepositoryError, Unit]]
}

object SearchSessionRepository {
  def noop: SearchSessionRepository = new SearchSessionRepository {
    override def save(session: SearchSession, event: OperationalEventEnvelope): IO[Either[RepositoryError, Unit]] =
      IO.pure(Right(()))
    override def find(id: java.util.UUID): IO[Either[RepositoryError, Option[SearchSession]]] =
      IO.pure(Right(None))
    override def recordInteraction(
        event: OperationalEventEnvelope,
        context: MutationWriteContext
    ): IO[Either[RepositoryError, Boolean]] =
      IO.pure(Right(true))
  }
}

trait ApplicationRepository {
  def find(id: ApplicationId): IO[Either[RepositoryError, Option[Application]]]
  def findByCandidate(candidateId: UserId, page: ApplicationPageRequest): IO[Either[RepositoryError, List[Application]]]
  def findByJob(jobId: JobId, page: ApplicationPageRequest): IO[Either[RepositoryError, List[Application]]]
  def history(
      applicationId: ApplicationId,
      page: ApplicationEventPageRequest
  ): IO[Either[RepositoryError, List[ApplicationEvent]]]
  def createForOpenJob(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent
  ): IO[Either[RepositoryError, Unit]]
  def createForOpenJobWithEvents(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext = MutationWriteContext.noop
  ): IO[Either[RepositoryError, Unit]]
  def updateStatus(application: Application, event: ApplicationEvent): IO[Either[RepositoryError, Unit]]
  def updateStatusWithEvents(
      application: Application,
      event: ApplicationEvent,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext = MutationWriteContext.noop
  ): IO[Either[RepositoryError, Unit]]
}
