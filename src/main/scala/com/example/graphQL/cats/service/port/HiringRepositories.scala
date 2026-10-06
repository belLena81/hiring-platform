package com.example.graphQL.cats.service.port

import cats.effect.IO
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{
  AccountCredentials,
  AccountDeletionStatus,
  Application,
  ApplicationEvent,
  EntityEmbedding,
  Job,
  User,
  PasswordHash,
  UserPageRequest,
  UserProfile
}
import com.example.graphQL.cats.service.AnalyticsReportSnapshot
import com.example.graphQL.cats.service.events.{OperationalEventEnvelope, SearchSession}
import com.example.graphQL.cats.shared.crypto.SourceHash
import com.example.graphQL.cats.domain.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest}
import com.example.graphQL.cats.service.search.{JobSearchFilter, RankedCandidate, RankedJob, VectorSearchQuery}
import java.time.Instant
import java.util.UUID

/** A stable, caller-scoped key for replaying a state-changing operation. */
final case class MutationReceiptKey(operation: String, actorScope: String, idempotencyKey: UUID)

/** A one-way fingerprint of the canonical mutation input. Never persist source input here. */
final case class MutationReceiptFingerprint private (value: String)

object MutationReceiptFingerprint {
  def fromCanonicalInput(input: String): MutationReceiptFingerprint =
    MutationReceiptFingerprint(SourceHash.sha256(input))

  private[cats] def stored(value: String): MutationReceiptFingerprint = MutationReceiptFingerprint(value)
}

/** A non-sensitive reference from a completed receipt to its authoritative result. */
final case class MutationEntityReference(entityType: String, entityId: String)

/** A repository snapshot paired with the revision required for compare-and-set writes. */
final case class Versioned[+A](value: A, version: Long)

object Versioned {
  def nextVersion(version: Long): Option[Long] = Option.when(version >= 0L && version < Long.MaxValue)(version + 1L)
}

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

  /** Explicit direct-write context for operations outside an idempotent mutation. */
  val directWrite: MutationWriteContext = new MutationWriteContext {}
}

final case class MutationReceiptWrite[+A](value: A, entity: MutationEntityReference)

enum MutationWriteOutcome[+A, +E] {
  case Applied(write: MutationReceiptWrite[A])
  case Rejected(error: E)
}

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
      write: MutationWriteContext => RepositoryIO[MutationWriteOutcome[A, E]]
  ): RepositoryIO[MutationReceiptExecution[A, E]]
}

import com.example.graphQL.cats.service.read.*

trait UserRepository {
  def relatedUsers(scope: HiringReadScope, keys: List[UserRelationKey]): RepositoryIO[List[RelatedUser]]
  def find(id: UserId): RepositoryIO[Option[User]]
  def findVersioned(id: UserId): RepositoryIO[Option[Versioned[User]]]
  def findMany(ids: List[UserId]): RepositoryIO[List[User]]
  def updateEmbedding(id: UserId, embedding: EntityEmbedding): RepositoryIO[Unit]
  def updateEmbedding(observed: Versioned[User], embedding: EntityEmbedding): RepositoryIO[Unit]
}

trait UserAccountRepository {
  def bootstrap(
      user: User,
      passwordHash: PasswordHash,
      context: MutationWriteContext
  ): RepositoryIO[Unit]
  def initialized: RepositoryIO[Boolean]
  def createAccount(
      user: User,
      passwordHash: PasswordHash,
      now: Instant,
      context: MutationWriteContext
  ): RepositoryIO[Unit]
  def findByCanonicalName(nameCanonical: String): RepositoryIO[Option[AccountCredentials]]
  def updateProfile(
      userId: UserId,
      profile: UserProfile,
      now: Instant,
      context: MutationWriteContext
  ): RepositoryIO[User]
  def listAccounts(page: UserPageRequest): RepositoryIO[List[User]]
  def deleteAccount(
      userId: UserId,
      now: Instant,
      tombstone: String,
      context: MutationWriteContext
  ): RepositoryIO[Unit]
}

/** A durable request for removing a deleted subject from analytical projections. */
final case class AnalyticsErasureRequest(userId: UserId, requestedAt: Instant)

trait AnalyticsErasureRequestRepository {

  /** Confirms an erasure worker is live before a new account deletion can mutate Mongo. */
  def workerReady(now: Instant): RepositoryIO[Unit]

  /** Records the request in the caller's mutation transaction. Repeating the same request is intentionally idempotent
    * so receipt replay cannot create work twice.
    */
  def enqueue(userId: UserId, now: Instant, context: MutationWriteContext): RepositoryIO[String]

  /** Returns lifecycle state only when the receipt belongs to the supplied subject. */
  def statusForSubject(
      userId: UserId,
      receiptId: String
  ): RepositoryIO[AccountDeletionStatus]

  /** Removes durable outbox rows attributed to the subject after producer drain has been proven. */
  def purgeSubjectOutbox(userId: UserId): RepositoryIO[Unit]

  /** Retains a completed tombstone through the replay horizon before TTL cleanup. */
  def markComplete(userId: UserId, now: Instant): RepositoryIO[Unit]
}

object AnalyticsErasureRequestRepository {
  val unavailable: AnalyticsErasureRequestRepository = new AnalyticsErasureRequestRepository {
    override def workerReady(now: Instant): RepositoryIO[Unit] =
      RepositoryIO.fromEither(Left(RepositoryError.Unavailable))

    override def enqueue(
        userId: UserId,
        now: Instant,
        context: MutationWriteContext
    ): RepositoryIO[String] =
      RepositoryIO.fromEither(Left(RepositoryError.Unavailable))

    override def statusForSubject(
        userId: UserId,
        receiptId: String
    ): RepositoryIO[AccountDeletionStatus] =
      RepositoryIO.fromEither(Left(RepositoryError.Unavailable))

    override def purgeSubjectOutbox(userId: UserId): RepositoryIO[Unit] =
      RepositoryIO.fromEither(Left(RepositoryError.Unavailable))

    override def markComplete(userId: UserId, now: Instant): RepositoryIO[Unit] =
      RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
  }
}

trait AnalyticsReportRepository {
  def latest: RepositoryIO[Option[AnalyticsReportSnapshot]]
}

/** Stable publication epoch and ordering token reserved before a batch starts doing expensive work. */
opaque type AnalyticsRunId = String

object AnalyticsRunId {
  def from(value: String): Option[AnalyticsRunId] =
    Option(value).filter(_.trim.nonEmpty)

  extension (runId: AnalyticsRunId) def value: String = runId
}

opaque type AnalyticsRangeFingerprint = String

object AnalyticsRangeFingerprint {
  def from(value: String): Option[AnalyticsRangeFingerprint] =
    Option(value).filter(_.trim.nonEmpty)

  extension (fingerprint: AnalyticsRangeFingerprint) def value: String = fingerprint
}

final case class AnalyticsReportRunReservation(
    runId: AnalyticsRunId,
    rangeFingerprint: AnalyticsRangeFingerprint,
    generation: Long,
    revision: Long
)

/** Publishes a complete, immutable analytical snapshot. The analytics batch owns expiry calculation; the operational
  * API only owns the read model contract.
  */
trait AnalyticsReportSnapshotPublisher {
  def reserve(
      runId: AnalyticsRunId,
      rangeFingerprint: AnalyticsRangeFingerprint,
      now: Instant,
      reservationExpiresAt: Instant
  ): RepositoryIO[AnalyticsReportRunReservation]

  def publish(
      reservation: AnalyticsReportRunReservation,
      snapshot: AnalyticsReportSnapshot,
      expiresAt: Instant
  ): RepositoryIO[Unit]
}

object AnalyticsReportSnapshotPublisher {
  val unavailable: AnalyticsReportSnapshotPublisher = new AnalyticsReportSnapshotPublisher {
    override def reserve(
        runId: AnalyticsRunId,
        rangeFingerprint: AnalyticsRangeFingerprint,
        now: Instant,
        reservationExpiresAt: Instant
    ): RepositoryIO[AnalyticsReportRunReservation] =
      RepositoryIO.fromEither(Left(RepositoryError.Unavailable))

    override def publish(
        reservation: AnalyticsReportRunReservation,
        snapshot: AnalyticsReportSnapshot,
        expiresAt: Instant
    ): RepositoryIO[Unit] =
      RepositoryIO.fromEither(Left(RepositoryError.Unavailable))
  }
}

trait JobRepository {
  def relatedJobs(scope: HiringReadScope, keys: List[JobRelationKey]): RepositoryIO[List[RelatedJob]]
  def find(id: JobId): RepositoryIO[Option[Job]]
  def findVersioned(id: JobId): RepositoryIO[Option[Versioned[Job]]]
  def findMany(ids: List[JobId]): RepositoryIO[List[Job]]
  def findOpen(filter: JobSearchFilter, page: JobPageRequest): RepositoryIO[List[Job]]
  def findAll(page: JobPageRequest): RepositoryIO[List[Job]]
  def findByRecruiter(recruiterId: UserId, page: JobPageRequest): RepositoryIO[List[Job]]
  def createWithEvents(
      job: Job,
      now: Instant,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): RepositoryIO[Unit]
  def updateWithEvents(
      expected: Versioned[Job],
      replacement: Job,
      now: Instant,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): RepositoryIO[Versioned[Job]]
  def updateEmbedding(id: JobId, embedding: EntityEmbedding): RepositoryIO[Unit]
  def updateEmbedding(observed: Versioned[Job], embedding: EntityEmbedding): RepositoryIO[Unit]
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
  def enqueue(key: EmbeddingWorkKey, now: Instant): RepositoryIO[Unit]
  def claim(
      workerId: String,
      now: Instant,
      leaseUntil: Instant
  ): RepositoryIO[Option[ClaimedEmbeddingWork]]
  def complete(claim: ClaimedEmbeddingWork): RepositoryIO[Unit]
  def retry(claim: ClaimedEmbeddingWork, availableAt: Instant): RepositoryIO[Unit]
  def fail(claim: ClaimedEmbeddingWork, failure: EmbeddingWorkFailure, now: Instant): RepositoryIO[Unit]
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

/** Returns ranked validation candidates up to the configured branch-result cap; the service applies the final page
  * size.
  */
trait SemanticSearchRepository {
  def authorizedJobEligibility(
      scope: com.example.graphQL.cats.service.read.HiringReadScope,
      ids: List[JobId],
      queryCandidate: Option[com.example.graphQL.cats.service.search.CandidateSearchEligibility]
  ): RepositoryIO[List[com.example.graphQL.cats.service.search.JobSearchEligibility]]
  def authorizedCandidateEligibility(
      scope: com.example.graphQL.cats.service.read.HiringReadScope,
      queryJob: com.example.graphQL.cats.service.search.JobSearchEligibility,
      ids: List[UserId]
  ): RepositoryIO[List[com.example.graphQL.cats.service.search.CandidateSearchEligibility]]

  def jobEligibility(ids: List[JobId]): RepositoryIO[List[com.example.graphQL.cats.service.search.JobSearchEligibility]]
  def candidateEligibility(
      ids: List[UserId]
  ): RepositoryIO[List[com.example.graphQL.cats.service.search.CandidateSearchEligibility]]
  def searchJobs(query: VectorSearchQuery): RepositoryIO[List[RankedJob]]
  def recommendedJobs(query: VectorSearchQuery): RepositoryIO[List[RankedJob]]
  def candidateMatches(query: VectorSearchQuery): RepositoryIO[List[RankedCandidate]]
}

trait SearchSessionRepository {
  def save(session: SearchSession, event: OperationalEventEnvelope): RepositoryIO[Unit]
  def find(id: java.util.UUID): RepositoryIO[Option[SearchSession]]
  def recordInteraction(
      event: OperationalEventEnvelope,
      context: MutationWriteContext
  ): RepositoryIO[Boolean]
}

final case class ClaimedOperationalEvent(
    event: OperationalEventEnvelope,
    envelopeBytes: Array[Byte],
    partitionKey: String,
    leaseToken: String,
    attempts: Int,
    subjectIds: List[String] = Nil
)

enum OperationalEventFailureCategory {
  case MalformedEnvelope, UnsupportedVersion, InvalidOrdering, ConsumerFailure
}

trait OperationalEventOutboxRepository {
  def claim(
      workerId: String,
      transactionalId: String,
      now: Instant,
      leaseUntil: Instant,
      limit: Int
  ): RepositoryIO[List[ClaimedOperationalEvent]]
  def renewLease(
      eventId: java.util.UUID,
      leaseToken: String,
      subjectIds: List[String],
      leaseUntil: Instant
  ): RepositoryIO[Unit]
  def markPublished(
      eventId: java.util.UUID,
      leaseToken: String,
      now: Instant,
      retentionExpiresAt: Instant
  ): RepositoryIO[Unit]
  def releaseForRetry(
      eventId: java.util.UUID,
      leaseToken: String,
      now: Instant,
      availableAt: Instant
  ): RepositoryIO[Unit]
  def markFailed(
      eventId: java.util.UUID,
      leaseToken: String,
      now: Instant,
      reason: String
  ): RepositoryIO[Unit]
}

trait ConsumerReceiptRepository {
  def exists(consumerGroup: String, eventId: java.util.UUID): RepositoryIO[Boolean]
  def record(
      consumerGroup: String,
      event: OperationalEventEnvelope,
      now: Instant,
      expiresAt: Instant
  ): RepositoryIO[Boolean]
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
  def save(record: EventQuarantineRecord): RepositoryIO[Unit]
}

object SearchSessionRepository {
  def noop: SearchSessionRepository = new SearchSessionRepository {
    override def save(session: SearchSession, event: OperationalEventEnvelope): RepositoryIO[Unit] =
      RepositoryIO.fromEither(Right(()))
    override def find(id: java.util.UUID): RepositoryIO[Option[SearchSession]] =
      RepositoryIO.fromEither(Right(None))
    override def recordInteraction(
        event: OperationalEventEnvelope,
        context: MutationWriteContext
    ): RepositoryIO[Boolean] =
      RepositoryIO.fromEither(Right(true))
  }
}

trait ApplicationRepository {
  def find(id: ApplicationId): RepositoryIO[Option[Application]]
  def findByCandidate(scope: HiringReadScope, page: ApplicationPageRequest): RepositoryIO[List[Application]]
  def findByJob(scope: HiringReadScope, jobId: JobId, page: ApplicationPageRequest): RepositoryIO[List[Application]]
  def history(
      scope: HiringReadScope,
      applicationId: ApplicationId,
      page: ApplicationEventPageRequest
  ): RepositoryIO[List[ApplicationEvent]]
  def createForOpenJob(
      observedJob: Versioned[Job],
      application: Application,
      initialEvent: ApplicationEvent
  ): RepositoryIO[Unit]
  def createForOpenJobWithEvents(
      observedJob: Versioned[Job],
      application: Application,
      initialEvent: ApplicationEvent,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): RepositoryIO[Unit]
  def updateStatus(application: Application, event: ApplicationEvent): RepositoryIO[Unit]
  def updateStatusWithEvents(
      application: Application,
      event: ApplicationEvent,
      events: List[OperationalEventEnvelope],
      context: MutationWriteContext
  ): RepositoryIO[Unit]
}
