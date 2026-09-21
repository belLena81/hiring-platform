package com.example.graphQL.cats.repository.protocol

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{AccountCredentials, Application, ApplicationEvent, EntityEmbedding, Job, User, UserPageRequest, UserProfile}
import com.example.graphQL.cats.shared.events.{OperationalEventEnvelope, SearchSession}
import com.example.graphQL.cats.shared.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest}
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob, VectorSearchQuery}
import java.time.Instant

trait UserRepository[F[_]] {
  def find(id: UserId): F[Either[RepositoryError, Option[User]]]
  def findMany(ids: List[UserId]): F[Either[RepositoryError, List[User]]]
  def updateEmbedding(id: UserId, embedding: EntityEmbedding): F[Either[RepositoryError, Unit]]
}

trait UserAccountRepository[F[_]] {
  def bootstrap(user: User, passwordHash: String): F[Either[RepositoryError, Unit]]
  def initialized: F[Either[RepositoryError, Boolean]]
  def createAccount(user: User, passwordHash: String, now: Instant): F[Either[RepositoryError, Unit]]
  def findByCanonicalName(nameCanonical: String): F[Either[RepositoryError, Option[AccountCredentials]]]
  def updateProfile(userId: UserId, profile: UserProfile, now: Instant): F[Either[RepositoryError, User]]
  def listAccounts(page: UserPageRequest): F[Either[RepositoryError, List[User]]]
  def deleteAccount(userId: UserId, now: Instant, tombstone: String): F[Either[RepositoryError, Unit]]
}

trait JobRepository[F[_]] {
  def find(id: JobId): F[Either[RepositoryError, Option[Job]]]
  def findMany(ids: List[JobId]): F[Either[RepositoryError, List[Job]]]
  def findOpen(filter: JobSearchFilter, page: JobPageRequest): F[Either[RepositoryError, List[Job]]]
  def findAll(page: JobPageRequest): F[Either[RepositoryError, List[Job]]]
  def findByRecruiter(recruiterId: UserId, page: JobPageRequest): F[Either[RepositoryError, List[Job]]]
  def create(job: Job, now: Instant): F[Either[RepositoryError, Unit]]
  def createWithEvents(job: Job, now: Instant, events: List[OperationalEventEnvelope]): F[Either[RepositoryError, Unit]]
  def update(job: Job, now: Instant): F[Either[RepositoryError, Job]]
  def updateWithEvents(job: Job, now: Instant, events: List[OperationalEventEnvelope]): F[Either[RepositoryError, Job]]
  def updateEmbedding(id: JobId, embedding: EntityEmbedding): F[Either[RepositoryError, Unit]]
}

trait EmbeddingService[F[_]] {
  def embed(input: EmbeddingInput): F[Either[EmbeddingError, EmbeddingVector]]
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

trait EmbeddingWorkRepository[F[_]] {
  def enqueue(key: EmbeddingWorkKey, now: Instant): F[Either[RepositoryError, Unit]]
  def claim(workerId: String, now: Instant, leaseUntil: Instant): F[Either[RepositoryError, Option[ClaimedEmbeddingWork]]]
  def complete(claim: ClaimedEmbeddingWork): F[Either[RepositoryError, Unit]]
  def retry(claim: ClaimedEmbeddingWork, availableAt: Instant): F[Either[RepositoryError, Unit]]
  def fail(claim: ClaimedEmbeddingWork, failure: EmbeddingWorkFailure, now: Instant): F[Either[RepositoryError, Unit]]
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

trait SemanticSearchRepository[F[_]] {
  def searchJobs(query: VectorSearchQuery): F[Either[RepositoryError, List[RankedJob]]]
  def recommendedJobs(query: VectorSearchQuery): F[Either[RepositoryError, List[RankedJob]]]
  def candidateMatches(query: VectorSearchQuery): F[Either[RepositoryError, List[RankedCandidate]]]
}

trait SearchSessionRepository[F[_]] {
  def save(session: SearchSession, event: OperationalEventEnvelope): F[Either[RepositoryError, Unit]]
  def find(id: java.util.UUID): F[Either[RepositoryError, Option[SearchSession]]]
  def recordInteraction(event: OperationalEventEnvelope): F[Either[RepositoryError, Boolean]]
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

trait OperationalEventOutboxRepository[F[_]] {
  def claim(workerId: String, now: Instant, leaseUntil: Instant, limit: Int): F[Either[RepositoryError, List[ClaimedOperationalEvent]]]
  def markPublished(eventId: java.util.UUID, leaseToken: String, now: Instant, retentionExpiresAt: Instant): F[Either[RepositoryError, Unit]]
  def releaseForRetry(eventId: java.util.UUID, leaseToken: String, now: Instant, availableAt: Instant): F[Either[RepositoryError, Unit]]
  def markFailed(eventId: java.util.UUID, leaseToken: String, now: Instant, reason: String): F[Either[RepositoryError, Unit]]
}

trait ConsumerReceiptRepository[F[_]] {
  def exists(consumerGroup: String, eventId: java.util.UUID): F[Either[RepositoryError, Boolean]]
  def record(consumerGroup: String, event: OperationalEventEnvelope, now: Instant, expiresAt: Instant): F[Either[RepositoryError, Boolean]]
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

trait EventQuarantineRepository[F[_]] {
  def save(record: EventQuarantineRecord): F[Either[RepositoryError, Unit]]
}

object SearchSessionRepository {
  def noop[F[_]](using cats.Applicative[F]): SearchSessionRepository[F] = new SearchSessionRepository[F] {
    override def save(session: SearchSession, event: OperationalEventEnvelope): F[Either[RepositoryError, Unit]] =
      Right(()).pure[F]
    override def find(id: java.util.UUID): F[Either[RepositoryError, Option[SearchSession]]] =
      Right(None).pure[F]
    override def recordInteraction(event: OperationalEventEnvelope): F[Either[RepositoryError, Boolean]] =
      Right(true).pure[F]
  }
}

trait ApplicationRepository[F[_]] {
  def find(id: ApplicationId): F[Either[RepositoryError, Option[Application]]]
  def findByCandidate(candidateId: UserId, page: ApplicationPageRequest): F[Either[RepositoryError, List[Application]]]
  def findByJob(jobId: JobId, page: ApplicationPageRequest): F[Either[RepositoryError, List[Application]]]
  def history(applicationId: ApplicationId, page: ApplicationEventPageRequest): F[Either[RepositoryError, List[ApplicationEvent]]]
  def createForOpenJob(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent
  ): F[Either[RepositoryError, Unit]]
  def createForOpenJobWithEvents(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent,
      events: List[OperationalEventEnvelope]
  ): F[Either[RepositoryError, Unit]]
  def updateStatus(application: Application, event: ApplicationEvent): F[Either[RepositoryError, Unit]]
  def updateStatusWithEvents(
      application: Application,
      event: ApplicationEvent,
      events: List[OperationalEventEnvelope]
  ): F[Either[RepositoryError, Unit]]
}
