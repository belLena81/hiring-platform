package com.example.graphQL.cats.repository.protocol

import cats.effect.IO
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{AccountCredentials, Application, ApplicationEvent, EntityEmbedding, Job, User, UserPageRequest, UserProfile}
import com.example.graphQL.cats.shared.events.{OperationalEventEnvelope, SearchSession}
import com.example.graphQL.cats.shared.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest}
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob, VectorSearchQuery}
import java.time.Instant

trait UserRepository {
  def find(id: UserId): IO[Either[RepositoryError, Option[User]]]
  def findMany(ids: List[UserId]): IO[Either[RepositoryError, List[User]]]
  def updateEmbedding(id: UserId, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]]
}

trait UserAccountRepository {
  def bootstrap(user: User, passwordHash: String): IO[Either[RepositoryError, Unit]]
  def initialized: IO[Either[RepositoryError, Boolean]]
  def createAccount(user: User, passwordHash: String, now: Instant): IO[Either[RepositoryError, Unit]]
  def findByCanonicalName(nameCanonical: String): IO[Either[RepositoryError, Option[AccountCredentials]]]
  def updateProfile(userId: UserId, profile: UserProfile, now: Instant): IO[Either[RepositoryError, User]]
  def listAccounts(page: UserPageRequest): IO[Either[RepositoryError, List[User]]]
  def deleteAccount(userId: UserId, now: Instant, tombstone: String): IO[Either[RepositoryError, Unit]]
}

trait JobRepository {
  def find(id: JobId): IO[Either[RepositoryError, Option[Job]]]
  def findMany(ids: List[JobId]): IO[Either[RepositoryError, List[Job]]]
  def findOpen(filter: JobSearchFilter, page: JobPageRequest): IO[Either[RepositoryError, List[Job]]]
  def findAll(page: JobPageRequest): IO[Either[RepositoryError, List[Job]]]
  def findByRecruiter(recruiterId: UserId, page: JobPageRequest): IO[Either[RepositoryError, List[Job]]]
  def create(job: Job, now: Instant): IO[Either[RepositoryError, Unit]]
  def createWithEvents(job: Job, now: Instant, events: List[OperationalEventEnvelope]): IO[Either[RepositoryError, Unit]]
  def update(job: Job, now: Instant): IO[Either[RepositoryError, Job]]
  def updateWithEvents(job: Job, now: Instant, events: List[OperationalEventEnvelope]): IO[Either[RepositoryError, Job]]
  def updateEmbedding(id: JobId, embedding: EntityEmbedding): IO[Either[RepositoryError, Unit]]
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
  def claim(workerId: String, now: Instant, leaseUntil: Instant): IO[Either[RepositoryError, Option[ClaimedEmbeddingWork]]]
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
  def recordInteraction(event: OperationalEventEnvelope): IO[Either[RepositoryError, Boolean]]
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
  def claim(workerId: String, now: Instant, leaseUntil: Instant, limit: Int): IO[Either[RepositoryError, List[ClaimedOperationalEvent]]]
  def markPublished(eventId: java.util.UUID, leaseToken: String, now: Instant, retentionExpiresAt: Instant): IO[Either[RepositoryError, Unit]]
  def releaseForRetry(eventId: java.util.UUID, leaseToken: String, now: Instant, availableAt: Instant): IO[Either[RepositoryError, Unit]]
  def markFailed(eventId: java.util.UUID, leaseToken: String, now: Instant, reason: String): IO[Either[RepositoryError, Unit]]
}

trait ConsumerReceiptRepository {
  def exists(consumerGroup: String, eventId: java.util.UUID): IO[Either[RepositoryError, Boolean]]
  def record(consumerGroup: String, event: OperationalEventEnvelope, now: Instant, expiresAt: Instant): IO[Either[RepositoryError, Boolean]]
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
    override def recordInteraction(event: OperationalEventEnvelope): IO[Either[RepositoryError, Boolean]] =
      IO.pure(Right(true))
  }
}

trait ApplicationRepository {
  def find(id: ApplicationId): IO[Either[RepositoryError, Option[Application]]]
  def findByCandidate(candidateId: UserId, page: ApplicationPageRequest): IO[Either[RepositoryError, List[Application]]]
  def findByJob(jobId: JobId, page: ApplicationPageRequest): IO[Either[RepositoryError, List[Application]]]
  def history(applicationId: ApplicationId, page: ApplicationEventPageRequest): IO[Either[RepositoryError, List[ApplicationEvent]]]
  def createForOpenJob(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent
  ): IO[Either[RepositoryError, Unit]]
  def createForOpenJobWithEvents(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent,
      events: List[OperationalEventEnvelope]
  ): IO[Either[RepositoryError, Unit]]
  def updateStatus(application: Application, event: ApplicationEvent): IO[Either[RepositoryError, Unit]]
  def updateStatusWithEvents(
      application: Application,
      event: ApplicationEvent,
      events: List[OperationalEventEnvelope]
  ): IO[Either[RepositoryError, Unit]]
}
