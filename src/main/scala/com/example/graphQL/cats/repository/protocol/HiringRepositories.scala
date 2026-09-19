package com.example.graphQL.cats.repository.protocol

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{AccountCredentials, Application, ApplicationEvent, EntityEmbedding, Job, User, UserPageRequest, UserProfile}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.shared.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest}
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob, VectorSearchQuery}
import java.time.Instant
import scala.annotation.unused

trait UserRepository[F[_]] {
  def find(id: UserId): F[Option[User]]
  def findMany(ids: List[UserId]): F[List[User]]
  def updateEmbedding(id: UserId, observedVersion: Long, embedding: EntityEmbedding): F[Either[RepositoryError, Unit]]
}

trait UserAccountRepository[F[_]] {
  def bootstrap(user: User, passwordHash: String): F[Either[RepositoryError, Unit]]
  def initialized: F[Boolean]
  def createAccount(user: User, passwordHash: String): F[Either[RepositoryError, Unit]]
  def createAccount(user: User, passwordHash: String, @unused now: Instant): F[Either[RepositoryError, Unit]] =
    createAccount(user, passwordHash)
  def findByCanonicalName(nameCanonical: String): F[Option[AccountCredentials]]
  def updateProfile(userId: UserId, profile: UserProfile, now: Instant): F[Either[RepositoryError, User]]
  def listAccounts(page: UserPageRequest): F[List[User]]
  def deleteAccount(userId: UserId, now: Instant, tombstone: String): F[Either[RepositoryError, Unit]]
}

trait JobRepository[F[_]] {
  def find(id: JobId): F[Option[Job]]
  def findMany(ids: List[JobId]): F[List[Job]]
  def findOpen(filter: JobSearchFilter, page: JobPageRequest): F[List[Job]]
  def findAll(page: JobPageRequest): F[List[Job]]
  def findByRecruiter(recruiterId: UserId, page: JobPageRequest): F[List[Job]]
  def create(job: Job): F[Either[RepositoryError, Unit]]
  def create(job: Job, @unused now: Instant): F[Either[RepositoryError, Unit]] = create(job)
  def update(job: Job): F[Either[RepositoryError, Job]]
  def update(job: Job, @unused now: Instant): F[Either[RepositoryError, Job]] = update(job)
  def updateEmbedding(id: JobId, observedVersion: Long, embedding: EntityEmbedding): F[Either[RepositoryError, Unit]]
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
  case RetryExhausted, DocumentTooLarge
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

trait ApplicationRepository[F[_]] {
  def find(id: ApplicationId): F[Option[Application]]
  def findByCandidate(candidateId: UserId, page: ApplicationPageRequest): F[List[Application]]
  def findByJob(jobId: JobId, page: ApplicationPageRequest): F[List[Application]]
  def history(applicationId: ApplicationId, page: ApplicationEventPageRequest): F[List[ApplicationEvent]]
  def createForOpenJob(
      observedJob: Job,
      application: Application,
      initialEvent: ApplicationEvent
  ): F[Either[RepositoryError, Unit]]
  def updateStatus(application: Application, event: ApplicationEvent): F[Either[RepositoryError, Unit]]
}
