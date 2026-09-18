package com.example.graphQL.cats.repository.protocol

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{AccountCredentials, Application, ApplicationEvent, CandidateProfile, EntityEmbedding, Job, RecruiterProfile, User, UserPageRequest}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.shared.pagination.{ApplicationEventPageRequest, ApplicationPageRequest, JobPageRequest}
import com.example.graphQL.cats.shared.search.{JobSearchFilter, RankedCandidate, RankedJob, VectorSearchQuery}
import java.time.Instant

trait UserRepository[F[_]] {
  def find(id: UserId): F[Option[User]]
  def findMany(ids: List[UserId]): F[List[User]]
  def updateEmbedding(id: UserId, embedding: EntityEmbedding): F[Either[RepositoryError, Unit]]
}

trait UserAccountRepository[F[_]] {
  def bootstrap(user: User, passwordHash: String): F[Either[RepositoryError, Unit]]
  def initialized: F[Boolean]
  def createAccount(user: User, passwordHash: String): F[Either[RepositoryError, Unit]]
  def findByCanonicalName(nameCanonical: String): F[Option[AccountCredentials]]
  def updateProfile(userId: UserId, profile: Option[CandidateProfile], recruiterProfile: Option[RecruiterProfile]): F[Either[RepositoryError, User]]
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
  def update(job: Job): F[Either[RepositoryError, Job]]
  def updateEmbedding(id: JobId, observedVersion: Long, embedding: EntityEmbedding): F[Either[RepositoryError, Unit]]
}

trait EmbeddingService[F[_]] {
  def embed(input: EmbeddingInput): F[Either[EmbeddingError, EmbeddingVector]]
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
