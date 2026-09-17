package com.example.graphQL.cats.application.port

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.domain.error.DomainValidationError.InvalidNumber
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, EmbeddingMeta, EntityEmbedding, Job, SearchMode, User}
import java.time.Instant
import java.util.UUID

trait UserRepository[F[_]] {
  def find(id: UserId): F[Option[User]]
  def findMany(ids: List[UserId]): F[List[User]]
  def updateEmbedding(id: UserId, embedding: EntityEmbedding): F[Either[RepositoryError, Unit]]
}

enum RepositoryError {
  case DuplicateApplication
  case Conflict
  case Unavailable
}

trait JobRepository[F[_]] {
  def find(id: JobId): F[Option[Job]]
  def findMany(ids: List[JobId]): F[List[Job]]
  def findOpen(filter: JobSearchFilter, page: JobPageRequest): F[List[Job]]
  def findAll(page: JobPageRequest): F[List[Job]]
  def findByRecruiter(recruiterId: UserId, page: JobPageRequest): F[List[Job]]
  def create(job: Job): F[Either[RepositoryError, Unit]]
  def update(job: Job): F[Either[RepositoryError, Job]]
  def updateEmbedding(id: JobId, embedding: EntityEmbedding): F[Either[RepositoryError, Unit]]
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

final case class VectorSearchQuery(
    vector: List[Float],
    filter: JobSearchFilter,
    first: PageSize,
    mode: SearchMode,
    model: String,
    version: Int,
    searchId: UUID
)

final case class RankedJob(job: Job, score: Double, mode: SearchMode, meta: EmbeddingMeta, searchId: UUID)
final case class RankedCandidate(candidate: User, score: Double, mode: SearchMode, meta: EmbeddingMeta, searchId: UUID)

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

final case class JobSearchFilter(
  city: Option[String],
  skills: Set[String],
  createdAfter: Option[Instant]
)

final case class ApplicationCursor(createdAt: Instant, id: ApplicationId)
final case class JobCursor(createdAt: Instant, id: JobId)
final case class ApplicationEventCursor(occurredAt: Instant, id: ApplicationEventId)

opaque type PageSize = Int
object PageSize {
  val Min: Int = 1
  val Max: Int = 100

  def fromInt(value: Int): ValidatedNel[DomainValidationError, PageSize] =
    if (value >= Min && value <= Max) value.validNel
    else InvalidNumber("pageSize", Min, Max, value).invalidNel

  def next(size: PageSize): PageSize = size + 1

  extension (size: PageSize) def value: Int = size
}

final case class ApplicationPageRequest(
  status: Option[ApplicationStatus],
  cursor: Option[ApplicationCursor],
  pageSize: PageSize
)

final case class JobPageRequest(
  status: Option[com.example.graphQL.cats.domain.model.JobStatus],
  cursor: Option[JobCursor],
  pageSize: PageSize
)

final case class ApplicationEventPageRequest(
  cursor: Option[ApplicationEventCursor],
  pageSize: PageSize
)
