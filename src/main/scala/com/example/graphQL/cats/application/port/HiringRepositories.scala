package com.example.graphQL.cats.application.port

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.domain.error.DomainValidationError.InvalidNumber
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, Job, User}
import java.time.Instant

trait UserRepository[F[_]] {
  def find(id: UserId): F[Option[User]]
  def findMany(ids: List[UserId]): F[List[User]]
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
  def findByRecruiter(recruiterId: UserId, page: JobPageRequest): F[List[Job]]
  def create(job: Job): F[Either[RepositoryError, Unit]]
  def update(job: Job): F[Either[RepositoryError, Job]]
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
