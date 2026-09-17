package com.example.graphQL.cats.shared.pagination

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.domain.error.DomainValidationError.InvalidNumber
import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId}
import java.time.Instant

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
