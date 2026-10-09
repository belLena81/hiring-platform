package com.example.graphQL.cats.domain.pagination

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.domain.error.DomainValidationError.InvalidNumber
import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId}
import java.time.Instant

/** Keyset position `(timestamp, id)` shared by every newest-first connection; `Id` selects the connection. */
final case class TimestampIdCursor[+Id](createdAt: Instant, id: Id) {

  /** Application events order by occurrence time; the keyset timestamp is the same value. */
  def occurredAt: Instant = createdAt
}

type ApplicationCursor = TimestampIdCursor[ApplicationId]
val ApplicationCursor: TimestampIdCursor.type = TimestampIdCursor
type JobCursor = TimestampIdCursor[JobId]
val JobCursor: TimestampIdCursor.type = TimestampIdCursor
type ApplicationEventCursor = TimestampIdCursor[ApplicationEventId]
val ApplicationEventCursor: TimestampIdCursor.type = TimestampIdCursor

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
