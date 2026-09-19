package com.example.graphQL.cats.domain.model

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainValidationError
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import java.time.Instant

enum ApplicationStatus {
  case Created, Accepted, Declined, Interview, Hired, Rejected
}

final case class Application(
  id: ApplicationId,
  candidateId: UserId,
  jobId: JobId,
  status: ApplicationStatus,
  createdAt: Instant,
  updatedAt: Instant
)

object Application {
  def create(
      id: ApplicationId,
      candidateId: UserId,
      jobId: JobId,
      now: Instant
  ): Application =
    Application(id, candidateId, jobId, ApplicationStatus.Created, now, now)
}

final case class ApplicationEvent(
  id: ApplicationEventId,
  applicationId: ApplicationId,
  previousStatus: Option[ApplicationStatus],
  newStatus: ApplicationStatus,
  actorId: UserId,
  occurredAt: Instant,
  feedback: Option[String],
  reason: Option[String]
)

object ApplicationEvent {
  def validate(
      id: ApplicationEventId,
      applicationId: ApplicationId,
      previousStatus: Option[ApplicationStatus],
      newStatus: ApplicationStatus,
      actorId: UserId,
      occurredAt: Instant,
      feedback: Option[String],
      reason: Option[String]
  ): ValidatedNel[DomainValidationError, ApplicationEvent] =
    (
      validateOptionalText("feedback", feedback, FieldLimits.LongTextMaxChars),
      validateOptionalText("reason", reason, FieldLimits.LongTextMaxChars)
    ).mapN { (validFeedback, validReason) =>
      ApplicationEvent(id, applicationId, previousStatus, newStatus, actorId, occurredAt, validFeedback, validReason)
    }
}
