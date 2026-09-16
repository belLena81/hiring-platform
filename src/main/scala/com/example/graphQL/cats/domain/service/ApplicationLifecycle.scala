package com.example.graphQL.cats.domain.service

import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.error.DomainError.{
  DeclineReasonRequired, InvalidStatusTransition, RejectionFeedbackRequired
}
import com.example.graphQL.cats.domain.model.Application
import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.model.ApplicationStatus.{Accepted, Created, Declined, Hired, Interview, Rejected}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import java.time.Instant

object ApplicationLifecycle {
  def canTransition(from: ApplicationStatus, to: ApplicationStatus): Boolean =
    permitted.get(from).exists(_.contains(to))

  def changeStatus(
      application: Application,
      target: ApplicationStatus,
      actorId: UserId,
      now: Instant,
      feedback: Option[String],
      reason: Option[String]
  ): Either[DomainError, StatusChange] =
    for {
      _ <- Either.cond(canTransition(application.status, target), (), InvalidStatusTransition(application.status, target))
      _ <- Either.cond(target != Rejected || feedback.exists(_.trim.nonEmpty), (), RejectionFeedbackRequired)
      _ <- Either.cond(target != Declined || reason.exists(_.trim.nonEmpty), (), DeclineReasonRequired)
    } yield StatusChange(
      application.copy(status = target, updatedAt = now),
      previousStatus = application.status,
      newStatus = target,
      actorId = actorId,
      occurredAt = now,
      feedback = feedback.map(_.trim).filter(_.nonEmpty),
      reason = reason.map(_.trim).filter(_.nonEmpty)
    )

  private val permitted: Map[ApplicationStatus, Set[ApplicationStatus]] = Map(
    Created -> Set(Accepted, Declined, Rejected),
    Accepted -> Set(Interview),
    Interview -> Set(Hired, Rejected),
    Declined -> Set.empty,
    Hired -> Set.empty,
    Rejected -> Set.empty
  )
}

final case class StatusChange(
  application: Application,
  previousStatus: ApplicationStatus,
  newStatus: ApplicationStatus,
  actorId: UserId,
  occurredAt: Instant,
  feedback: Option[String],
  reason: Option[String]
)
