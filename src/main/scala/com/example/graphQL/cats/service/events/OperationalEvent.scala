package com.example.graphQL.cats.service.events

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, Job}
import java.time.Instant
import java.util.UUID

enum OperationalEventType {
  case JOB_CREATED, JOB_UPDATED, JOB_CLOSED, JOB_VIEWED, SEARCH_PERFORMED, SEARCH_RESULT_CLICKED,
    APPLICATION_CREATED, APPLICATION_STATUS_CHANGED, CANDIDATE_HIRED
}

enum OperationalAggregateType {
  case Job, Application, Search
}

final case class OperationalEventEnvelope(
    eventId: UUID,
    eventType: OperationalEventType,
    occurredAt: Instant,
    aggregateType: OperationalAggregateType,
    aggregateId: String,
    actorId: UserId,
    payload: OperationalEventPayload
) {
  def partitionKey: String =
    aggregateId
}

object OperationalEventEnvelope {
  val Topic: String = "hiring.operational-events"

  def fromPayload(
      eventId: UUID,
      occurredAt: Instant,
      actorId: UserId,
      payload: OperationalEventPayload
  ): OperationalEventEnvelope = {
    OperationalEventEnvelope(
      eventId,
      payload.eventType,
      occurredAt,
      payload.aggregateType,
      payload.aggregateId.toString,
      actorId,
      payload
    )
  }
}

final case class SearchSessionResult(
    resultId: String,
    rank: Int,
    score: Double
)

final case class SearchSession(
    id: UUID,
    actorId: UserId,
    searchKind: OperationalEventPayload.SearchKind,
    query: Option[String],
    filter: SearchFilterSnapshot,
    model: Option[String],
    results: List[SearchSessionResult],
    occurredAt: Instant,
    expiresAt: Instant
)

object OperationalEvents {
  import OperationalEventPayload.*

  private def snapshot(job: Job): JobSnapshot =
    JobSnapshot(job.id.value, job.skills.toList.sorted, job.status)

  def jobEvent(
      kind: JobKind,
      eventId: UUID,
      job: Job,
      actorId: UserId,
      occurredAt: Instant
  ): Either[OperationalEventContractError, OperationalEventEnvelope] =
    OperationalEventContract.validate(
      OperationalEventEnvelope.fromPayload(eventId, occurredAt, actorId, JobFact(kind, snapshot(job)))
    )

  def applicationCreated(
      eventId: UUID,
      application: Application,
      actorId: UserId,
      occurredAt: Instant
  ): OperationalEventEnvelope =
    OperationalEventEnvelope.fromPayload(
      eventId,
      occurredAt,
      actorId,
      ApplicationCreated(
        application.id.value,
        application.candidateId.value,
        application.jobId.value,
        application.status
      )
    )

  def statusChanged(eventId: UUID, application: Application, event: ApplicationEvent): OperationalEventEnvelope =
    OperationalEventEnvelope.fromPayload(
      eventId,
      event.occurredAt,
      event.actorId,
      StatusChanged(
        application.id.value,
        application.candidateId.value,
        application.jobId.value,
        event.previousStatus,
        event.newStatus
      )
    )

  def candidateHired(eventId: UUID, application: Application, event: ApplicationEvent): OperationalEventEnvelope =
    OperationalEventEnvelope.fromPayload(
      eventId,
      event.occurredAt,
      event.actorId,
      CandidateHired(
        application.id.value,
        application.candidateId.value,
        application.jobId.value,
        ApplicationStatus.Hired
      )
    )

  def searchPerformed(
      eventId: UUID,
      session: SearchSession
  ): Either[OperationalEventContractError, OperationalEventEnvelope] =
    OperationalEventContract.validate(
      OperationalEventEnvelope.fromPayload(
        eventId,
        session.occurredAt,
        session.actorId,
        SearchPerformed(session.id, session.searchKind, session.results)
      )
    )

  def jobViewed(
      eventId: UUID,
      jobId: JobId,
      actorId: UserId,
      searchId: Option[UUID],
      rank: Option[Int],
      occurredAt: Instant
  ): OperationalEventEnvelope =
    OperationalEventEnvelope.fromPayload(eventId, occurredAt, actorId, JobViewed(searchId, jobId.value, None, rank))

  def searchResultClicked(
      eventId: UUID,
      searchId: UUID,
      resultId: String,
      searchKind: SearchKind,
      actorId: UserId,
      rank: Int,
      occurredAt: Instant
  ): Either[OperationalEventContractError, OperationalEventEnvelope] =
    for {
      id <- OperationalEventContract.fullUuid(resultId).leftMap(_ => OperationalEventContractError.InvalidPayload)
      event <- OperationalEventContract.validate(
        OperationalEventEnvelope.fromPayload(
          eventId,
          occurredAt,
          actorId,
          SearchResultClicked(searchId, id, searchKind, rank)
        )
      )
    } yield event
}
