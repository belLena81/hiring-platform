package com.example.graphQL.cats.service.events

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{ApplicationStatus, FieldLimits, JobStatus}
import com.example.graphQL.cats.service.events.OperationalEventPayload.*
import com.example.graphQL.cats.shared.Parsing.parseUuid
import java.util.UUID

/** Pure semantic rules every operational event satisfies before it is stored or published. */
object OperationalEventContract {

  /** Canonical textual form only: 8-4-4-4-12 hexadecimal digits, with no lenient shortened forms. */
  def fullUuid(value: String): Either[String, UUID] =
    Either
      .cond(
        value.matches("(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"),
        (),
        "invalid identifier"
      )
      .flatMap(_ => parseUuid(value).leftMap(_ => "invalid identifier"))

  def validate(event: OperationalEventEnvelope): Either[OperationalEventContractError, OperationalEventEnvelope] =
    for {
      _ <- Either.cond(
        event.eventType == event.payload.eventType && validPayload(event.payload),
        (),
        OperationalEventContractError.InvalidPayload
      )
      _ <- Either.cond(
        event.aggregateType == event.payload.aggregateType && event.aggregateId == event.payload.aggregateId.toString,
        (),
        OperationalEventContractError.AggregateMismatch
      )
    } yield event

  private def validPayload(payload: OperationalEventPayload): Boolean = payload match {
    case OperationalEventPayload.JobFact(kind, snapshot) =>
      snapshot.skills.size <= FieldLimits.CollectionMaxValues &&
      snapshot.skills.distinct.size == snapshot.skills.size &&
      snapshot.skills.forall(skill => skill.trim.nonEmpty && skill.length <= FieldLimits.ShortTextMaxChars) &&
      (kind != JobKind.Closed || snapshot.status == JobStatus.Closed)
    case OperationalEventPayload.ApplicationCreated(_, _, _, status) => status == ApplicationStatus.Created
    case _: OperationalEventPayload.StatusChanged                    => true
    case OperationalEventPayload.CandidateHired(_, _, _, status)     => status == ApplicationStatus.Hired
    case OperationalEventPayload.SearchPerformed(_, _, results)      =>
      results.size <= FieldLimits.CollectionMaxValues &&
      results.map(_.resultId).distinct.size == results.size &&
      results.map(_.rank) == (1 to results.size).toList &&
      results.forall(result => fullUuid(result.resultId).isRight && result.score.isFinite)
    case OperationalEventPayload.JobViewed(searchId, _, searchKind, rank) =>
      searchKind.isEmpty && searchId.isDefined == rank.isDefined && rank.forall(validRank)
    case OperationalEventPayload.SearchResultClicked(_, _, _, rank) => validRank(rank)
  }

  private def validRank(rank: Int): Boolean = rank >= 1 && rank <= FieldLimits.CollectionMaxValues
}
