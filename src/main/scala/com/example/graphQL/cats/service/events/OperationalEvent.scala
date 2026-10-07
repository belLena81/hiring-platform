package com.example.graphQL.cats.service.events

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, Job}
import io.circe.{Decoder, Encoder, Json}
import io.circe.generic.semiauto.*
import io.circe.parser.parse
import java.time.Instant
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.nio.ByteBuffer
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
    payload: Json
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
    import OperationalEventPayload.*
    val (eventType, aggregateType, aggregateId) = payload match {
      case JobFact(kind, job)              => (kind.eventType, OperationalAggregateType.Job, job.jobId)
      case ApplicationCreated(id, _, _, _) =>
        (OperationalEventType.APPLICATION_CREATED, OperationalAggregateType.Application, id)
      case StatusChanged(id, _, _, _, _) =>
        (OperationalEventType.APPLICATION_STATUS_CHANGED, OperationalAggregateType.Application, id)
      case CandidateHired(id, _, _, _) =>
        (OperationalEventType.CANDIDATE_HIRED, OperationalAggregateType.Application, id)
      case SearchPerformed(id, _, _) => (OperationalEventType.SEARCH_PERFORMED, OperationalAggregateType.Search, id)
      case JobViewed(_, id, _, _)    => (OperationalEventType.JOB_VIEWED, OperationalAggregateType.Search, id)
      case SearchResultClicked(id, _, _, _) =>
        (OperationalEventType.SEARCH_RESULT_CLICKED, OperationalAggregateType.Search, id)
    }
    OperationalEventEnvelope(
      eventId,
      eventType,
      occurredAt,
      aggregateType,
      aggregateId.toString,
      actorId,
      OperationalEventPayload.json(payload)
    )
  }
}

object OperationalEventJson {
  // Wire contract budget, independently below the broker's framed-record limit.
  val MaxEnvelopeBytes: Int = 256 * 1024

  def validate(value: OperationalEventEnvelope): Either[OperationalEventContractError, OperationalEventEnvelope] =
    for {
      nonNull <- Option(value).toRight(OperationalEventContractError.MalformedEnvelope)
      _ <- Either.cond(
        Option(nonNull.eventId).nonEmpty && Option(nonNull.occurredAt).nonEmpty &&
          Option(nonNull.actorId).exists(actor => Option(actor.value).nonEmpty) &&
          Option(nonNull.eventType).nonEmpty && Option(nonNull.aggregateType).nonEmpty &&
          Option(nonNull.aggregateId).nonEmpty && Option(nonNull.payload).nonEmpty,
        (),
        OperationalEventContractError.MalformedEnvelope
      )
      _ <- OperationalEventPayload.decode(nonNull)
      _ <- Either.cond(bytes(nonNull).length <= MaxEnvelopeBytes, (), OperationalEventContractError.EnvelopeTooLarge)
    } yield nonNull

  def encode(value: OperationalEventEnvelope): Either[OperationalEventContractError, Array[Byte]] =
    validate(value).map(bytes)
  private val EnvelopeFields = Set(
    "eventId",
    "eventType",
    "occurredAt",
    "aggregateType",
    "aggregateId",
    "actorId",
    "payload"
  )
  private given Encoder[UUID] = Encoder.encodeString.contramap(_.toString)

  private given Decoder[UUID] = Decoder.decodeString.emap { raw =>
    OperationalEventPayload.fullUuid(raw)
  }

  private given Encoder[Instant] = Encoder.encodeString.contramap(_.toString)

  private given Decoder[Instant] = Decoder.decodeString.emap { raw =>
    Either.catchNonFatal(Instant.parse(raw)).left.map(_ => "invalid timestamp")
  }

  private given Encoder[OperationalEventType] = Encoder.encodeString.contramap(_.toString)

  private given Decoder[OperationalEventType] = Decoder.decodeString.emap { raw =>
    OperationalEventType.values.find(_.toString == raw).toRight("invalid event type")
  }

  private given Encoder[OperationalAggregateType] = Encoder.encodeString.contramap(_.toString)

  private given Decoder[OperationalAggregateType] = Decoder.decodeString.emap { raw =>
    OperationalAggregateType.values.find(_.toString == raw).toRight("invalid aggregate type")
  }

  private given Encoder[UserId] = Encoder.encodeString.contramap(_.value.toString)

  private given Decoder[UserId] = Decoder.decodeString.emap { raw =>
    OperationalEventPayload.fullUuid(raw).map(UserId.apply)
  }

  private given Encoder[OperationalEventEnvelope] = deriveEncoder
  private given Decoder[OperationalEventEnvelope] = deriveDecoder

  def json(value: OperationalEventEnvelope): Json =
    summon[Encoder[OperationalEventEnvelope]].apply(value)

  def bytes(value: OperationalEventEnvelope): Array[Byte] =
    json(value).noSpaces.getBytes(StandardCharsets.UTF_8)

  def decode(bytes: Array[Byte]): Either[String, OperationalEventEnvelope] =
    Option(bytes)
      .toRight("MalformedEnvelope")
      .flatMap(value =>
        if (value.length > MaxEnvelopeBytes) Left(OperationalEventContractError.EnvelopeTooLarge.toString)
        else
          Either
            .catchNonFatal(
              StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(value))
                .toString
            )
            .leftMap(_ => "MalformedEnvelope")
            .flatMap(raw => parse(raw).leftMap(_ => "MalformedEnvelope").flatMap(decode))
      )

  def decode(json: Json): Either[String, OperationalEventEnvelope] =
    for {
      nonNull <- Option(json).toRight("MalformedEnvelope")
      fields <- nonNull.asObject.toRight("MalformedEnvelope")
      _ <- Either.cond(fields.keys.toSet == EnvelopeFields, (), "MalformedEnvelope")
      value <- summon[Decoder[OperationalEventEnvelope]].decodeJson(nonNull).leftMap(_ => "MalformedEnvelope")
      valid <- validate(value).leftMap(_.toString)
    } yield valid
}

final case class SearchSessionResult(
    resultId: String,
    rank: Int,
    score: Double
)

final case class SearchSession(
    id: UUID,
    actorId: UserId,
    searchKind: String,
    query: Option[String],
    filter: Json,
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
      eventType: OperationalEventType,
      eventId: UUID,
      job: Job,
      actorId: UserId,
      occurredAt: Instant
  ): Either[OperationalEventContractError, OperationalEventEnvelope] =
    JobKind.values
      .find(_.eventType == eventType)
      .toRight(OperationalEventContractError.InvalidPayload)
      .map(kind => OperationalEventEnvelope.fromPayload(eventId, occurredAt, actorId, JobFact(kind, snapshot(job))))
      .flatMap(OperationalEventJson.validate)

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
    SearchKind.values
      .find(_.wire == session.searchKind)
      .toRight(OperationalEventContractError.InvalidPayload)
      .map(kind =>
        OperationalEventEnvelope
          .fromPayload(eventId, session.occurredAt, session.actorId, SearchPerformed(session.id, kind, session.results))
      )
      .flatMap(OperationalEventJson.validate)

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
      searchKind: String,
      actorId: UserId,
      rank: Int,
      occurredAt: Instant
  ): Either[OperationalEventContractError, OperationalEventEnvelope] =
    for {
      id <- OperationalEventPayload.fullUuid(resultId).leftMap(_ => OperationalEventContractError.InvalidPayload)
      kind <- SearchKind.values.find(_.wire == searchKind).toRight(OperationalEventContractError.InvalidPayload)
      event <- OperationalEventJson.validate(
        OperationalEventEnvelope.fromPayload(
          eventId,
          occurredAt,
          actorId,
          SearchResultClicked(searchId, id, kind, rank)
        )
      )
    } yield event
}
