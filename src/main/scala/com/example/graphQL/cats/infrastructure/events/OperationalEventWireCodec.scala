package com.example.graphQL.cats.infrastructure.events

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.{ApplicationStatus, JobStatus}
import com.example.graphQL.cats.service.events.*
import com.example.graphQL.cats.service.events.OperationalEventPayload.*
import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import io.circe.generic.semiauto.*
import io.circe.parser.parse
import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.time.Instant
import java.util.UUID

/** The operational event wire contract: the seven-field envelope and the payload shapes as JSON bytes. Semantic rules
  * live in [[OperationalEventContract]]; this codec adds the structural key sets and the envelope size budget.
  */
object OperationalEventWireCodec {
  // Wire contract budget, independently below the broker's framed-record limit.
  val MaxEnvelopeBytes: Int = 256 * 1024

  /** The semantic contract plus the size budget, for callers that are about to store or publish the bytes. The budget
    * is a parameter so a test can exercise the oversize outcome, which the field limits keep out of reach at the
    * contract maximum.
    */
  def validate(
      value: OperationalEventEnvelope,
      maxBytes: Int = MaxEnvelopeBytes
  ): Either[OperationalEventContractError, OperationalEventEnvelope] =
    OperationalEventContract
      .validate(value)
      .flatMap(valid =>
        Either.cond(bytes(valid).length <= maxBytes, valid, OperationalEventContractError.EnvelopeTooLarge)
      )

  def encode(value: OperationalEventEnvelope): Either[OperationalEventContractError, Array[Byte]] =
    validate(value).map(bytes)

  private final case class WireEnvelope(
      eventId: UUID,
      eventType: OperationalEventType,
      occurredAt: Instant,
      aggregateType: OperationalAggregateType,
      aggregateId: String,
      actorId: UserId,
      payload: Json
  )

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
    OperationalEventContract.fullUuid(raw)
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
    OperationalEventContract.fullUuid(raw).map(UserId.apply)
  }

  private given Encoder[ApplicationStatus] = Encoder.encodeString.contramap(_.toString)
  private given Decoder[ApplicationStatus] =
    Decoder.decodeString.emap(value => ApplicationStatus.values.find(_.toString == value).toRight("invalid status"))
  private given Encoder[JobStatus] = Encoder.encodeString.contramap(_.toString)
  private given Decoder[JobStatus] =
    Decoder.decodeString.emap(value => JobStatus.values.find(_.toString == value).toRight("invalid status"))
  private given Encoder[SearchKind] = Encoder.encodeString.contramap(_.wire)
  private given Decoder[SearchKind] =
    Decoder.decodeString.emap(value => SearchKind.values.find(_.wire == value).toRight("invalid search kind"))
  // Payload numbers keep their JSON types; Circe's default numeric decoders also accept strings.
  private given Decoder[Int] = Decoder.instance(cursor =>
    cursor.value.asNumber
      .filter(_.toString.matches("-?(0|[1-9][0-9]*)"))
      .flatMap(_.toInt)
      .toRight(DecodingFailure("expected integer number", cursor.history))
  )
  private given Decoder[Double] = Decoder.instance(cursor =>
    cursor.value.asNumber
      .map(_.toDouble)
      .filter(_.isFinite)
      .toRight(DecodingFailure("expected finite number", cursor.history))
  )
  private given Encoder[JobSnapshot] = deriveEncoder
  private given Decoder[JobSnapshot] = deriveDecoder
  private given Encoder[SearchSessionResult] = deriveEncoder
  private given Decoder[SearchSessionResult] = deriveDecoder
  private given Encoder[OperationalEventPayload.ApplicationCreated] = deriveEncoder
  private given Decoder[OperationalEventPayload.ApplicationCreated] = deriveDecoder
  private given Encoder[OperationalEventPayload.StatusChanged] = deriveEncoder
  private given Decoder[OperationalEventPayload.StatusChanged] = deriveDecoder
  private given Encoder[OperationalEventPayload.CandidateHired] = deriveEncoder
  private given Decoder[OperationalEventPayload.CandidateHired] = deriveDecoder
  private given Encoder[OperationalEventPayload.SearchPerformed] = deriveEncoder
  private given Decoder[OperationalEventPayload.SearchPerformed] = deriveDecoder
  private given Encoder[OperationalEventPayload.JobViewed] = deriveEncoder
  private given Decoder[OperationalEventPayload.JobViewed] = deriveDecoder
  private given Encoder[OperationalEventPayload.SearchResultClicked] = deriveEncoder
  private given Decoder[OperationalEventPayload.SearchResultClicked] = deriveDecoder
  private given Encoder[OperationalEventPayload.SearchRecordingFailed] = deriveEncoder
  private given Decoder[OperationalEventPayload.SearchRecordingFailed] = deriveDecoder
  private given Encoder[WireEnvelope] = deriveEncoder
  private given Decoder[WireEnvelope] = deriveDecoder

  def payloadJson(value: OperationalEventPayload): Json = value match {
    case OperationalEventPayload.JobFact(_, job)           => Json.obj("job" -> summon[Encoder[JobSnapshot]].apply(job))
    case value: OperationalEventPayload.ApplicationCreated =>
      summon[Encoder[OperationalEventPayload.ApplicationCreated]].apply(value)
    case value: OperationalEventPayload.StatusChanged =>
      summon[Encoder[OperationalEventPayload.StatusChanged]].apply(value)
    case value: OperationalEventPayload.CandidateHired =>
      summon[Encoder[OperationalEventPayload.CandidateHired]].apply(value)
    case value: OperationalEventPayload.SearchPerformed =>
      summon[Encoder[OperationalEventPayload.SearchPerformed]].apply(value)
    case value: OperationalEventPayload.JobViewed =>
      summon[Encoder[OperationalEventPayload.JobViewed]].apply(value)
    case value: OperationalEventPayload.SearchResultClicked =>
      summon[Encoder[OperationalEventPayload.SearchResultClicked]].apply(value)
    case value: OperationalEventPayload.SearchRecordingFailed =>
      summon[Encoder[OperationalEventPayload.SearchRecordingFailed]].apply(value)
  }

  def json(value: OperationalEventEnvelope): Json =
    summon[Encoder[WireEnvelope]].apply(
      WireEnvelope(
        value.eventId,
        value.eventType,
        value.occurredAt,
        value.aggregateType,
        value.aggregateId,
        value.actorId,
        payloadJson(value.payload)
      )
    )

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
      wire <- summon[Decoder[WireEnvelope]].decodeJson(nonNull).leftMap(_ => "MalformedEnvelope")
      payload <- decodePayload(wire.eventType, wire.payload).leftMap(_.toString)
      valid <- OperationalEventWireCodec
        .validate(
          OperationalEventEnvelope(
            wire.eventId,
            wire.eventType,
            wire.occurredAt,
            wire.aggregateType,
            wire.aggregateId,
            wire.actorId,
            payload
          )
        )
        .leftMap(_.toString)
    } yield valid

  private def exactFields(value: Json, names: String*): Boolean =
    Option(value).flatMap(_.asObject).exists(_.keys.toSet == names.toSet)

  private def read[A: Decoder](value: Json, names: String*): Either[OperationalEventContractError, A] =
    Either
      .cond(exactFields(value, names*), (), OperationalEventContractError.InvalidPayload)
      .flatMap(_ => summon[Decoder[A]].decodeJson(value).leftMap(_ => OperationalEventContractError.InvalidPayload))

  /** Reads the payload shape named by the event type; the semantic rules are applied afterwards by the contract. */
  def decodePayload(
      eventType: OperationalEventType,
      value: Json
  ): Either[OperationalEventContractError, OperationalEventPayload] =
    eventType match {
      case OperationalEventType.JOB_CREATED | OperationalEventType.JOB_UPDATED | OperationalEventType.JOB_CLOSED =>
        for {
          _ <- Either.cond(exactFields(value, "job"), (), OperationalEventContractError.InvalidPayload)
          job <- value.hcursor.get[Json]("job").leftMap(_ => OperationalEventContractError.InvalidPayload)
          snapshot <- read[JobSnapshot](job, "jobId", "skills", "status")
        } yield OperationalEventPayload.JobFact(
          eventType match {
            case OperationalEventType.JOB_CREATED => JobKind.Created
            case OperationalEventType.JOB_CLOSED  => JobKind.Closed
            case _                                => JobKind.Updated
          },
          snapshot
        )
      case OperationalEventType.APPLICATION_CREATED =>
        read[OperationalEventPayload.ApplicationCreated](
          value,
          "applicationId",
          "candidateId",
          "jobId",
          "status"
        )
      case OperationalEventType.APPLICATION_STATUS_CHANGED =>
        read[OperationalEventPayload.StatusChanged](
          value,
          "applicationId",
          "candidateId",
          "jobId",
          "previousStatus",
          "newStatus"
        )
      case OperationalEventType.CANDIDATE_HIRED =>
        read[OperationalEventPayload.CandidateHired](value, "applicationId", "candidateId", "jobId", "status")
      case OperationalEventType.SEARCH_PERFORMED =>
        read[OperationalEventPayload.SearchPerformed](value, "searchId", "searchKind", "results").filterOrElse(
          _ =>
            value.hcursor
              .get[List[Json]]("results")
              .toOption
              .exists(_.forall(exactFields(_, "resultId", "rank", "score"))),
          OperationalEventContractError.InvalidPayload
        )
      case OperationalEventType.JOB_VIEWED =>
        read[OperationalEventPayload.JobViewed](value, "searchId", "resultId", "searchKind", "rank")
      case OperationalEventType.SEARCH_RESULT_CLICKED =>
        read[OperationalEventPayload.SearchResultClicked](value, "searchId", "resultId", "searchKind", "rank")
      case OperationalEventType.SEARCH_RECORDING_FAILED =>
        read[OperationalEventPayload.SearchRecordingFailed](value, "searchId", "searchKind", "failure")
    }
}
