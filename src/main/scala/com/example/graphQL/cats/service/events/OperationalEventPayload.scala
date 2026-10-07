package com.example.graphQL.cats.service.events

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{ApplicationStatus, FieldLimits, JobStatus}
import io.circe.{Decoder, DecodingFailure, Encoder, Json}
import io.circe.generic.semiauto.*
import java.util.UUID
import com.example.graphQL.cats.shared.Parsing.parseUuid

enum OperationalEventContractError {
  case MalformedEnvelope, InvalidPayload, AggregateMismatch, EnvelopeTooLarge
}

/** The payload ADT owns business facts; the seven-field envelope is a wire DTO. */
enum OperationalEventPayload {
  import OperationalEventPayload.*
  case JobFact(kind: OperationalEventPayload.JobKind, job: OperationalEventPayload.JobSnapshot)
  case ApplicationCreated(applicationId: UUID, candidateId: UUID, jobId: UUID, status: ApplicationStatus)
  case StatusChanged(
      applicationId: UUID,
      candidateId: UUID,
      jobId: UUID,
      previousStatus: Option[ApplicationStatus],
      newStatus: ApplicationStatus
  )
  case CandidateHired(applicationId: UUID, candidateId: UUID, jobId: UUID, status: ApplicationStatus)
  case SearchPerformed(
      searchId: UUID,
      searchKind: OperationalEventPayload.SearchKind,
      results: List[SearchSessionResult]
  )
  case JobViewed(
      searchId: Option[UUID],
      resultId: UUID,
      searchKind: Option[OperationalEventPayload.SearchKind],
      rank: Option[Int]
  )
  case SearchResultClicked(searchId: UUID, resultId: UUID, searchKind: OperationalEventPayload.SearchKind, rank: Int)

  def subjectIds: Either[OperationalEventContractError, List[UUID]] = this match {
    case ApplicationCreated(_, candidate, _, _)                   => Right(List(candidate))
    case StatusChanged(_, candidate, _, _, _)                     => Right(List(candidate))
    case CandidateHired(_, candidate, _, _)                       => Right(List(candidate))
    case SearchPerformed(_, SearchKind.CandidateMatches, results) =>
      results.traverse(result => parseUuid(result.resultId).leftMap(_ => OperationalEventContractError.InvalidPayload))
    case SearchResultClicked(_, candidate, SearchKind.CandidateMatches, _) => Right(List(candidate))
    case _                                                                 => Right(Nil)
  }
}

object OperationalEventPayload {
  enum JobKind(val eventType: OperationalEventType) {
    case Created extends JobKind(OperationalEventType.JOB_CREATED)
    case Updated extends JobKind(OperationalEventType.JOB_UPDATED)
    case Closed extends JobKind(OperationalEventType.JOB_CLOSED)
  }

  final case class JobSnapshot(jobId: UUID, skills: List[String], status: JobStatus)

  enum SearchKind(val wire: String) {
    case Jobs extends SearchKind("jobs")
    case RecommendedJobs extends SearchKind("recommendedJobs")
    case SemanticJobSearch extends SearchKind("semanticJobSearch")
    case CandidateMatches extends SearchKind("candidateMatches")
  }

  private given Encoder[UUID] = Encoder.encodeString.contramap(_.toString)
  private[events] def fullUuid(value: String): Either[String, UUID] =
    Either
      .cond(
        value.matches("(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"),
        (),
        "invalid identifier"
      )
      .flatMap(_ => parseUuid(value).leftMap(_ => "invalid identifier"))

  private given Decoder[UUID] = Decoder.decodeString.emap(fullUuid)
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
  private given Encoder[ApplicationCreated] = deriveEncoder
  private given Decoder[ApplicationCreated] = deriveDecoder
  private given Encoder[StatusChanged] = deriveEncoder
  private given Decoder[StatusChanged] = deriveDecoder
  private given Encoder[CandidateHired] = deriveEncoder
  private given Decoder[CandidateHired] = deriveDecoder
  private given Encoder[SearchPerformed] = deriveEncoder
  private given Decoder[SearchPerformed] = deriveDecoder
  private given Encoder[JobViewed] = deriveEncoder
  private given Decoder[JobViewed] = deriveDecoder
  private given Encoder[SearchResultClicked] = deriveEncoder
  private given Decoder[SearchResultClicked] = deriveDecoder

  def json(value: OperationalEventPayload): Json = value match {
    case JobFact(_, job)            => Json.obj("job" -> summon[Encoder[JobSnapshot]].apply(job))
    case value: ApplicationCreated  => summon[Encoder[ApplicationCreated]].apply(value)
    case value: StatusChanged       => summon[Encoder[StatusChanged]].apply(value)
    case value: CandidateHired      => summon[Encoder[CandidateHired]].apply(value)
    case value: SearchPerformed     => summon[Encoder[SearchPerformed]].apply(value)
    case value: JobViewed           => summon[Encoder[JobViewed]].apply(value)
    case value: SearchResultClicked => summon[Encoder[SearchResultClicked]].apply(value)
  }

  private def exactFields(value: Json, names: String*): Boolean =
    Option(value).flatMap(_.asObject).exists(_.keys.toSet == names.toSet)

  private def read[A: Decoder](value: Json, names: String*): Either[OperationalEventContractError, A] =
    Either
      .cond(exactFields(value, names*), (), OperationalEventContractError.InvalidPayload)
      .flatMap(_ => summon[Decoder[A]].decodeJson(value).leftMap(_ => OperationalEventContractError.InvalidPayload))

  def decode(event: OperationalEventEnvelope): Either[OperationalEventContractError, OperationalEventPayload] = {
    val value = event.payload
    val decoded: Either[OperationalEventContractError, OperationalEventPayload] = event.eventType match {
      case OperationalEventType.JOB_CREATED | OperationalEventType.JOB_UPDATED | OperationalEventType.JOB_CLOSED =>
        for {
          _ <- Either.cond(exactFields(value, "job"), (), OperationalEventContractError.InvalidPayload)
          job <- value.hcursor.get[Json]("job").leftMap(_ => OperationalEventContractError.InvalidPayload)
          snapshot <- read[JobSnapshot](job, "jobId", "skills", "status")
          _ <- Either.cond(
            snapshot.skills.size <= FieldLimits.CollectionMaxValues &&
              snapshot.skills.distinct.size == snapshot.skills.size &&
              snapshot.skills.forall(skill => skill.trim.nonEmpty && skill.length <= FieldLimits.ShortTextMaxChars) &&
              (event.eventType != OperationalEventType.JOB_CLOSED || snapshot.status == JobStatus.Closed),
            (),
            OperationalEventContractError.InvalidPayload
          )
        } yield JobFact(
          event.eventType match {
            case OperationalEventType.JOB_CREATED => JobKind.Created
            case OperationalEventType.JOB_CLOSED  => JobKind.Closed
            case _                                => JobKind.Updated
          },
          snapshot
        )
      case OperationalEventType.APPLICATION_CREATED =>
        read[ApplicationCreated](value, "applicationId", "candidateId", "jobId", "status")
          .filterOrElse(_.status == ApplicationStatus.Created, OperationalEventContractError.InvalidPayload)
      case OperationalEventType.APPLICATION_STATUS_CHANGED =>
        read[StatusChanged](value, "applicationId", "candidateId", "jobId", "previousStatus", "newStatus")
      case OperationalEventType.CANDIDATE_HIRED =>
        read[CandidateHired](value, "applicationId", "candidateId", "jobId", "status")
          .filterOrElse(_.status == ApplicationStatus.Hired, OperationalEventContractError.InvalidPayload)
      case OperationalEventType.SEARCH_PERFORMED =>
        read[SearchPerformed](value, "searchId", "searchKind", "results").filterOrElse(
          payload =>
            payload.results.size <= FieldLimits.CollectionMaxValues &&
              payload.results.map(_.resultId).distinct.size == payload.results.size &&
              payload.results.map(_.rank) == (1 to payload.results.size).toList &&
              payload.results.forall(result => fullUuid(result.resultId).isRight && result.score.isFinite) &&
              value.hcursor
                .get[List[Json]]("results")
                .toOption
                .exists(_.forall(exactFields(_, "resultId", "rank", "score"))),
          OperationalEventContractError.InvalidPayload
        )
      case OperationalEventType.JOB_VIEWED =>
        read[JobViewed](value, "searchId", "resultId", "searchKind", "rank").filterOrElse(
          payload =>
            payload.searchKind.isEmpty && payload.searchId.isDefined == payload.rank.isDefined &&
              payload.rank.forall(validRank),
          OperationalEventContractError.InvalidPayload
        )
      case OperationalEventType.SEARCH_RESULT_CLICKED =>
        read[SearchResultClicked](value, "searchId", "resultId", "searchKind", "rank")
          .filterOrElse(payload => validRank(payload.rank), OperationalEventContractError.InvalidPayload)
    }
    decoded.flatMap { payload =>
      val (aggregateType, id) = payload match {
        case JobFact(_, job)                  => (OperationalAggregateType.Job, job.jobId)
        case ApplicationCreated(id, _, _, _)  => (OperationalAggregateType.Application, id)
        case StatusChanged(id, _, _, _, _)    => (OperationalAggregateType.Application, id)
        case CandidateHired(id, _, _, _)      => (OperationalAggregateType.Application, id)
        case SearchPerformed(id, _, _)        => (OperationalAggregateType.Search, id)
        case JobViewed(_, id, _, _)           => (OperationalAggregateType.Search, id)
        case SearchResultClicked(id, _, _, _) => (OperationalAggregateType.Search, id)
      }
      Either.cond(
        event.aggregateType == aggregateType && event.aggregateId == id.toString,
        payload,
        OperationalEventContractError.AggregateMismatch
      )
    }
  }

  private def validRank(rank: Int): Boolean = rank >= 1 && rank <= FieldLimits.CollectionMaxValues
}
