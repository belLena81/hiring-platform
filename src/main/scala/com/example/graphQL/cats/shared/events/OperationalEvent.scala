package com.example.graphQL.cats.shared.events

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.domain.model.{Application, ApplicationEvent, ApplicationStatus, Job}
import io.circe.Json
import io.circe.parser.parse
import java.time.Instant
import java.nio.charset.StandardCharsets
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
    schemaVersion: Int,
    occurredAt: Instant,
    aggregateType: OperationalAggregateType,
    aggregateId: String,
    aggregateVersion: Long,
    sequence: Long,
    actorId: UserId,
    payload: Json
) {
  def partitionKey: String =
    aggregateId
}

object OperationalEventEnvelope {
  val SchemaVersion: Int = 1
  val Topic: String = "hiring.operational-events.v1"
}

object OperationalEventJson {
  def json(value: OperationalEventEnvelope): Json =
    Json.obj(
      "eventId" -> Json.fromString(value.eventId.toString),
      "eventType" -> Json.fromString(value.eventType.toString),
      "schemaVersion" -> Json.fromInt(value.schemaVersion),
      "occurredAt" -> Json.fromString(value.occurredAt.toString),
      "aggregateType" -> Json.fromString(value.aggregateType.toString),
      "aggregateId" -> Json.fromString(value.aggregateId),
      "aggregateVersion" -> Json.fromLong(value.aggregateVersion),
      "sequence" -> Json.fromLong(value.sequence),
      "actorId" -> Json.fromString(value.actorId.value.toString),
      "payload" -> value.payload
    )

  def bytes(value: OperationalEventEnvelope): Array[Byte] =
    json(value).noSpaces.getBytes(StandardCharsets.UTF_8)

  def decode(bytes: Array[Byte]): Either[String, OperationalEventEnvelope] =
    parse(new String(bytes, StandardCharsets.UTF_8)).leftMap(_ => "MalformedEnvelope").flatMap(decode)

  def decode(json: Json): Either[String, OperationalEventEnvelope] = {
    val cursor = json.hcursor
    for {
      schemaVersion <- cursor.get[Int]("schemaVersion").leftMap(_ => "MalformedEnvelope")
      _ <- Either.cond(schemaVersion == OperationalEventEnvelope.SchemaVersion, (), "UnsupportedVersion")
      eventId <- cursor.get[String]("eventId").flatMap(value => Either.catchNonFatal(UUID.fromString(value)).leftMap(_ => io.circe.DecodingFailure("eventId", Nil))).leftMap(_ => "MalformedEnvelope")
      eventTypeText <- cursor.get[String]("eventType").leftMap(_ => "MalformedEnvelope")
      eventType <- OperationalEventType.values.find(_.toString == eventTypeText).toRight("MalformedEnvelope")
      occurredAtText <- cursor.get[String]("occurredAt").leftMap(_ => "MalformedEnvelope")
      occurredAt <- Either.catchNonFatal(Instant.parse(occurredAtText)).leftMap(_ => "MalformedEnvelope")
      aggregateTypeText <- cursor.get[String]("aggregateType").leftMap(_ => "MalformedEnvelope")
      aggregateType <- OperationalAggregateType.values.find(_.toString == aggregateTypeText).toRight("MalformedEnvelope")
      aggregateId <- cursor.get[String]("aggregateId").leftMap(_ => "MalformedEnvelope")
      aggregateVersion <- cursor.get[Long]("aggregateVersion").leftMap(_ => "MalformedEnvelope")
      sequence <- cursor.get[Long]("sequence").leftMap(_ => "MalformedEnvelope")
      actorId <- cursor.get[String]("actorId").flatMap(value => Either.catchNonFatal(UserId(UUID.fromString(value))).leftMap(_ => io.circe.DecodingFailure("actorId", Nil))).leftMap(_ => "MalformedEnvelope")
      payload <- cursor.get[Json]("payload").leftMap(_ => "MalformedEnvelope")
    } yield OperationalEventEnvelope(eventId, eventType, schemaVersion, occurredAt, aggregateType, aggregateId, aggregateVersion, sequence, actorId, payload)
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
    searchKind: String,
    query: Option[String],
    filter: Json,
    model: Option[String],
    modelVersion: Option[Int],
    results: List[SearchSessionResult],
    occurredAt: Instant,
    expiresAt: Instant
)

object OperationalEvents {
  def jobSnapshot(job: Job): Json =
    Json.obj(
      "jobId" -> Json.fromString(job.id.value.toString),
      "title" -> Json.fromString(job.title),
      "description" -> Json.fromString(job.description),
      "requirements" -> Json.arr(job.requirements.map(Json.fromString)*),
      "skills" -> Json.arr(job.skills.toList.sorted.map(Json.fromString)*),
      "location" -> Json.obj(
        "country" -> Json.fromString(job.location.country),
        "city" -> Json.fromString(job.location.city),
        "remote" -> Json.fromBoolean(job.location.remote)
      ),
      "status" -> Json.fromString(job.status.toString),
      "createdAt" -> Json.fromString(job.createdAt.toString),
      "updatedAt" -> Json.fromString(job.updatedAt.toString),
      "closedAt" -> job.closedAt.fold(Json.Null)(instant => Json.fromString(instant.toString))
    )

  def jobEvent(eventType: OperationalEventType, eventId: UUID, job: Job, actorId: UserId, occurredAt: Instant): OperationalEventEnvelope =
    OperationalEventEnvelope(
      eventId,
      eventType,
      OperationalEventEnvelope.SchemaVersion,
      occurredAt,
      OperationalAggregateType.Job,
      job.id.value.toString,
      job.version,
      job.version,
      actorId,
      Json.obj("job" -> jobSnapshot(job))
    )

  def applicationCreated(eventId: UUID, application: Application, actorId: UserId, occurredAt: Instant): OperationalEventEnvelope =
    OperationalEventEnvelope(
      eventId,
      OperationalEventType.APPLICATION_CREATED,
      OperationalEventEnvelope.SchemaVersion,
      occurredAt,
      OperationalAggregateType.Application,
      application.id.value.toString,
      application.version,
      application.version,
      actorId,
      Json.obj(
        "applicationId" -> Json.fromString(application.id.value.toString),
        "candidateId" -> Json.fromString(application.candidateId.value.toString),
        "jobId" -> Json.fromString(application.jobId.value.toString),
        "status" -> Json.fromString(application.status.toString)
      )
    )

  def statusChanged(eventId: UUID, application: Application, event: ApplicationEvent): OperationalEventEnvelope =
    OperationalEventEnvelope(
      eventId,
      OperationalEventType.APPLICATION_STATUS_CHANGED,
      OperationalEventEnvelope.SchemaVersion,
      event.occurredAt,
      OperationalAggregateType.Application,
      application.id.value.toString,
      application.version,
      application.version,
      event.actorId,
      Json.obj(
        "applicationId" -> Json.fromString(application.id.value.toString),
        "candidateId" -> Json.fromString(application.candidateId.value.toString),
        "jobId" -> Json.fromString(application.jobId.value.toString),
        "previousStatus" -> event.previousStatus.fold(Json.Null)(status => Json.fromString(status.toString)),
        "newStatus" -> Json.fromString(event.newStatus.toString),
        "feedback" -> event.feedback.fold(Json.Null)(Json.fromString),
        "reason" -> event.reason.fold(Json.Null)(Json.fromString)
      )
    )

  def candidateHired(eventId: UUID, application: Application, event: ApplicationEvent): OperationalEventEnvelope =
    statusChanged(eventId, application, event).copy(
      eventType = OperationalEventType.CANDIDATE_HIRED,
      sequence = application.version + 1L,
      payload = Json.obj(
        "applicationId" -> Json.fromString(application.id.value.toString),
        "candidateId" -> Json.fromString(application.candidateId.value.toString),
        "jobId" -> Json.fromString(application.jobId.value.toString),
        "status" -> Json.fromString(ApplicationStatus.Hired.toString)
      )
    )

  def searchPerformed(eventId: UUID, session: SearchSession): OperationalEventEnvelope =
    OperationalEventEnvelope(
      eventId,
      OperationalEventType.SEARCH_PERFORMED,
      OperationalEventEnvelope.SchemaVersion,
      session.occurredAt,
      OperationalAggregateType.Search,
      session.id.toString,
      1L,
      1L,
      session.actorId,
      Json.obj(
        "searchId" -> Json.fromString(session.id.toString),
        "searchKind" -> Json.fromString(session.searchKind),
        "query" -> session.query.fold(Json.Null)(Json.fromString),
        "filter" -> session.filter,
        "model" -> session.model.fold(Json.Null)(Json.fromString),
        "modelVersion" -> session.modelVersion.fold(Json.Null)(Json.fromInt),
        "results" -> Json.arr(session.results.map(result =>
          Json.obj(
            "resultId" -> Json.fromString(result.resultId),
            "rank" -> Json.fromInt(result.rank),
            "score" -> Json.fromDoubleOrNull(result.score)
          )
        )*)
      )
    )

  def jobViewed(eventId: UUID, jobId: JobId, actorId: UserId, searchId: Option[UUID], rank: Option[Int], occurredAt: Instant): OperationalEventEnvelope =
    interaction(eventId, OperationalEventType.JOB_VIEWED, jobId.value.toString, actorId, searchId, jobId.value.toString, rank, occurredAt)

  def searchResultClicked(eventId: UUID, searchId: UUID, resultId: String, actorId: UserId, rank: Int, occurredAt: Instant): OperationalEventEnvelope =
    interaction(eventId, OperationalEventType.SEARCH_RESULT_CLICKED, searchId.toString, actorId, Some(searchId), resultId, Some(rank), occurredAt)

  private def interaction(
      eventId: UUID,
      eventType: OperationalEventType,
      aggregateId: String,
      actorId: UserId,
      searchId: Option[UUID],
      resultId: String,
      rank: Option[Int],
      occurredAt: Instant
  ): OperationalEventEnvelope =
    OperationalEventEnvelope(
      eventId,
      eventType,
      OperationalEventEnvelope.SchemaVersion,
      occurredAt,
      OperationalAggregateType.Search,
      aggregateId,
      1L,
      1L,
      actorId,
      Json.obj(
        "searchId" -> searchId.fold(Json.Null)(id => Json.fromString(id.toString)),
        "resultId" -> Json.fromString(resultId),
        "rank" -> rank.fold(Json.Null)(Json.fromInt)
      )
    )
}
