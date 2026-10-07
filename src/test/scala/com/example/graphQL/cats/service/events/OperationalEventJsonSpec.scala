package com.example.graphQL.cats.service.events

import com.example.graphQL.cats.domain.model.{ApplicationStatus, JobStatus}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import io.circe.Json
import munit.FunSuite
import java.time.Instant
import java.util.UUID

class OperationalEventJsonSpec extends FunSuite {
  import OperationalEventPayload.*
  private val eventId = new UUID(0L, 201L)
  private val actor = UserId(new UUID(0L, 202L))
  private val applicationId = new UUID(0L, 203L)
  private val jobId = new UUID(0L, 204L)
  private val searchId = new UUID(0L, 205L)
  private val now = Instant.parse("2026-09-19T10:15:30Z")
  private def envelope(payload: OperationalEventPayload): OperationalEventEnvelope =
    OperationalEventEnvelope.fromPayload(eventId, now, actor, payload)
  private val statusEvent = envelope(
    StatusChanged(applicationId, actor.value, jobId, Some(ApplicationStatus.Interview), ApplicationStatus.Hired)
  )

  test("Kafka tombstones and null JSON return a malformed outcome") {
    assertEquals(OperationalEventJson.decode(null: Array[Byte]), Left("MalformedEnvelope"))
    assertEquals(OperationalEventJson.decode(null: Json), Left("MalformedEnvelope"))
    assertEquals(OperationalEventJson.decode(Json.Null), Left("MalformedEnvelope"))
  }

  test("raw adapter DTOs with null fields return typed errors") {
    val invalid = List(
      null,
      statusEvent.copy(eventId = null),
      statusEvent.copy(eventType = null),
      statusEvent.copy(occurredAt = null),
      statusEvent.copy(aggregateType = null),
      statusEvent.copy(aggregateId = null),
      statusEvent.copy(actorId = null.asInstanceOf[UserId]),
      statusEvent.copy(payload = null)
    )
    invalid.foreach(value =>
      assertEquals(OperationalEventJson.validate(value), Left(OperationalEventContractError.MalformedEnvelope))
    )
  }

  test("all nine fact types retain exactly seven envelope fields and round-trip") {
    val payloads = List(
      JobFact(JobKind.Created, JobSnapshot(jobId, List("Scala"), JobStatus.Open)),
      JobFact(JobKind.Updated, JobSnapshot(jobId, List("Scala"), JobStatus.Open)),
      JobFact(JobKind.Closed, JobSnapshot(jobId, List("Scala"), JobStatus.Closed)),
      ApplicationCreated(applicationId, actor.value, jobId, ApplicationStatus.Created),
      StatusChanged(applicationId, actor.value, jobId, Some(ApplicationStatus.Interview), ApplicationStatus.Hired),
      CandidateHired(applicationId, actor.value, jobId, ApplicationStatus.Hired),
      SearchPerformed(searchId, SearchKind.Jobs, List(SearchSessionResult(jobId.toString, 1, 0.95))),
      JobViewed(Some(searchId), jobId, None, Some(1)),
      SearchResultClicked(searchId, jobId, SearchKind.Jobs, 1)
    )
    payloads.foreach { payload =>
      val event = envelope(payload)
      assertEquals(
        OperationalEventJson.json(event).asObject.map(_.keys.toSet),
        Some(Set("eventId", "eventType", "occurredAt", "aggregateType", "aggregateId", "actorId", "payload"))
      )
      assertEquals(
        OperationalEventJson
          .encode(event)
          .flatMap(bytes =>
            OperationalEventJson.decode(bytes).left.map(_ => OperationalEventContractError.MalformedEnvelope)
          ),
        Right(event)
      )
    }
  }

  test("minimal facts exclude job free text, lifecycle notes and search query/filter/model") {
    assertEquals(
      statusEvent.payload.asObject.map(_.keys.toSet),
      Some(Set("applicationId", "candidateId", "jobId", "previousStatus", "newStatus"))
    )
    val session = SearchSession(
      searchId,
      actor,
      "jobs",
      Some("private query"),
      Json.obj("private" -> Json.fromString("data")),
      Some("private-model"),
      List(SearchSessionResult(jobId.toString, 1, 0.5)),
      now,
      now.plusSeconds(60)
    )
    val event = OperationalEvents.searchPerformed(eventId, session).fold(error => fail(error.toString), identity)
    assertEquals(event.payload.asObject.map(_.keys.toSet), Some(Set("searchId", "searchKind", "results")))
    assert(!OperationalEventJson.json(event).noSpaces.contains("private"))
    assertEquals(OperationalEventJson.validate(event), Right(event))
  }

  test("JOB_VIEWED preserves Search aggregate keyed by job and nullable interaction fields") {
    val event = OperationalEvents.jobViewed(
      eventId,
      com.example.graphQL.cats.domain.model.Identifiers.JobId(jobId),
      actor,
      None,
      None,
      now
    )
    assertEquals(event.aggregateType, OperationalAggregateType.Search)
    assertEquals(event.aggregateId, jobId.toString)
    assertEquals(
      event.payload,
      Json.obj(
        "searchId" -> Json.Null,
        "resultId" -> Json.fromString(jobId.toString),
        "searchKind" -> Json.Null,
        "rank" -> Json.Null
      )
    )
    assertEquals(OperationalEventJson.validate(event), Right(event))
  }

  test("missing, extra, sensitive and mismatched payload fields are rejected") {
    val malformed = List(
      statusEvent.copy(payload = statusEvent.payload.mapObject(_.remove("candidateId"))),
      statusEvent.copy(payload = statusEvent.payload.mapObject(_.add("feedback", Json.fromString("secret")))),
      statusEvent.copy(payload = statusEvent.payload.mapObject(_.add("newStatus", Json.fromString("Unknown")))),
      statusEvent.copy(aggregateId = jobId.toString),
      statusEvent.copy(aggregateType = OperationalAggregateType.Job),
      statusEvent.copy(eventType = OperationalEventType.APPLICATION_CREATED),
      statusEvent.copy(payload = Json.Null)
    )
    malformed.foreach(event => assert(OperationalEventJson.validate(event).isLeft))
    assertEquals(
      OperationalEventJson.validate(statusEvent.copy(aggregateId = jobId.toString)),
      Left(OperationalEventContractError.AggregateMismatch)
    )
  }

  test("wire UUIDs require full groups and optional fields cannot coerce malformed values to absence") {
    val json = OperationalEventJson.json(statusEvent)
    assert(OperationalEventJson.decode(json.mapObject(_.add("eventId", Json.fromString("1-1-1-1-1")))).isLeft)
    assert(OperationalEventJson.decode(json.mapObject(_.add("actorId", Json.fromString("1-1-1-1-1")))).isLeft)
    assert(
      OperationalEventJson
        .validate(
          statusEvent.copy(payload = statusEvent.payload.mapObject(_.add("candidateId", Json.fromString("1-1-1-1-1"))))
        )
        .isLeft
    )
    assert(
      OperationalEventJson
        .validate(statusEvent.copy(payload = statusEvent.payload.mapObject(_.add("previousStatus", Json.obj()))))
        .isLeft
    )
    val upper = statusEvent.copy(payload =
      statusEvent.payload.mapObject(_.add("applicationId", Json.fromString(applicationId.toString.toUpperCase)))
    )
    assertEquals(OperationalEventJson.validate(upper), Right(upper))
  }

  test("search results require finite scores, unique UUID identities and consecutive bounded ranks") {
    val invalid = List(
      List(SearchSessionResult(jobId.toString, 1, Double.NaN)),
      List(SearchSessionResult(jobId.toString, 1, Double.PositiveInfinity)),
      List(SearchSessionResult("invalid", 1, 0.1)),
      List(SearchSessionResult(jobId.toString, 0, 0.1)),
      List(SearchSessionResult(jobId.toString, 2, 0.1)),
      List(SearchSessionResult(jobId.toString, 1, 0.1), SearchSessionResult(jobId.toString, 2, 0.2))
    )
    invalid.foreach(results =>
      assert(OperationalEventJson.validate(envelope(SearchPerformed(searchId, SearchKind.Jobs, results))).isLeft)
    )
    assert(OperationalEventJson.validate(envelope(SearchResultClicked(searchId, jobId, SearchKind.Jobs, 101))).isLeft)
  }

  test("fact factories return typed validation failures for unsupported inputs") {
    val session = SearchSession(searchId, actor, "unknown", None, Json.obj(), None, Nil, now, now.plusSeconds(60))
    assertEquals(
      OperationalEvents.searchPerformed(eventId, session),
      Left(OperationalEventContractError.InvalidPayload)
    )
    assertEquals(
      OperationalEvents.searchResultClicked(eventId, searchId, "bad", "jobs", actor, 1, now),
      Left(OperationalEventContractError.InvalidPayload)
    )
    assertEquals(
      OperationalEvents.searchResultClicked(eventId, searchId, jobId.toString, "jobs", actor, 0, now),
      Left(OperationalEventContractError.InvalidPayload)
    )
  }

  test("payload rank and score strings, numeric identifiers and wrong search identity are rejected") {
    val good = envelope(SearchPerformed(searchId, SearchKind.Jobs, List(SearchSessionResult(jobId.toString, 1, 0.5))))
    def resultField(name: String, value: Json) = good.copy(payload =
      good.payload.mapObject(fields =>
        fields.add("results", Json.arr(fields("results").get.asArray.get.head.mapObject(_.add(name, value))))
      )
    )
    assert(OperationalEventJson.validate(resultField("score", Json.fromString("0.5"))).isLeft)
    assert(OperationalEventJson.validate(resultField("rank", Json.fromString("1"))).isLeft)
    assert(OperationalEventJson.validate(resultField("rank", Json.fromDoubleOrNull(1.0))).isLeft)
    assert(
      OperationalEventJson
        .validate(statusEvent.copy(payload = statusEvent.payload.mapObject(_.add("candidateId", Json.fromInt(1)))))
        .isLeft
    )
    assert(
      OperationalEventJson
        .validate(good.copy(payload = good.payload.mapObject(_.add("searchId", Json.fromString(jobId.toString)))))
        .isLeft
    )
  }

  test("candidate search payloads retain every deletion subject") {
    val result = new UUID(0L, 300L)
    val payload =
      SearchPerformed(searchId, SearchKind.CandidateMatches, List(SearchSessionResult(result.toString, 1, 0.5)))
    assertEquals(OperationalEventPayload.decode(envelope(payload)).flatMap(_.subjectIds), Right(List(result)))
    assertEquals(
      OperationalEventPayload
        .decode(envelope(SearchResultClicked(searchId, result, SearchKind.CandidateMatches, 1)))
        .flatMap(_.subjectIds),
      Right(List(result))
    )
  }

  test("maximum escaped and Unicode job skills fit the byte contract without tightening domain limits") {
    val skills = (1 to 100).toList.map(index => index.toString + ("\u0001" * (255 - index.toString.length)) + "x")
    val event = envelope(JobFact(JobKind.Created, JobSnapshot(jobId, skills, JobStatus.Open)))
    assertEquals(OperationalEventJson.validate(event), Right(event))
    assert(OperationalEventJson.bytes(event).length > 60 * 1024)
    assert(OperationalEventJson.bytes(event).length < OperationalEventJson.MaxEnvelopeBytes)
    val unicode = envelope(
      JobFact(
        JobKind.Created,
        JobSnapshot(jobId, (1 to 100).toList.map(index => index.toString + ("界" * 250)), JobStatus.Open)
      )
    )
    assertEquals(OperationalEventJson.validate(unicode), Right(unicode))
  }

  test("malformed UTF-8 is rejected without replacing skill text") {
    val event = envelope(JobFact(JobKind.Created, JobSnapshot(jobId, List("Scala"), JobStatus.Open)))
    val bytes = OperationalEventJson.bytes(event)
    val damaged = bytes.clone()
    damaged(bytes.indexOf('S'.toByte, bytes.indexOf('s'.toByte))) = 0xff.toByte
    assertEquals(OperationalEventJson.decode(damaged), Left("MalformedEnvelope"))
  }

  test("serialized inbound byte bound is exact and checked before JSON parsing") {
    val bytes = OperationalEventJson.bytes(statusEvent)
    val exact = bytes ++ Array.fill[Byte](OperationalEventJson.MaxEnvelopeBytes - bytes.length)(' '.toByte)
    assertEquals(OperationalEventJson.decode(exact), Right(statusEvent))
    assertEquals(OperationalEventJson.decode(exact ++ Array(' '.toByte)), Left("EnvelopeTooLarge"))
  }

  test("versioned or malformed envelope fields remain rejected") {
    val json = OperationalEventJson.json(statusEvent)
    val invalid = List(
      json.mapObject(_.add("schemaVersion", Json.fromInt(99))),
      json.mapObject(_.add("eventId", Json.fromString("invalid"))),
      json.mapObject(_.add("eventType", Json.fromString("UNKNOWN"))),
      json.mapObject(_.add("occurredAt", Json.fromString("invalid"))),
      json.mapObject(_.add("aggregateType", Json.fromString("Unknown"))),
      json.mapObject(_.add("actorId", Json.fromString("invalid"))),
      json.mapObject(_.remove("payload"))
    )
    invalid.foreach(value => assertEquals(OperationalEventJson.decode(value), Left("MalformedEnvelope")))
  }
}
