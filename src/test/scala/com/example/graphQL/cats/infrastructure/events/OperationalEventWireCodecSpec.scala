package com.example.graphQL.cats.infrastructure.events

import com.example.graphQL.cats.domain.model.{ApplicationStatus, JobStatus}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.events.*
import io.circe.Json
import munit.FunSuite
import java.time.Instant
import java.util.UUID

class OperationalEventWireCodecSpec extends FunSuite {
  import OperationalEventPayload.*
  private val Codec = OperationalEventWireCodec
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

  private def payloadOf(event: OperationalEventEnvelope): Json = Codec.payloadJson(event.payload)
  private def withPayload(event: OperationalEventEnvelope, change: Json => Json) =
    Codec.decode(Codec.json(event).mapObject(_.add("payload", change(payloadOf(event)))))

  test("Kafka tombstones and null JSON return a malformed outcome") {
    assertEquals(Codec.decode(null: Array[Byte]), Left("MalformedEnvelope"))
    assertEquals(Codec.decode(null: Json), Left("MalformedEnvelope"))
    assertEquals(Codec.decode(Json.Null), Left("MalformedEnvelope"))
  }

  test("all ten fact types retain exactly seven envelope fields and round-trip") {
    val payloads = List(
      JobFact(JobKind.Created, JobSnapshot(jobId, List("Scala"), JobStatus.Open)),
      JobFact(JobKind.Updated, JobSnapshot(jobId, List("Scala"), JobStatus.Open)),
      JobFact(JobKind.Closed, JobSnapshot(jobId, List("Scala"), JobStatus.Closed)),
      ApplicationCreated(applicationId, actor.value, jobId, ApplicationStatus.Created),
      StatusChanged(applicationId, actor.value, jobId, Some(ApplicationStatus.Interview), ApplicationStatus.Hired),
      CandidateHired(applicationId, actor.value, jobId, ApplicationStatus.Hired),
      SearchPerformed(searchId, SearchKind.Jobs, List(SearchSessionResult(jobId.toString, 1, 0.95))),
      JobViewed(Some(searchId), jobId, None, Some(1)),
      SearchResultClicked(searchId, jobId, SearchKind.Jobs, 1),
      SearchRecordingFailed(searchId, SearchKind.SemanticJobSearch, "UNAVAILABLE")
    )
    payloads.foreach { payload =>
      val event = envelope(payload)
      assertEquals(
        Codec.json(event).asObject.map(_.keys.toSet),
        Some(Set("eventId", "eventType", "occurredAt", "aggregateType", "aggregateId", "actorId", "payload"))
      )
      assertEquals(
        Codec
          .encode(event)
          .flatMap(bytes => Codec.decode(bytes).left.map(_ => OperationalEventContractError.MalformedEnvelope)),
        Right(event)
      )
    }
  }

  test("AIR-04.3 a search recording failure carries only the search id, kind and a bounded failure code") {
    val event = OperationalEvents
      .searchRecordingFailed(eventId, searchId, SearchKind.Jobs, actor, "java.lang.RuntimeException", now)
      .fold(error => fail(error.toString), identity)
    assertEquals(payloadOf(event).asObject.map(_.keys.toSet), Some(Set("searchId", "searchKind", "failure")))
    assertEquals(event.aggregateType, OperationalAggregateType.Search)
    assertEquals(event.aggregateId, searchId.toString)
    List("", "free text with spaces", "a" * 101, "query=secret").foreach { failure =>
      assert(
        OperationalEvents.searchRecordingFailed(eventId, searchId, SearchKind.Jobs, actor, failure, now).isLeft,
        clue(failure)
      )
    }
  }

  test("minimal facts exclude job free text, lifecycle notes and search query/filter/model") {
    assertEquals(
      payloadOf(statusEvent).asObject.map(_.keys.toSet),
      Some(Set("applicationId", "candidateId", "jobId", "previousStatus", "newStatus"))
    )
    val session = SearchSession(
      searchId,
      actor,
      SearchKind.Jobs,
      Some("private query"),
      SearchFilterSnapshot.ForJob(JobId(jobId)),
      Some("private-model"),
      List(SearchSessionResult(jobId.toString, 1, 0.5)),
      now,
      now.plusSeconds(60)
    )
    val event = OperationalEvents.searchPerformed(eventId, session).fold(error => fail(error.toString), identity)
    assertEquals(payloadOf(event).asObject.map(_.keys.toSet), Some(Set("searchId", "searchKind", "results")))
    assert(!Codec.json(event).noSpaces.contains("private"))
    assertEquals(Codec.validate(event), Right(event))
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
      payloadOf(event),
      Json.obj(
        "searchId" -> Json.Null,
        "resultId" -> Json.fromString(jobId.toString),
        "searchKind" -> Json.Null,
        "rank" -> Json.Null
      )
    )
    assertEquals(Codec.validate(event), Right(event))
  }

  test("missing, extra, sensitive and mismatched payload fields are rejected") {
    val malformed = List(
      withPayload(statusEvent, _.mapObject(_.remove("candidateId"))),
      withPayload(statusEvent, _.mapObject(_.add("feedback", Json.fromString("secret")))),
      withPayload(statusEvent, _.mapObject(_.add("newStatus", Json.fromString("Unknown")))),
      withPayload(statusEvent, _ => Json.Null),
      Codec.validate(statusEvent.copy(aggregateId = jobId.toString)),
      Codec.validate(statusEvent.copy(aggregateType = OperationalAggregateType.Job)),
      Codec.validate(statusEvent.copy(eventType = OperationalEventType.APPLICATION_CREATED))
    )
    malformed.foreach(result => assert(result.isLeft))
    assertEquals(
      Codec.validate(statusEvent.copy(aggregateId = jobId.toString)),
      Left(OperationalEventContractError.AggregateMismatch)
    )
  }

  test("wire UUIDs require full groups and optional fields cannot coerce malformed values to absence") {
    val json = Codec.json(statusEvent)
    assert(Codec.decode(json.mapObject(_.add("eventId", Json.fromString("1-1-1-1-1")))).isLeft)
    assert(Codec.decode(json.mapObject(_.add("actorId", Json.fromString("1-1-1-1-1")))).isLeft)
    assert(withPayload(statusEvent, _.mapObject(_.add("candidateId", Json.fromString("1-1-1-1-1")))).isLeft)
    assert(withPayload(statusEvent, _.mapObject(_.add("previousStatus", Json.obj()))).isLeft)
    assertEquals(
      withPayload(
        statusEvent,
        _.mapObject(_.add("applicationId", Json.fromString(applicationId.toString.toUpperCase)))
      ),
      Right(statusEvent)
    )
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
      assert(Codec.validate(envelope(SearchPerformed(searchId, SearchKind.Jobs, results))).isLeft)
    )
    assert(Codec.validate(envelope(SearchResultClicked(searchId, jobId, SearchKind.Jobs, 101))).isLeft)
  }

  test("fact factories return typed validation failures for unsupported inputs") {
    val tooMany = (1 to 101).toList.map(rank => SearchSessionResult(UUID.randomUUID().toString, rank, 0.5))
    val session =
      SearchSession(
        searchId,
        actor,
        SearchKind.Jobs,
        None,
        SearchFilterSnapshot.Unfiltered,
        None,
        tooMany,
        now,
        now.plusSeconds(60)
      )
    assertEquals(
      OperationalEvents.searchPerformed(eventId, session),
      Left(OperationalEventContractError.InvalidPayload)
    )
    assertEquals(
      OperationalEvents.searchResultClicked(eventId, searchId, "bad", SearchKind.Jobs, actor, 1, now),
      Left(OperationalEventContractError.InvalidPayload)
    )
    assertEquals(
      OperationalEvents.searchResultClicked(eventId, searchId, jobId.toString, SearchKind.Jobs, actor, 0, now),
      Left(OperationalEventContractError.InvalidPayload)
    )
  }

  test("payload rank and score strings, numeric identifiers and wrong search identity are rejected") {
    val good = envelope(SearchPerformed(searchId, SearchKind.Jobs, List(SearchSessionResult(jobId.toString, 1, 0.5))))
    def resultField(name: String, value: Json) = withPayload(
      good,
      _.mapObject(fields =>
        fields.add("results", Json.arr(fields("results").get.asArray.get.head.mapObject(_.add(name, value))))
      )
    )
    assert(resultField("score", Json.fromString("0.5")).isLeft)
    assert(resultField("rank", Json.fromString("1")).isLeft)
    assert(resultField("rank", Json.fromDoubleOrNull(1.0)).isLeft)
    assert(withPayload(statusEvent, _.mapObject(_.add("candidateId", Json.fromInt(1)))).isLeft)
    assert(withPayload(good, _.mapObject(_.add("searchId", Json.fromString(jobId.toString)))).isLeft)
  }

  test("candidate search payloads retain every deletion subject") {
    val result = new UUID(0L, 300L)
    val payload =
      SearchPerformed(searchId, SearchKind.CandidateMatches, List(SearchSessionResult(result.toString, 1, 0.5)))
    assertEquals(payload.subjectIds, Right(List(result)))
    assertEquals(
      SearchResultClicked(searchId, result, SearchKind.CandidateMatches, 1).subjectIds,
      Right(List(result))
    )
  }

  test("maximum escaped and Unicode job skills fit the byte contract without tightening domain limits") {
    val skills = (1 to 100).toList.map(index => index.toString + ("\u0001" * (255 - index.toString.length)) + "x")
    val event = envelope(JobFact(JobKind.Created, JobSnapshot(jobId, skills, JobStatus.Open)))
    assertEquals(Codec.validate(event), Right(event))
    assert(Codec.bytes(event).length > 60 * 1024)
    assert(Codec.bytes(event).length < Codec.MaxEnvelopeBytes)
    val unicode = envelope(
      JobFact(
        JobKind.Created,
        JobSnapshot(jobId, (1 to 100).toList.map(index => index.toString + ("界" * 250)), JobStatus.Open)
      )
    )
    assertEquals(Codec.validate(unicode), Right(unicode))
  }

  test("malformed UTF-8 is rejected without replacing skill text") {
    val event = envelope(JobFact(JobKind.Created, JobSnapshot(jobId, List("Scala"), JobStatus.Open)))
    val bytes = Codec.bytes(event)
    val damaged = bytes.clone()
    damaged(bytes.indexOf('S'.toByte, bytes.indexOf('s'.toByte))) = 0xff.toByte
    assertEquals(Codec.decode(damaged), Left("MalformedEnvelope"))
  }

  test("serialized inbound byte bound is exact and checked before JSON parsing") {
    val bytes = Codec.bytes(statusEvent)
    val exact = bytes ++ Array.fill[Byte](Codec.MaxEnvelopeBytes - bytes.length)(' '.toByte)
    assertEquals(Codec.decode(exact), Right(statusEvent))
    assertEquals(Codec.decode(exact ++ Array(' '.toByte)), Left("EnvelopeTooLarge"))
  }

  test("write-side validation refuses an envelope above the byte budget") {
    val size = Codec.bytes(statusEvent).length
    assertEquals(Codec.validate(statusEvent, maxBytes = size), Right(statusEvent))
    assertEquals(Codec.validate(statusEvent, maxBytes = size - 1), Left(OperationalEventContractError.EnvelopeTooLarge))
    assertEquals(Codec.encode(statusEvent).map(_.length), Right(size))
  }

  test("versioned or malformed envelope fields remain rejected") {
    val json = Codec.json(statusEvent)
    val invalid = List(
      json.mapObject(_.add("schemaVersion", Json.fromInt(99))),
      json.mapObject(_.add("eventId", Json.fromString("invalid"))),
      json.mapObject(_.add("eventType", Json.fromString("UNKNOWN"))),
      json.mapObject(_.add("occurredAt", Json.fromString("invalid"))),
      json.mapObject(_.add("aggregateType", Json.fromString("Unknown"))),
      json.mapObject(_.add("actorId", Json.fromString("invalid"))),
      json.mapObject(_.remove("payload"))
    )
    invalid.foreach(value => assertEquals(Codec.decode(value), Left("MalformedEnvelope")))
  }
}
