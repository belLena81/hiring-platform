package com.example.graphQL.cats.infrastructure.events

import com.example.graphQL.cats.domain.model.{ApplicationStatus, JobStatus}
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.service.events.*
import io.circe.Json
import munit.FunSuite
import java.time.Instant
import java.util.UUID

class OperationalEventWireEvolutionSpec extends FunSuite {
  import OperationalEventPayload.*
  private val Codec = OperationalEventWireCodec
  private val eventId = new UUID(0L, 401L)
  private val actor = UserId(new UUID(0L, 402L))
  private val applicationId = new UUID(0L, 403L)
  private val jobId = new UUID(0L, 404L)
  private val searchId = new UUID(0L, 405L)
  private val now = Instant.parse("2026-09-19T10:15:30Z")
  private def envelope(payload: OperationalEventPayload) =
    OperationalEventEnvelope.fromPayload(eventId, now, actor, payload)

  private val statusEvent = envelope(
    StatusChanged(applicationId, actor.value, jobId, Some(ApplicationStatus.Interview), ApplicationStatus.Hired)
  )
  private val jobEvent = envelope(JobFact(JobKind.Created, JobSnapshot(jobId, List("Scala"), JobStatus.Open)))
  private val searchEvent =
    envelope(SearchPerformed(searchId, SearchKind.Jobs, List(SearchSessionResult(jobId.toString, 1, 0.5))))
  private val extra = Json.fromString("later")

  private val everyEvent = List(
    envelope(ApplicationCreated(applicationId, actor.value, jobId, ApplicationStatus.Created)),
    statusEvent,
    envelope(CandidateHired(applicationId, actor.value, jobId, ApplicationStatus.Hired)),
    jobEvent,
    searchEvent,
    envelope(JobViewed(Some(searchId), jobId, None, Some(1))),
    envelope(SearchResultClicked(searchId, jobId, SearchKind.Jobs, 1)),
    envelope(SearchRecordingFailed(searchId, SearchKind.Jobs, "boom"))
  )

  private def mapPayload(event: OperationalEventEnvelope)(change: io.circe.JsonObject => io.circe.JsonObject) =
    Codec.json(event).mapObject(_.add("payload", Codec.payloadJson(event.payload).mapObject(change)))

  test("KFH-03.1 an envelope with an unknown additive envelope field decodes") {
    everyEvent.foreach(event =>
      assertEquals(Codec.decode(Codec.json(event).mapObject(_.add("traceId", extra))), Right(event))
    )
  }

  test("KFH-03.1 an envelope with an unknown additive payload field decodes") {
    everyEvent.foreach(event => assertEquals(Codec.decode(mapPayload(event)(_.add("later", extra))), Right(event)))
  }

  test("KFH-03.1 an unknown additive field in the job snapshot decodes") {
    val json = mapPayload(jobEvent)(fields => fields.add("job", fields("job").get.mapObject(_.add("later", extra))))
    assertEquals(Codec.decode(json), Right(jobEvent))
  }

  test("KFH-03.1 an unknown additive field in a results entry decodes") {
    val json = mapPayload(searchEvent)(fields =>
      fields.add("results", Json.arr(fields("results").get.asArray.get.map(_.mapObject(_.add("later", extra)))*))
    )
    assertEquals(Codec.decode(json), Right(searchEvent))
  }

  test("KFH-03.1 an unknown additive field does not relax typed or semantic checks") {
    assert(
      Codec.decode(mapPayload(statusEvent)(_.add("later", extra).add("newStatus", Json.fromString("Nope")))).isLeft
    )
    assert(
      Codec
        .decode(Codec.json(statusEvent).mapObject(_.add("later", extra).add("eventId", Json.fromString("1-1-1-1-1"))))
        .isLeft
    )
  }

  test("KFH-03.2 an envelope missing a required field is malformed") {
    List("eventId", "eventType", "occurredAt", "aggregateType", "aggregateId", "actorId", "payload").foreach { key =>
      assertEquals(Codec.decode(Codec.json(statusEvent).mapObject(_.remove(key))), Left("MalformedEnvelope"), key)
    }
  }

  test("KFH-03.2 a payload missing a required field is invalid for every payload type, including nullable fields") {
    val required = Map(
      "ApplicationCreated" -> List("applicationId", "candidateId", "jobId", "status"),
      "StatusChanged" -> List("applicationId", "candidateId", "jobId", "previousStatus", "newStatus"),
      "CandidateHired" -> List("applicationId", "candidateId", "jobId", "status"),
      "SearchPerformed" -> List("searchId", "searchKind", "results"),
      "JobViewed" -> List("searchId", "resultId", "searchKind", "rank"),
      "SearchResultClicked" -> List("searchId", "resultId", "searchKind", "rank"),
      "SearchRecordingFailed" -> List("searchId", "searchKind", "failure")
    )
    everyEvent.filterNot(_.payload.isInstanceOf[JobFact]).foreach { event =>
      required(event.payload.getClass.getSimpleName).foreach { key =>
        assert(Codec.decode(mapPayload(event)(_.remove(key))).isLeft, s"${event.eventType} without $key")
      }
    }
    assert(Codec.decode(mapPayload(jobEvent)(_.remove("job"))).isLeft)
    List("jobId", "skills", "status").foreach { key =>
      val json = mapPayload(jobEvent)(fields => fields.add("job", fields("job").get.mapObject(_.remove(key))))
      assert(Codec.decode(json).isLeft, key)
    }
    List("resultId", "rank", "score").foreach { key =>
      val json = mapPayload(searchEvent)(fields =>
        fields.add("results", Json.arr(fields("results").get.asArray.get.map(_.mapObject(_.remove(key)))*))
      )
      assert(Codec.decode(json).isLeft, key)
    }
  }

  test("KFH-03.6 the encoder emits exactly the seven envelope keys and the known payload keys") {
    val envelopeKeys = Set("eventId", "eventType", "occurredAt", "aggregateType", "aggregateId", "actorId", "payload")
    val payloadKeys: Map[String, Set[String]] = Map(
      "ApplicationCreated" -> Set("applicationId", "candidateId", "jobId", "status"),
      "StatusChanged" -> Set("applicationId", "candidateId", "jobId", "previousStatus", "newStatus"),
      "CandidateHired" -> Set("applicationId", "candidateId", "jobId", "status"),
      "JobFact" -> Set("job"),
      "SearchPerformed" -> Set("searchId", "searchKind", "results"),
      "JobViewed" -> Set("searchId", "resultId", "searchKind", "rank"),
      "SearchResultClicked" -> Set("searchId", "resultId", "searchKind", "rank"),
      "SearchRecordingFailed" -> Set("searchId", "searchKind", "failure")
    )
    everyEvent.foreach { event =>
      val json = Codec.json(event)
      assertEquals(json.asObject.map(_.keys.toSet), Some(envelopeKeys))
      assertEquals(
        json.hcursor.downField("payload").focus.flatMap(_.asObject).map(_.keys.toSet),
        Some(payloadKeys(event.payload.getClass.getSimpleName))
      )
    }
    assertEquals(
      Codec.json(jobEvent).hcursor.downField("payload").downField("job").keys.map(_.toSet),
      Some(Set("jobId", "skills", "status"))
    )
    assertEquals(
      Codec.json(searchEvent).hcursor.downField("payload").downField("results").downArray.keys.map(_.toSet),
      Some(Set("resultId", "rank", "score"))
    )
  }

  private def decodeText(text: String) = Codec.decode(text.getBytes("UTF-8"))

  test("KFH-03.2 a repeated key at envelope/payload/job/result level is malformed") {
    val malformed = Left("MalformedEnvelope")
    val status = Codec.json(statusEvent).noSpaces
    val job = Codec.json(jobEvent).noSpaces
    val search = Codec.json(searchEvent).noSpaces
    assertEquals(decodeText(status), Right(statusEvent))
    assertEquals(decodeText(status.dropRight(1) + s""","actorId":"${actor.value}"}"""), malformed)
    assertEquals(
      decodeText(status.replace(""""newStatus":"Hired"""", """"newStatus":"Hired","newStatus":"Hired"""")),
      malformed
    )
    assertEquals(decodeText(job.replace(""""status":"Open"""", """"status":"Open","status":"Open"""")), malformed)
    assertEquals(decodeText(search.replace(""""rank":1""", """"rank":1,"rank":1""")), malformed)
    // escaped spelling of the same key still counts as a repeat; the same key in sibling objects does not
    assertEquals(
      decodeText(status.replace(""""newStatus":"Hired"""", """"newStatus":"Hired","newStatus":"Hired"""")),
      malformed
    )
    assertEquals(decodeText(search), Right(searchEvent))
  }

  test("KFH-03.5 shared tolerance fixtures decode or are refused as their expected outcome says") {
    val dir = java.nio.file.Paths.get(getClass.getResource("/operational-event-tolerance").toURI)
    val files = java.nio.file.Files.list(dir).toArray.map(_.asInstanceOf[java.nio.file.Path]).toList.sortBy(_.toString)
    assert(files.size >= 11)
    files.foreach { file =>
      val doc = io.circe.parser.parse(java.nio.file.Files.readString(file)).toOption.get
      val expected = doc.hcursor.get[String]("expected").toOption.get
      val text =
        doc.hcursor.get[String]("envelopeRaw").toOption.getOrElse(doc.hcursor.downField("envelope").focus.get.noSpaces)
      val result = Codec.decode(text.getBytes("UTF-8"))
      expected match {
        case "accepted" => assert(result.isRight, s"${file.getFileName}: $result")
        case "rejected" => assert(result.isLeft, s"${file.getFileName}: $result")
        case other      => fail(s"${file.getFileName}: unknown expectation $other")
      }
    }
  }
}
