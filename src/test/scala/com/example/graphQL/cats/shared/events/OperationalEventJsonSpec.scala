package com.example.graphQL.cats.shared.events

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import io.circe.Json
import munit.FunSuite

import java.time.Instant
import java.util.UUID

class OperationalEventJsonSpec extends FunSuite {
  private val eventId = UUID.fromString("00000000-0000-0000-0000-000000000201")
  private val actorId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000202"))
  private val envelope = OperationalEventEnvelope(
    eventId,
    OperationalEventType.APPLICATION_STATUS_CHANGED,
    Instant.parse("2026-09-19T10:15:30Z"),
    OperationalAggregateType.Application,
    "application-1",
    actorId,
    Json.obj(
      "applicationId" -> Json.fromString("application-1"),
      "previousStatus" -> Json.fromString("Interview"),
      "newStatus" -> Json.fromString("Hired")
    )
  )

  test("derived codec preserves the unversioned envelope wire shape and round-trips") {
    assertEquals(
      OperationalEventJson.json(envelope),
      Json.obj(
        "eventId" -> Json.fromString(eventId.toString),
        "eventType" -> Json.fromString("APPLICATION_STATUS_CHANGED"),
        "occurredAt" -> Json.fromString("2026-09-19T10:15:30Z"),
        "aggregateType" -> Json.fromString("Application"),
        "aggregateId" -> Json.fromString("application-1"),
        "actorId" -> Json.fromString(actorId.value.toString),
        "payload" -> envelope.payload
      )
    )
    assertEquals(OperationalEventJson.decode(OperationalEventJson.bytes(envelope)), Right(envelope))
  }

  test("unversioned envelope fixture decodes") {
    val fixture =
      """{
        |  "eventId": "00000000-0000-0000-0000-000000000201",
        |  "eventType": "APPLICATION_STATUS_CHANGED",
        |  "occurredAt": "2026-09-19T10:15:30Z",
        |  "aggregateType": "Application",
        |  "aggregateId": "application-1",
        |  "actorId": "00000000-0000-0000-0000-000000000202",
        |  "payload": {
        |    "applicationId": "application-1",
        |    "previousStatus": "Interview",
        |    "newStatus": "Hired"
        |  }
        |}""".stripMargin

    assertEquals(
      OperationalEventJson.decode(fixture.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
      Right(
        OperationalEventEnvelope(
          eventId,
          OperationalEventType.APPLICATION_STATUS_CHANGED,
          Instant.parse("2026-09-19T10:15:30Z"),
          OperationalAggregateType.Application,
          "application-1",
          actorId,
          Json.obj(
            "applicationId" -> Json.fromString("application-1"),
            "previousStatus" -> Json.fromString("Interview"),
            "newStatus" -> Json.fromString("Hired")
          )
        )
      )
    )
  }

  test("versioned envelope fixtures are rejected") {
    val versioned =
      """{
        |  "eventId": "00000000-0000-0000-0000-000000000201",
        |  "eventType": "APPLICATION_STATUS_CHANGED",
        |  "schemaVersion": 99,
        |  "occurredAt": "2026-09-19T10:15:30Z",
        |  "aggregateType": "Application",
        |  "aggregateId": "application-1",
        |  "actorId": "00000000-0000-0000-0000-000000000202",
        |  "payload": {}
        |}""".stripMargin

    assertEquals(
      OperationalEventJson.decode(versioned.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
      Left("MalformedEnvelope")
    )
  }

  test("malformed derived fields are normalized to the envelope error") {
    val malformed = List(
      OperationalEventJson.json(envelope).mapObject(_.add("eventId", Json.fromString("invalid"))),
      OperationalEventJson.json(envelope).mapObject(_.add("eventType", Json.fromString("UNKNOWN"))),
      OperationalEventJson.json(envelope).mapObject(_.add("occurredAt", Json.fromString("invalid"))),
      OperationalEventJson.json(envelope).mapObject(_.add("aggregateType", Json.fromString("Unknown"))),
      OperationalEventJson.json(envelope).mapObject(_.add("actorId", Json.fromString("invalid"))),
      OperationalEventJson.json(envelope).mapObject(_.remove("payload"))
    )

    malformed.foreach(value => assertEquals(OperationalEventJson.decode(value), Left("MalformedEnvelope")))
  }

  test("missing current fields are rejected even when a legacy version field is present") {
    val malformed = OperationalEventJson.json(envelope).mapObject { fields =>
      fields.add("schemaVersion", Json.fromInt(99)).remove("eventId")
    }

    assertEquals(OperationalEventJson.decode(malformed), Left("MalformedEnvelope"))
  }
}
