package com.example.graphQL.cats.shared.events

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import io.circe.Json
import munit.FunSuite

import java.time.Instant
import java.util.UUID

class OperationalEventJsonSpec extends FunSuite {
  private val eventId = UUID.fromString("00000000-0000-0000-0000-000000000201")
  private val actorId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000202"))

  test("v1 envelope fixture decodes unchanged") {
    val fixture =
      """{
        |  "eventId": "00000000-0000-0000-0000-000000000201",
        |  "eventType": "APPLICATION_STATUS_CHANGED",
        |  "schemaVersion": 1,
        |  "occurredAt": "2026-09-19T10:15:30Z",
        |  "aggregateType": "Application",
        |  "aggregateId": "application-1",
        |  "aggregateVersion": 7,
        |  "sequence": 7,
        |  "actorId": "00000000-0000-0000-0000-000000000202",
        |  "payload": {
        |    "applicationId": "application-1",
        |    "previousStatus": "Interview",
        |    "newStatus": "Hired"
        |  }
        |}""".stripMargin

    assertEquals(
      OperationalEventJson.decode(fixture.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
      Right(OperationalEventEnvelope(
        eventId,
        OperationalEventType.APPLICATION_STATUS_CHANGED,
        OperationalEventEnvelope.SchemaVersion,
        Instant.parse("2026-09-19T10:15:30Z"),
        OperationalAggregateType.Application,
        "application-1",
        7L,
        7L,
        actorId,
        Json.obj(
          "applicationId" -> Json.fromString("application-1"),
          "previousStatus" -> Json.fromString("Interview"),
          "newStatus" -> Json.fromString("Hired")
        )
      ))
    )
  }

  test("unknown schema versions are rejected without reinterpretation") {
    val unsupported =
      """{
        |  "eventId": "00000000-0000-0000-0000-000000000201",
        |  "eventType": "APPLICATION_STATUS_CHANGED",
        |  "schemaVersion": 99,
        |  "occurredAt": "2026-09-19T10:15:30Z",
        |  "aggregateType": "Application",
        |  "aggregateId": "application-1",
        |  "aggregateVersion": 7,
        |  "sequence": 7,
        |  "actorId": "00000000-0000-0000-0000-000000000202",
        |  "payload": {}
        |}""".stripMargin

    assertEquals(OperationalEventJson.decode(unsupported.getBytes(java.nio.charset.StandardCharsets.UTF_8)), Left("UnsupportedVersion"))
  }
}
