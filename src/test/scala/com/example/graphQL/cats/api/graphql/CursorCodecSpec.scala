package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.CursorCodec
import com.example.graphQL.cats.shared.pagination.{ApplicationCursor, ApplicationEventCursor, JobCursor}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId}
import io.circe.Json
import munit.FunSuite

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.{Base64, UUID}

final class CursorCodecSpec extends FunSuite {
  private val instant = Instant.parse("2026-09-17T08:00:00Z")
  private val id = UUID.fromString("10000000-0000-0000-0000-000000000001")

  test("job cursors only decode as job cursors") {
    val encoded = CursorCodec.encodeJob(JobCursor(instant, JobId(id)))

    assertEquals(CursorCodec.decodeJob(encoded), Right(JobCursor(instant, JobId(id))))
    assert(CursorCodec.decodeApplication(encoded).isLeft)
    assert(CursorCodec.decodeEvent(encoded).isLeft)
  }

  test("application cursors only decode as application cursors") {
    val encoded = CursorCodec.encodeApplication(ApplicationCursor(instant, ApplicationId(id)))

    assert(CursorCodec.decodeJob(encoded).isLeft)
    assertEquals(CursorCodec.decodeApplication(encoded), Right(ApplicationCursor(instant, ApplicationId(id))))
    assert(CursorCodec.decodeEvent(encoded).isLeft)
  }

  test("application event cursors only decode as event cursors") {
    val encoded = CursorCodec.encodeEvent(ApplicationEventCursor(instant, ApplicationEventId(id)))

    assert(CursorCodec.decodeJob(encoded).isLeft)
    assert(CursorCodec.decodeApplication(encoded).isLeft)
    assertEquals(CursorCodec.decodeEvent(encoded), Right(ApplicationEventCursor(instant, ApplicationEventId(id))))
  }

  test("malformed cursors do not decode") {
    assert(CursorCodec.decodeJob("not-base64").isLeft)
    assert(CursorCodec.decodeApplication("not-base64").isLeft)
    assert(CursorCodec.decodeEvent("not-base64").isLeft)
  }

  test("malformed cursor fields do not throw or decode") {
    val badCreatedAt = cursorJson("job", Some("not-an-instant"), None, id.toString)
    val badOccurredAt = cursorJson("applicationEvent", None, Some("not-an-instant"), id.toString)
    val badId = cursorJson("application", Some(instant.toString), None, "not-a-uuid")

    assert(CursorCodec.decodeJob(encode(badCreatedAt)).isLeft)
    assert(CursorCodec.decodeEvent(encode(badOccurredAt)).isLeft)
    assert(CursorCodec.decodeApplication(encode(badId)).isLeft)
  }

  private def cursorJson(kind: String, createdAt: Option[String], occurredAt: Option[String], id: String): Json =
    Json.obj(
      "kind" -> Json.fromString(kind),
      "createdAt" -> createdAt.fold(Json.Null)(Json.fromString),
      "occurredAt" -> occurredAt.fold(Json.Null)(Json.fromString),
      "id" -> Json.fromString(id)
    )

  private def encode(json: Json): String =
    Base64.getUrlEncoder.withoutPadding().encodeToString(json.noSpaces.getBytes(StandardCharsets.UTF_8))
}
