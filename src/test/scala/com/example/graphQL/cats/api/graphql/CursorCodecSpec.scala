package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.CursorCodec
import com.example.graphQL.cats.shared.pagination.{ApplicationCursor, ApplicationEventCursor, JobCursor}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.UserCursor
import io.circe.Json
import munit.FunSuite
import pdi.jwt.{JwtAlgorithm, JwtCirce}

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.{Base64, UUID}

final class CursorCodecSpec extends FunSuite {
  private val secret = "test-cursor-secret-01234567890123456789"
  private val codec = CursorCodec.fromSecret(secret)
  private val instant = Instant.parse("2026-09-17T08:00:00Z")
  private val id = UUID.fromString("10000000-0000-0000-0000-000000000001")
  test("job cursors only decode as job cursors") {
    val encoded = codec.jobCursorCodec.encode(JobCursor(instant, JobId(id)))

    assertEquals(codec.jobCursorCodec.decode(encoded), Right(JobCursor(instant, JobId(id))))
    assertEquals(codec.applicationCursorCodec.decode(encoded), Left(CursorCodec.CursorError.WrongKind("job")))
    assertEquals(codec.eventCursorCodec.decode(encoded), Left(CursorCodec.CursorError.WrongKind("job")))
  }

  test("encoded cursors use a signed JWT envelope") {
    val encoded = codec.jobCursorCodec.encode(JobCursor(instant, JobId(id)))

    assertEquals(encoded.split("\\.", -1).length, 3)
  }

  test("application cursors only decode as application cursors") {
    val encoded = codec.applicationCursorCodec.encode(ApplicationCursor(instant, ApplicationId(id)))

    assertEquals(codec.jobCursorCodec.decode(encoded), Left(CursorCodec.CursorError.WrongKind("application")))
    assertEquals(codec.applicationCursorCodec.decode(encoded), Right(ApplicationCursor(instant, ApplicationId(id))))
    assertEquals(codec.eventCursorCodec.decode(encoded), Left(CursorCodec.CursorError.WrongKind("application")))
  }

  test("application event cursors only decode as event cursors") {
    val encoded = codec.eventCursorCodec.encode(ApplicationEventCursor(instant, ApplicationEventId(id)))

    assertEquals(codec.jobCursorCodec.decode(encoded), Left(CursorCodec.CursorError.WrongKind("applicationEvent")))
    assertEquals(codec.applicationCursorCodec.decode(encoded), Left(CursorCodec.CursorError.WrongKind("applicationEvent")))
    assertEquals(codec.eventCursorCodec.decode(encoded), Right(ApplicationEventCursor(instant, ApplicationEventId(id))))
  }

  test("user cursors only decode as user cursors") {
    val expected = UserCursor(instant, UserId(id))
    val encoded = codec.userCursorCodec.encode(expected)

    assertEquals(codec.userCursorCodec.decode(encoded), Right(expected))
    assertEquals(codec.jobCursorCodec.decode(encoded), Left(CursorCodec.CursorError.WrongKind("user")))
  }

  test("malformed cursors do not decode") {
    assert(codec.jobCursorCodec.decode("not-base64").isLeft)
    assert(codec.applicationCursorCodec.decode("not-base64").isLeft)
    assert(codec.eventCursorCodec.decode("not-base64").isLeft)
  }

  test("tampered signed cursors do not decode") {
    val encoded = codec.jobCursorCodec.encode(JobCursor(instant, JobId(id)))
    val tampered = encoded.updated(0, if (encoded.head == 'A') 'B' else 'A')

    assert(codec.jobCursorCodec.decode(tampered).isLeft)
  }

  test("access-token signed JWTs do not decode as cursors") {
    val accessTokenSignedCursor = JwtCirce.encode(cursorJson("job", Some(instant.toString), None, id.toString), secret, JwtAlgorithm.HS256)

    assert(codec.jobCursorCodec.decode(accessTokenSignedCursor).isLeft)
  }

  test("old unsigned cursors do not decode") {
    val legacy = Base64.getUrlEncoder.withoutPadding().encodeToString(Json.obj(
      "v" -> Json.fromInt(1),
      "kind" -> Json.fromString("job"),
      "createdAt" -> Json.fromString(instant.toString),
      "id" -> Json.fromString(id.toString)
    ).noSpaces.getBytes(StandardCharsets.UTF_8))

    assert(codec.jobCursorCodec.decode(legacy).isLeft)
  }

  test("unsupported cursor versions report the received version") {
    val versionOne = Json.obj(
      "v" -> Json.fromInt(1),
      "kind" -> Json.fromString("job"),
      "createdAt" -> Json.fromString(instant.toString),
      "occurredAt" -> Json.Null,
      "id" -> Json.fromString(id.toString)
    )

    codec.jobCursorCodec.decode(encode(versionOne)) match {
      case Left(CursorCodec.CursorError.Malformed(message)) => assert(message.contains("Unsupported cursor version: 1"))
      case other => fail(s"Expected unsupported-version failure, received $other")
    }
  }

  test("old custom signed cursor envelopes do not decode") {
    val payload = Base64.getUrlEncoder.withoutPadding().encodeToString(Json.obj(
      "v" -> Json.fromInt(2),
      "kind" -> Json.fromString("job"),
      "createdAt" -> Json.fromString(instant.toString),
      "occurredAt" -> Json.Null,
      "id" -> Json.fromString(id.toString)
    ).noSpaces.getBytes(StandardCharsets.UTF_8))

    assert(codec.jobCursorCodec.decode(s"$payload.signature").isLeft)
  }

  test("malformed cursor fields do not throw or decode") {
    val badCreatedAt = cursorJson("job", Some("not-an-instant"), None, id.toString)
    val badOccurredAt = cursorJson("applicationEvent", None, Some("not-an-instant"), id.toString)
    val badId = cursorJson("application", Some(instant.toString), None, "not-a-uuid")

    assert(codec.jobCursorCodec.decode(encode(badCreatedAt)).isLeft)
    assert(codec.eventCursorCodec.decode(encode(badOccurredAt)).isLeft)
    assert(codec.applicationCursorCodec.decode(encode(badId)).isLeft)
  }

  private def cursorJson(kind: String, createdAt: Option[String], occurredAt: Option[String], id: String): Json =
    Json.obj(
      "kind" -> Json.fromString(kind),
      "v" -> Json.fromInt(2),
      "createdAt" -> createdAt.fold(Json.Null)(Json.fromString),
      "occurredAt" -> occurredAt.fold(Json.Null)(Json.fromString),
      "id" -> Json.fromString(id)
    )

  private def encode(json: Json): String = {
    JwtCirce.encode(json, secret, JwtAlgorithm.HS256)
  }
}
