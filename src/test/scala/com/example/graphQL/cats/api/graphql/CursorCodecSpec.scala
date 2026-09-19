package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.api.graphql.CursorCodec
import com.example.graphQL.cats.shared.pagination.{ApplicationCursor, ApplicationEventCursor, JobCursor}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId}
import io.circe.Json
import munit.FunSuite

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.{Base64, UUID}
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

final class CursorCodecSpec extends FunSuite {
  private val secret = "test-cursor-secret-01234567890123456789"
  private val codec = CursorCodec.fromSecret(secret)
  private val instant = Instant.parse("2026-09-17T08:00:00Z")
  private val id = UUID.fromString("10000000-0000-0000-0000-000000000001")

  test("job cursors only decode as job cursors") {
    val encoded = codec.encodeJob(JobCursor(instant, JobId(id)))

    assertEquals(codec.decodeJob(encoded), Right(JobCursor(instant, JobId(id))))
    assertEquals(codec.decodeApplication(encoded), Left(CursorCodec.CursorError.WrongKind("job")))
    assertEquals(codec.decodeEvent(encoded), Left(CursorCodec.CursorError.WrongKind("job")))
  }

  test("application cursors only decode as application cursors") {
    val encoded = codec.encodeApplication(ApplicationCursor(instant, ApplicationId(id)))

    assertEquals(codec.decodeJob(encoded), Left(CursorCodec.CursorError.WrongKind("application")))
    assertEquals(codec.decodeApplication(encoded), Right(ApplicationCursor(instant, ApplicationId(id))))
    assertEquals(codec.decodeEvent(encoded), Left(CursorCodec.CursorError.WrongKind("application")))
  }

  test("application event cursors only decode as event cursors") {
    val encoded = codec.encodeEvent(ApplicationEventCursor(instant, ApplicationEventId(id)))

    assertEquals(codec.decodeJob(encoded), Left(CursorCodec.CursorError.WrongKind("applicationEvent")))
    assertEquals(codec.decodeApplication(encoded), Left(CursorCodec.CursorError.WrongKind("applicationEvent")))
    assertEquals(codec.decodeEvent(encoded), Right(ApplicationEventCursor(instant, ApplicationEventId(id))))
  }

  test("malformed cursors do not decode") {
    assert(codec.decodeJob("not-base64").isLeft)
    assert(codec.decodeApplication("not-base64").isLeft)
    assert(codec.decodeEvent("not-base64").isLeft)
  }

  test("tampered signed cursors do not decode") {
    val encoded = codec.encodeJob(JobCursor(instant, JobId(id)))
    val tampered = encoded.updated(0, if (encoded.head == 'A') 'B' else 'A')

    assert(codec.decodeJob(tampered).isLeft)
  }

  test("old unsigned cursors do not decode") {
    val legacy = Base64.getUrlEncoder.withoutPadding().encodeToString(Json.obj(
      "v" -> Json.fromInt(1),
      "kind" -> Json.fromString("job"),
      "createdAt" -> Json.fromString(instant.toString),
      "id" -> Json.fromString(id.toString)
    ).noSpaces.getBytes(StandardCharsets.UTF_8))

    assert(codec.decodeJob(legacy).isLeft)
  }

  test("malformed cursor fields do not throw or decode") {
    val badCreatedAt = cursorJson("job", Some("not-an-instant"), None, id.toString)
    val badOccurredAt = cursorJson("applicationEvent", None, Some("not-an-instant"), id.toString)
    val badId = cursorJson("application", Some(instant.toString), None, "not-a-uuid")

    assert(codec.decodeJob(encode(badCreatedAt)).isLeft)
    assert(codec.decodeEvent(encode(badOccurredAt)).isLeft)
    assert(codec.decodeApplication(encode(badId)).isLeft)
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
    val payload = Base64.getUrlEncoder.withoutPadding().encodeToString(json.noSpaces.getBytes(StandardCharsets.UTF_8))
    val signature = Base64.getUrlEncoder.withoutPadding().encodeToString(sign(payload))
    s"$payload.$signature"
  }

  private def sign(payload: String): Array[Byte] = {
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(deriveKey, "HmacSHA256"))
    mac.doFinal(payload.getBytes(StandardCharsets.UTF_8))
  }

  private def deriveKey: Array[Byte] = {
    val mac = Mac.getInstance("HmacSHA256")
    mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"))
    mac.doFinal("hiring-platform:graphql-cursor:v2".getBytes(StandardCharsets.UTF_8))
  }
}
