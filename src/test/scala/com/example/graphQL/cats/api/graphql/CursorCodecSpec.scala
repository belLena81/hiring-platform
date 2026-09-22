package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.UserCursor
import com.example.graphQL.cats.shared.pagination.{ApplicationCursor, ApplicationEventCursor, JobCursor}
import munit.CatsEffectSuite

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.{Base64, UUID}

final class CursorCodecSpec extends CatsEffectSuite {
  private val secret = "test-cursor-secret-01234567890123456789"
  private given CursorCodec.CursorKey = CursorCodec.keyFromSecret(secret)
  private val instant = Instant.parse("2026-09-17T08:00:00Z")
  private val id = UUID.fromString("10000000-0000-0000-0000-000000000001")

  test("all cursor types round-trip through the compact signed format") {
    assertEquals(CursorCodec.decode[JobCursor](CursorCodec.encode(JobCursor(instant, JobId(id)))), Right(JobCursor(instant, JobId(id))))
    assertEquals(CursorCodec.decode[ApplicationCursor](CursorCodec.encode(ApplicationCursor(instant, ApplicationId(id)))), Right(ApplicationCursor(instant, ApplicationId(id))))
    assertEquals(CursorCodec.decode[ApplicationEventCursor](CursorCodec.encode(ApplicationEventCursor(instant, ApplicationEventId(id)))), Right(ApplicationEventCursor(instant, ApplicationEventId(id))))
    assertEquals(CursorCodec.decode[UserCursor](CursorCodec.encode(UserCursor(instant, UserId(id)))), Right(UserCursor(instant, UserId(id))))
  }

  test("a cursor key remains safe for concurrent signing and verification") {
    val cursor = JobCursor(instant, JobId(id))
    val expected = CursorCodec.encode(cursor)

    List.fill(256)(()).parTraverse { _ =>
      IO.cede *> IO {
        val encoded = CursorCodec.encode(cursor)
        (encoded, CursorCodec.decode[JobCursor](encoded))
      }
    }.map { results =>
      assert(results.forall { case (encoded, decoded) => encoded == expected && decoded == Right(cursor) })
    }
  }

  test("cursors are one compact base64url envelope rather than JWTs") {
    val encoded = CursorCodec.encode(JobCursor(instant, JobId(id)))
    val decoded = new String(Base64.getUrlDecoder.decode(encoded), StandardCharsets.UTF_8)

    assert(!encoded.contains('.'))
    assertEquals(decoded.split("\\|", -1).toList.take(3), List("j", instant.toString, id.toString))
    assertEquals(decoded.split("\\|", -1).length, 4)
  }

  test("cursor kinds remain isolated") {
    val job = CursorCodec.encode(JobCursor(instant, JobId(id)))
    val application = CursorCodec.encode(ApplicationCursor(instant, ApplicationId(id)))
    val event = CursorCodec.encode(ApplicationEventCursor(instant, ApplicationEventId(id)))

    assertEquals(CursorCodec.decode[ApplicationCursor](job), Left(CursorCodec.CursorError.WrongKind("j")))
    assertEquals(CursorCodec.decode[JobCursor](application), Left(CursorCodec.CursorError.WrongKind("a")))
    assertEquals(CursorCodec.decode[JobCursor](event), Left(CursorCodec.CursorError.WrongKind("e")))
  }

  test("malformed, versioned, and JWT-shaped cursors do not decode") {
    assert(CursorCodec.decode[JobCursor]("not-base64").isLeft)
    assert(CursorCodec.decode[JobCursor]("eyJhbGciOiJIUzI1NiJ9.eyJ2IjoyfQ.signature").isLeft)

    val versioned = Base64.getUrlEncoder.withoutPadding().encodeToString(s"v3|j|$instant|$id|AAAAAAAAAAAAAAAAAAAAAA".getBytes(StandardCharsets.UTF_8))
    assert(CursorCodec.decode[JobCursor](versioned).isLeft)
  }

  test("payload and MAC tampering do not decode") {
    val encoded = CursorCodec.encode(JobCursor(instant, JobId(id)))
    val decoded = new String(Base64.getUrlDecoder.decode(encoded), StandardCharsets.UTF_8)
    val fields = decoded.split("\\|", -1)
    val changedPayload = fields.updated(2, UUID.randomUUID().toString).mkString("|")
    val changedMac = fields.updated(3, fields(3).updated(0, if fields(3).head == 'A' then 'B' else 'A')).mkString("|")

    assert(CursorCodec.decode[JobCursor](encodeText(changedPayload)).isLeft)
    assert(CursorCodec.decode[JobCursor](encodeText(changedMac)).isLeft)
  }

  test("invalid field values and versioned payloads are rejected") {
    val invalidTimestamp = encodeText(s"j|not-an-instant|$id|AAAAAAAAAAAAAAAAAAAAAA")
    val invalidId = encodeText(s"j|$instant|not-a-uuid|AAAAAAAAAAAAAAAAAAAAAA")
    val versioned = encodeText(s"v2|j|$instant|$id|AAAAAAAAAAAAAAAAAAAAAA")

    assert(CursorCodec.decode[JobCursor](invalidTimestamp).isLeft)
    assert(CursorCodec.decode[JobCursor](invalidId).isLeft)
    assert(CursorCodec.decode[JobCursor](versioned).isLeft)
  }

  private def encodeText(value: String): String =
    Base64.getUrlEncoder.withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8))
}
