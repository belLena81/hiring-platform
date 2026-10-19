package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.UserCursor
import com.example.graphQL.cats.domain.pagination.{ApplicationCursor, ApplicationEventCursor, JobCursor}
import com.example.graphQL.cats.service.search.NearbyJobCursor
import munit.CatsEffectSuite

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.{Base64, UUID}

final class CursorCodecSpec extends CatsEffectSuite {
  private val secret = "test-cursor-secret-01234567890123456789"
  private val ttlSeconds = 60L
  private val defaultKey = CursorCodec.keyFromSecret(secret, ttlSeconds)
  private given CursorCodec.CursorKey = defaultKey
  private val issuedAt = Instant.parse("2026-09-17T08:00:00Z")
  private val id = UUID.fromString("10000000-0000-0000-0000-000000000001")

  test("all cursor types round-trip through signed opaque cursors") {
    assertEquals(
      CursorCodec.decode[JobCursor](CursorCodec.encode(JobCursor(issuedAt, JobId(id)), issuedAt), issuedAt),
      Right(JobCursor(issuedAt, JobId(id)))
    )
    assertEquals(
      CursorCodec.decode[ApplicationCursor](
        CursorCodec.encode(ApplicationCursor(issuedAt, ApplicationId(id)), issuedAt),
        issuedAt
      ),
      Right(ApplicationCursor(issuedAt, ApplicationId(id)))
    )
    assertEquals(
      CursorCodec.decode[ApplicationEventCursor](
        CursorCodec.encode(ApplicationEventCursor(issuedAt, ApplicationEventId(id)), issuedAt),
        issuedAt
      ),
      Right(ApplicationEventCursor(issuedAt, ApplicationEventId(id)))
    )
    assertEquals(
      CursorCodec.decode[UserCursor](CursorCodec.encode(UserCursor(issuedAt, UserId(id)), issuedAt), issuedAt),
      Right(UserCursor(issuedAt, UserId(id)))
    )
  }

  test("a cursor key remains safe for concurrent signing and verification") {
    val cursor = JobCursor(issuedAt, JobId(id))
    val expected = CursorCodec.encode(cursor, issuedAt)

    List
      .fill(256)(())
      .parTraverse { _ =>
        IO.cede *> IO {
          val encoded = CursorCodec.encode(cursor, issuedAt)
          (encoded, CursorCodec.decode[JobCursor](encoded, issuedAt))
        }
      }
      .map { results =>
        assert(results.forall { case (encoded, decoded) => encoded == expected && decoded == Right(cursor) })
      }
  }

  test("a cursor is two url-safe segments: the keyset payload with its expiry, and its HMAC") {
    val encoded = CursorCodec.encode(JobCursor(issuedAt, JobId(id)), issuedAt)
    val parts = encoded.split("\\.", -1)

    assertEquals(parts.length, 2)
    assert(encoded.matches("[A-Za-z0-9_.-]+"), encoded)
    assertEquals(
      new String(Base64.getUrlDecoder.decode(parts(0)), StandardCharsets.UTF_8),
      s"j|$issuedAt|$id|${issuedAt.plusSeconds(ttlSeconds).getEpochSecond}"
    )
    assert(encoded.length < 200, encoded.length)
  }

  test("the default lifetime outlasts a paused client") {
    assertEquals(CursorCodec.DefaultTtlSeconds, 86400L)
    val key = CursorCodec.keyFromSecret(secret)
    assertEquals(key.ttlSeconds, 86400L)
    val encoded = CursorCodec.encode(JobCursor(issuedAt, JobId(id)), issuedAt)(using summon, key)
    assert(
      CursorCodec
        .decode[JobCursor](encoded, issuedAt.plusSeconds(3600))(using summon, key)
        .isRight
    )
  }

  test("expired cursors are rejected, and the last valid second is accepted") {
    val encoded = CursorCodec.encode(JobCursor(issuedAt, JobId(id)), issuedAt)

    assert(CursorCodec.decode[JobCursor](encoded, issuedAt.plusSeconds(ttlSeconds)).isRight)
    assert(CursorCodec.decode[JobCursor](encoded, issuedAt.plusSeconds(ttlSeconds + 1)).isLeft)
  }

  test("cursor kinds remain isolated") {
    val job = CursorCodec.encode(JobCursor(issuedAt, JobId(id)), issuedAt)
    val application = CursorCodec.encode(ApplicationCursor(issuedAt, ApplicationId(id)), issuedAt)
    val event = CursorCodec.encode(ApplicationEventCursor(issuedAt, ApplicationEventId(id)), issuedAt)

    assertEquals(CursorCodec.decode[ApplicationCursor](job, issuedAt), Left(CursorCodec.CursorError.WrongKind("j")))
    assertEquals(CursorCodec.decode[JobCursor](application, issuedAt), Left(CursorCodec.CursorError.WrongKind("a")))
    assertEquals(CursorCodec.decode[JobCursor](event, issuedAt), Left(CursorCodec.CursorError.WrongKind("e")))
  }

  test("malformed, unsigned and foreign tokens do not decode") {
    val unsigned = encodeSegment(s"j|$issuedAt|$id|${issuedAt.plusSeconds(ttlSeconds).getEpochSecond}")
    val forged = unsigned + "." + encodeSegment("not the mac")
    val jwtShaped = "e30.e30.e30"

    List("", "not-base64", ".", "a.b.c", unsigned, forged, jwtShaped).foreach(token =>
      assert(CursorCodec.decode[JobCursor](token, issuedAt).isLeft, token)
    )
  }

  test("payload and signature tampering do not decode") {
    val encoded = CursorCodec.encode(JobCursor(issuedAt, JobId(id)), issuedAt)
    val parts = encoded.split("\\.", -1)
    val otherPayload =
      encodeSegment(s"j|$issuedAt|${UUID.randomUUID()}|${issuedAt.plusSeconds(ttlSeconds).getEpochSecond}")
    val longerLife = encodeSegment(s"j|$issuedAt|$id|${issuedAt.plusSeconds(ttlSeconds * 1000).getEpochSecond}")

    assert(CursorCodec.decode[JobCursor](s"$otherPayload.${parts(1)}", issuedAt).isLeft)
    assert(CursorCodec.decode[JobCursor](s"$longerLife.${parts(1)}", issuedAt.plusSeconds(ttlSeconds + 1)).isLeft)
    assert(CursorCodec.decode[JobCursor](s"${parts(0)}.${parts(1).reverse}", issuedAt).isLeft)
    assert(CursorCodec.decode[JobCursor](s"${parts(0)}.", issuedAt).isLeft)
  }

  test("wrong signing keys are rejected") {
    val encoded = CursorCodec.encode(JobCursor(issuedAt, JobId(id)), issuedAt)(using
      summon[CursorCodec.Keyed[JobCursor]],
      defaultKey
    )
    given otherKey: CursorCodec.CursorKey =
      CursorCodec.keyFromSecret("other-cursor-secret-01234567890123456789", ttlSeconds)

    assert(
      CursorCodec.decode[JobCursor](encoded, issuedAt)(using summon[CursorCodec.Keyed[JobCursor]], otherKey).isLeft
    )
  }

  private def encodeSegment(value: String): String =
    Base64.getUrlEncoder.withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8))

  test("nearby cursors use the signed codec with their own kind and reject other connections") {
    val nearby = NearbyJobCursor(1.234567890123456d, JobId(id), "fingerprint")
    val encoded = CursorCodec.encode(nearby, issuedAt)
    assertEquals(CursorCodec.decode[NearbyJobCursor](encoded, issuedAt), Right(nearby))
    assertEquals(CursorCodec.decode[JobCursor](encoded, issuedAt), Left(CursorCodec.CursorError.WrongKind("n")))
    val job = CursorCodec.encode(JobCursor(issuedAt, JobId(id)), issuedAt)
    assertEquals(CursorCodec.decode[NearbyJobCursor](job, issuedAt), Left(CursorCodec.CursorError.WrongKind("j")))
    val nan = CursorCodec.encode(NearbyJobCursor(Double.NaN, JobId(id), "fingerprint"), issuedAt)
    assert(CursorCodec.decode[NearbyJobCursor](nan, issuedAt).exists(!_.hasValidDistance))
    assert(CursorCodec.decode[NearbyJobCursor](encoded, issuedAt.plusSeconds(ttlSeconds + 1)).isLeft)
  }

  test("a correctly signed cursor with a malformed keyset is reported as malformed, never decoded") {
    val expiry = issuedAt.plusSeconds(ttlSeconds).getEpochSecond
    def signed(payload: String): String = {
      val body = encodeSegment(s"$payload|$expiry")
      body + "." + Base64.getUrlEncoder.withoutPadding().encodeToString(defaultKey.sign(body))
    }
    List(s"n|NaN-text|$id|fp", "n|1.0|not-a-uuid|fp", "n|1.0").foreach { payload =>
      assert(
        CursorCodec
          .decode[NearbyJobCursor](signed(payload), issuedAt)
          .left
          .exists(_.isInstanceOf[CursorCodec.CursorError.Malformed]),
        payload
      )
    }
    assert(CursorCodec.decode[JobCursor](signed(s"j|not-a-time|$id"), issuedAt).isLeft)
    assert(CursorCodec.decode[JobCursor](signed(s"x|$issuedAt|$id"), issuedAt).isLeft)
  }

  test("truncated cursors do not decode") {
    val encoded = CursorCodec.encode(JobCursor(issuedAt, JobId(id)), issuedAt)
    val dot = encoded.indexOf('.')
    List(encoded.dropRight(5), encoded.take(dot), encoded.take(dot / 2), encoded.drop(dot), encoded.dropRight(1))
      .foreach(token => assert(CursorCodec.decode[JobCursor](token, issuedAt).isLeft, token))
  }

  test("a scoped cursor round-trips with its scope and keeps its kind") {
    val scoped = CursorCodec.Scoped("job-applications:x:any", ApplicationCursor(issuedAt, ApplicationId(id)))
    val encoded = CursorCodec.encode(scoped, issuedAt)
    assertEquals(CursorCodec.decode[CursorCodec.Scoped[ApplicationCursor]](encoded, issuedAt), Right(scoped))
    assertEquals(
      CursorCodec.decode[CursorCodec.Scoped[JobCursor]](encoded, issuedAt),
      Left(CursorCodec.CursorError.WrongKind("a"))
    )
  }
}
