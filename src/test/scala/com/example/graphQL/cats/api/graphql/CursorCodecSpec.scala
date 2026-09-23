package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.UserCursor
import com.example.graphQL.cats.shared.pagination.{ApplicationCursor, ApplicationEventCursor, JobCursor}
import munit.CatsEffectSuite
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtClaim}

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

  test("all cursor types round-trip through expiring JWTs") {
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

  test("cursor values are three-part JWTs carrying the signed keyset subject") {
    val encoded = CursorCodec.encode(JobCursor(issuedAt, JobId(id)), issuedAt)
    val parts = encoded.split("\\.", -1)
    val claim = JwtCirce.parseClaim(new String(Base64.getUrlDecoder.decode(parts(1)), StandardCharsets.UTF_8))

    assertEquals(parts.length, 3)
    assertEquals(claim.subject, Some(s"j|$issuedAt|$id"))
    assert(claim.issuer.nonEmpty)
    assert(claim.audience.nonEmpty)
    assertEquals(claim.issuedAt, Some(issuedAt.getEpochSecond))
    assertEquals(claim.expiration, Some(issuedAt.plusSeconds(ttlSeconds).getEpochSecond))
  }

  test("expired cursors are rejected") {
    val encoded = CursorCodec.encode(JobCursor(issuedAt, JobId(id)), issuedAt)

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

  test("malformed, legacy, and non-cursor JWTs do not decode") {
    val legacy = encodeText(s"j|$issuedAt|$id|AAAAAAAAAAAAAAAAAAAAAA")
    val accessLike = JwtCirce.encode(
      JwtClaim()
        .about(id.toString)
        .by("hiring-platform-local")
        .to("hiring-graphql-api")
        .issuedAt(issuedAt.getEpochSecond)
        .expiresAt(issuedAt.plusSeconds(ttlSeconds).getEpochSecond),
      summon[CursorCodec.CursorKey].secretKey,
      JwtAlgorithm.HS256
    )

    assert(CursorCodec.decode[JobCursor]("not-base64", issuedAt).isLeft)
    assert(CursorCodec.decode[JobCursor](legacy, issuedAt).isLeft)
    assert(CursorCodec.decode[JobCursor](accessLike, issuedAt).isLeft)
  }

  test("payload, signature, issuer, and audience tampering do not decode") {
    val encoded = CursorCodec.encode(JobCursor(issuedAt, JobId(id)), issuedAt)
    val parts = encoded.split("\\.", -1)
    val changedPayload =
      parts.updated(1, encodeSegment(s"""{"sub":"j|$issuedAt|${UUID.randomUUID()}"}""")).mkString(".")
    val changedSignature = parts.updated(2, parts(2).reverse).mkString(".")
    val wrongIssuer = JwtCirce.encode(
      JwtClaim()
        .about(s"j|$issuedAt|$id")
        .by("wrong-issuer")
        .to("hiring-graphql-api")
        .issuedAt(issuedAt.getEpochSecond)
        .expiresAt(issuedAt.plusSeconds(ttlSeconds).getEpochSecond),
      summon[CursorCodec.CursorKey].secretKey,
      JwtAlgorithm.HS256
    )
    val wrongAudience = JwtCirce.encode(
      JwtClaim()
        .about(s"j|$issuedAt|$id")
        .by("hiring-platform-cursor")
        .to("wrong-audience")
        .issuedAt(issuedAt.getEpochSecond)
        .expiresAt(issuedAt.plusSeconds(ttlSeconds).getEpochSecond),
      summon[CursorCodec.CursorKey].secretKey,
      JwtAlgorithm.HS256
    )

    assert(CursorCodec.decode[JobCursor](changedPayload, issuedAt).isLeft)
    assert(CursorCodec.decode[JobCursor](changedSignature, issuedAt).isLeft)
    assert(CursorCodec.decode[JobCursor](wrongIssuer, issuedAt).isLeft)
    assert(CursorCodec.decode[JobCursor](wrongAudience, issuedAt).isLeft)
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

  private def encodeText(value: String): String = encodeSegment(value)

  private def encodeSegment(value: String): String =
    Base64.getUrlEncoder.withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8))
}
