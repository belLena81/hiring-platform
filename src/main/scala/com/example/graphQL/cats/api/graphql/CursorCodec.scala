package com.example.graphQL.cats.api.graphql

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.shared.Parsing
import com.example.graphQL.cats.domain.pagination.TimestampIdCursor
import com.example.graphQL.cats.service.search.NearbyJobCursor
import com.example.graphQL.cats.shared.crypto.{Hmac, HmacJwt}
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtClaim}

import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID

private[cats] object CursorCodec {
  private val KeyDerivationLabel = "hiring-platform:graphql-cursor"
  private val CursorIssuer = "hiring-platform-cursor"
  private val CursorAudience = "hiring-graphql-api"

  final class CursorKey private[graphql] (private val bytes: Array[Byte], val ttlSeconds: Long) {
    private[graphql] val secretKey = Hmac.secretKey(bytes)
  }

  /** Wire shape of one cursor type: the payload is `kind|field|field...` signed inside the JWT subject. */
  trait Keyed[A] {
    def kind: CursorKind
    def fields(value: A): List[String]
    def read(fields: List[String]): Either[CursorError, A]
  }

  enum CursorKind(val tag: String) {
    case Job extends CursorKind("j")
    case Application extends CursorKind("a")
    case Event extends CursorKind("e")
    case User extends CursorKind("u")
    case Nearby extends CursorKind("n")
  }

  enum CursorError {
    case Malformed(message: String)
    case WrongKind(actual: String)
    case CriteriaMismatch
  }

  /** Identifier types usable in a `(timestamp, id)` keyset cursor, with the connection kind they belong to. */
  trait CursorId[I] {
    def kind: CursorKind
    def uuid(id: I): UUID
    def fromUuid(value: UUID): I
  }

  object CursorId {
    private def of[I](cursorKind: CursorKind, toUuid: I => UUID, fromUuidValue: UUID => I): CursorId[I] =
      new CursorId[I] {
        def kind: CursorKind = cursorKind
        def uuid(id: I): UUID = toUuid(id)
        def fromUuid(value: UUID): I = fromUuidValue(value)
      }

    given CursorId[JobId] = of(CursorKind.Job, _.value, JobId.apply)
    given CursorId[ApplicationId] = of(CursorKind.Application, _.value, ApplicationId.apply)
    given CursorId[ApplicationEventId] = of(CursorKind.Event, _.value, ApplicationEventId.apply)
    given CursorId[UserId] = of(CursorKind.User, _.value, UserId.apply)
  }

  given [I](using cursorId: CursorId[I]): Keyed[TimestampIdCursor[I]] with
    def kind: CursorKind = cursorId.kind
    def fields(value: TimestampIdCursor[I]): List[String] =
      List(value.createdAt.toString, cursorId.uuid(value.id).toString)
    def read(fields: List[String]): Either[CursorError, TimestampIdCursor[I]] =
      fields match {
        case List(at, id) =>
          for {
            timestamp <- Either
              .catchNonFatal(Instant.parse(at))
              .left
              .map(_ => CursorError.Malformed("Invalid cursor timestamp"))
            uuid <- Parsing.parseUuid(id).left.map(_ => CursorError.Malformed("Invalid cursor id"))
          } yield TimestampIdCursor(timestamp, cursorId.fromUuid(uuid))
        case _ => Left(CursorError.Malformed("Invalid cursor shape"))
      }

  given Keyed[NearbyJobCursor] with
    def kind: CursorKind = CursorKind.Nearby
    def fields(value: NearbyJobCursor): List[String] =
      List(value.distanceKm.toString, value.jobId.value.toString, value.queryFingerprint)
    def read(fields: List[String]): Either[CursorError, NearbyJobCursor] =
      fields match {
        case List(distance, id, fingerprint) =>
          for {
            km <- Either
              .catchNonFatal(distance.toDouble)
              .left
              .map(_ => CursorError.Malformed("Invalid cursor distance"))
            uuid <- Parsing.parseUuid(id).left.map(_ => CursorError.Malformed("Invalid cursor id"))
          } yield NearbyJobCursor(km, JobId(uuid), fingerprint)
        case _ => Left(CursorError.Malformed("Invalid cursor shape"))
      }

  def keyFromSecret(secret: String, ttlSeconds: Long = 900L): CursorKey = {
    new CursorKey(Hmac.sha256(secret.getBytes(StandardCharsets.UTF_8), KeyDerivationLabel), ttlSeconds)
  }

  def encode[A](value: A, now: Instant)(using keyed: Keyed[A], key: CursorKey): String = {
    val payload = (keyed.kind.tag :: keyed.fields(value)).mkString("|")
    val claim = JwtClaim()
      .about(payload)
      .by(CursorIssuer)
      .to(CursorAudience)
      .issuedAt(now.getEpochSecond)
      .expiresAt(now.plusSeconds(key.ttlSeconds).getEpochSecond)
    JwtCirce.encode(claim, key.secretKey, JwtAlgorithm.HS256)
  }

  def decode[A](value: String, now: Instant)(using keyed: Keyed[A], key: CursorKey): Either[CursorError, A] = {
    for {
      claim <- HmacJwt
        .decode(value, key.secretKey, now, CursorIssuer, CursorAudience)
        .toRight(CursorError.Malformed("Invalid cursor"))
      payload <- claim.subject.toRight(CursorError.Malformed("Invalid cursor subject"))
      tagged <- payload.split("\\|", -1).toList match {
        case tag :: rest => Right((tag, rest))
        case Nil         => Left(CursorError.Malformed("Invalid cursor shape"))
      }
      (tag, fields) = tagged
      actualKind <- CursorKind.values.find(_.tag == tag).toRight(CursorError.Malformed("Unknown cursor kind"))
      _ <- Either.cond(actualKind == keyed.kind, (), CursorError.WrongKind(actualKind.tag))
      cursor <- keyed.read(fields)
    } yield cursor
  }
}
