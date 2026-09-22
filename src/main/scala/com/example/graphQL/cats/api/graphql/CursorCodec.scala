package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.UserCursor
import com.example.graphQL.cats.shared.Parsing
import com.example.graphQL.cats.shared.pagination.{ApplicationCursor, ApplicationEventCursor, JobCursor}
import pdi.jwt.{JwtAlgorithm, JwtCirce, JwtClaim, JwtOptions}

import java.nio.charset.StandardCharsets
import java.time.{Clock as JavaClock, Instant, ZoneOffset}
import java.util.UUID
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.util.Try

private[cats] object CursorCodec {
  private val HmacAlgorithm = "HmacSHA256"
  private val KeyDerivationLabel = "hiring-platform:graphql-cursor"
  private val CursorIssuer = "hiring-platform-cursor"
  private val CursorAudience = "hiring-graphql-api"
  private val Algorithms = Seq(JwtAlgorithm.HS256)
  private val Options = JwtOptions(signature = true, expiration = true, notBefore = true, leeway = 0)

  final class CursorKey private[graphql] (private val bytes: Array[Byte], val ttlSeconds: Long) {
    private[graphql] val secretKey = new SecretKeySpec(bytes, HmacAlgorithm)
  }

  trait Keyed[A] {
    def kind: CursorKind
    def at(value: A): Instant
    def id(value: A): UUID
    def make(at: Instant, id: UUID): A
  }

  enum CursorKind(val tag: String) {
    case Job extends CursorKind("j")
    case Application extends CursorKind("a")
    case Event extends CursorKind("e")
    case User extends CursorKind("u")
  }

  enum CursorError {
    case Malformed(message: String)
    case WrongKind(actual: String)
  }

  given Keyed[JobCursor] with
    def kind: CursorKind = CursorKind.Job
    def at(value: JobCursor): Instant = value.createdAt
    def id(value: JobCursor): UUID = value.id.value
    def make(at: Instant, id: UUID): JobCursor = JobCursor(at, JobId(id))

  given Keyed[ApplicationCursor] with
    def kind: CursorKind = CursorKind.Application
    def at(value: ApplicationCursor): Instant = value.createdAt
    def id(value: ApplicationCursor): UUID = value.id.value
    def make(at: Instant, id: UUID): ApplicationCursor = ApplicationCursor(at, ApplicationId(id))

  given Keyed[ApplicationEventCursor] with
    def kind: CursorKind = CursorKind.Event
    def at(value: ApplicationEventCursor): Instant = value.occurredAt
    def id(value: ApplicationEventCursor): UUID = value.id.value
    def make(at: Instant, id: UUID): ApplicationEventCursor = ApplicationEventCursor(at, ApplicationEventId(id))

  given Keyed[UserCursor] with
    def kind: CursorKind = CursorKind.User
    def at(value: UserCursor): Instant = value.createdAt
    def id(value: UserCursor): UUID = value.id.value
    def make(at: Instant, id: UUID): UserCursor = UserCursor(at, UserId(id))

  def keyFromSecret(secret: String, ttlSeconds: Long = 900L): CursorKey = {
    val mac = Mac.getInstance(HmacAlgorithm)
    mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HmacAlgorithm))
    new CursorKey(mac.doFinal(KeyDerivationLabel.getBytes(StandardCharsets.UTF_8)), ttlSeconds)
  }

  def encode[A](value: A, now: Instant)(using keyed: Keyed[A], key: CursorKey): String = {
    val payload = s"${keyed.kind.tag}|${keyed.at(value)}|${keyed.id(value)}"
    val claim = JwtClaim()
      .about(payload)
      .by(CursorIssuer)
      .to(CursorAudience)
      .issuedAt(now.getEpochSecond)
      .expiresAt(now.plusSeconds(key.ttlSeconds).getEpochSecond)
    JwtCirce.encode(claim, key.secretKey, JwtAlgorithm.HS256)
  }

  def decode[A](value: String, now: Instant)(using keyed: Keyed[A], key: CursorKey): Either[CursorError, A] = {
    given clock: JavaClock = JavaClock.fixed(now, ZoneOffset.UTC)

    for {
      claim <- JwtCirce(clock).decode(value, key.secretKey, Algorithms, Options).toEither
        .left.map(_ => CursorError.Malformed("Invalid cursor"))
      _ <- Either.cond(claim.isValid(CursorIssuer, CursorAudience), (), CursorError.Malformed("Invalid cursor"))
      payload <- claim.subject.toRight(CursorError.Malformed("Invalid cursor subject"))
      parts <- payload.split("\\|", -1) match {
        case Array(tag, at, id) => Right((tag, at, id))
        case _ => Left(CursorError.Malformed("Invalid cursor shape"))
      }
      (tag, at, id) = parts
      actualKind <- CursorKind.values.find(_.tag == tag).toRight(CursorError.Malformed("Unknown cursor kind"))
      _ <- Either.cond(actualKind == keyed.kind, (), CursorError.WrongKind(actualKind.tag))
      timestamp <- Try(Instant.parse(at)).toEither.left.map(_ => CursorError.Malformed("Invalid cursor timestamp"))
      uuid <- Parsing.parseUuid(id).left.map(_ => CursorError.Malformed("Invalid cursor id"))
    } yield keyed.make(timestamp, uuid)
  }
}
