package com.example.graphQL.cats.api.graphql

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.shared.Parsing
import com.example.graphQL.cats.domain.pagination.TimestampIdCursor
import com.example.graphQL.cats.service.search.NearbyJobCursor
import com.example.graphQL.cats.shared.crypto.Hmac

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.util.{Base64, UUID}

private[cats] object CursorCodec {
  private val KeyDerivationLabel = "hiring-platform:graphql-cursor"

  /** A cursor is a position, not a credential: every page request is authorized again, so the lifetime only bounds how
    * long a paused client may resume a listing. One day outlasts a paused session; tampering is excluded by the
    * signature, not by a short expiry.
    */
  val DefaultTtlSeconds: Long = 86400L

  private val Separator = "."
  private val PayloadSeparator = "|"
  private val url = Base64.getUrlEncoder.withoutPadding()
  private val urlDecoder = Base64.getUrlDecoder

  final class CursorKey private[graphql] (private val bytes: Array[Byte], val ttlSeconds: Long) {
    private[graphql] def sign(value: String): Array[Byte] = Hmac.sha256(bytes, value)
  }

  /** Wire shape of one cursor type: the payload is `kind|field|field...`, followed by the expiry on the wire. */
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

  /** A cursor bound to the list it was issued for (parent id and filters): replaying it on another parent or with
    * another filter is refused, like a nearby cursor with other criteria. The scope holds no `|`.
    */
  final case class Scoped[A](scope: String, value: A)

  given [A](using inner: Keyed[A]): Keyed[Scoped[A]] with
    def kind: CursorKind = inner.kind
    def fields(value: Scoped[A]): List[String] = value.scope :: inner.fields(value.value)
    def read(fields: List[String]): Either[CursorError, Scoped[A]] =
      fields match {
        case scope :: rest => inner.read(rest).map(Scoped(scope, _))
        case Nil           => Left(CursorError.Malformed("Invalid cursor shape"))
      }

  def keyFromSecret(secret: String, ttlSeconds: Long = DefaultTtlSeconds): CursorKey =
    new CursorKey(Hmac.sha256(secret.getBytes(StandardCharsets.UTF_8), KeyDerivationLabel), ttlSeconds)

  /** Compact opaque cursor: `base64url(kind|fields|expiryEpochSecond).base64url(HMAC-SHA256 of the first segment)`. */
  def encode[A](value: A, now: Instant)(using keyed: Keyed[A], key: CursorKey): String = {
    val payload =
      (keyed.kind.tag :: keyed.fields(value) ::: List(now.plusSeconds(key.ttlSeconds).getEpochSecond.toString))
        .mkString(PayloadSeparator)
    val body = url.encodeToString(payload.getBytes(StandardCharsets.UTF_8))
    body + Separator + url.encodeToString(key.sign(body))
  }

  def decode[A](value: String, now: Instant)(using keyed: Keyed[A], key: CursorKey): Either[CursorError, A] =
    for {
      signed <- value.split("\\.", -1).toList match {
        case List(body, signature) => Right((body, signature))
        case _                     => Left(CursorError.Malformed("Invalid cursor"))
      }
      (body, signature) = signed
      presented <- Either
        .catchNonFatal(urlDecoder.decode(signature))
        .leftMap(_ => CursorError.Malformed("Invalid cursor"))
      _ <- Either.cond(MessageDigest.isEqual(key.sign(body), presented), (), CursorError.Malformed("Invalid cursor"))
      payload <- Either
        .catchNonFatal(new String(urlDecoder.decode(body), StandardCharsets.UTF_8))
        .leftMap(_ => CursorError.Malformed("Invalid cursor"))
      tagged <- payload.split("\\|", -1).toList match {
        case tag :: rest if rest.nonEmpty => Right((tag, rest.init, rest.last))
        case _                            => Left(CursorError.Malformed("Invalid cursor shape"))
      }
      (tag, fields, expiry) = tagged
      expiresAt <- expiry.toLongOption.toRight(CursorError.Malformed("Invalid cursor expiry"))
      _ <- Either.cond(now.getEpochSecond <= expiresAt, (), CursorError.Malformed("Invalid cursor"))
      actualKind <- CursorKind.values.find(_.tag == tag).toRight(CursorError.Malformed("Unknown cursor kind"))
      _ <- Either.cond(actualKind == keyed.kind, (), CursorError.WrongKind(actualKind.tag))
      cursor <- keyed.read(fields)
    } yield cursor
}
