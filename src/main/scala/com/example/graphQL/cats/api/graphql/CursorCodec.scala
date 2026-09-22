package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.UserCursor
import com.example.graphQL.cats.shared.Parsing
import com.example.graphQL.cats.shared.pagination.{ApplicationCursor, ApplicationEventCursor, JobCursor}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Instant
import java.util.{Base64, UUID}
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.util.Try

private[cats] object CursorCodec {
  private val HmacAlgorithm = "HmacSHA256"
  private val KeyDerivationLabel = "hiring-platform:graphql-cursor"
  private val MacBytes = 16
  private val Base64Encoder = Base64.getUrlEncoder.withoutPadding()
  private val Base64Decoder = Base64.getUrlDecoder

  final class CursorKey private[graphql] (private val bytes: Array[Byte]) {
    private val keySpec = new SecretKeySpec(bytes, HmacAlgorithm)
    private val macs: ThreadLocal[Mac] = ThreadLocal.withInitial(() => Mac.getInstance(HmacAlgorithm))

    private[graphql] def sign(payload: String): Array[Byte] = {
      val mac = macs.get()
      mac.init(keySpec)
      mac.doFinal(payload.getBytes(StandardCharsets.UTF_8))
    }
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

  def keyFromSecret(secret: String): CursorKey = {
    val mac = Mac.getInstance(HmacAlgorithm)
    mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HmacAlgorithm))
    new CursorKey(mac.doFinal(KeyDerivationLabel.getBytes(StandardCharsets.UTF_8)))
  }

  def encode[A](value: A)(using keyed: Keyed[A], key: CursorKey): String = {
    val payload = s"${keyed.kind.tag}|${keyed.at(value)}|${keyed.id(value)}"
    val mac = Base64Encoder.encodeToString(sign(payload, key).take(MacBytes))
    Base64Encoder.encodeToString(s"$payload|$mac".getBytes(StandardCharsets.UTF_8))
  }

  def decode[A](value: String)(using keyed: Keyed[A], key: CursorKey): Either[CursorError, A] =
    for {
      decoded <- decodeText(value)
      parts <- decoded.split("\\|", -1) match {
        case Array(tag, at, id, mac) => Right((tag, at, id, mac))
        case _ => Left(CursorError.Malformed("Invalid cursor shape"))
      }
      (tag, at, id, mac) = parts
      actualKind <- CursorKind.values.find(_.tag == tag).toRight(CursorError.Malformed("Unknown cursor kind"))
      payload = s"$tag|$at|$id"
      suppliedMac <- decodeMac(mac)
      _ <- Either.cond(
        MessageDigest.isEqual(suppliedMac, sign(payload, key).take(MacBytes)),
        (),
        CursorError.Malformed("Invalid cursor signature")
      )
      _ <- Either.cond(actualKind == keyed.kind, (), CursorError.WrongKind(actualKind.tag))
      timestamp <- Try(Instant.parse(at)).toEither.left.map(_ => CursorError.Malformed("Invalid cursor timestamp"))
      uuid <- Parsing.parseUuid(id).left.map(_ => CursorError.Malformed("Invalid cursor id"))
    } yield keyed.make(timestamp, uuid)

  private def decodeText(value: String): Either[CursorError, String] =
    Try(new String(Base64Decoder.decode(value), StandardCharsets.UTF_8)).toEither
      .left.map(error => CursorError.Malformed(error.getMessage))

  private def decodeMac(value: String): Either[CursorError, Array[Byte]] =
    Try(Base64Decoder.decode(value)).toEither
      .left.map(error => CursorError.Malformed(error.getMessage))
      .flatMap(bytes => Either.cond(bytes.length == MacBytes, bytes, CursorError.Malformed("Invalid cursor signature")))

  private def sign(payload: String, key: CursorKey): Array[Byte] = key.sign(payload)
}
