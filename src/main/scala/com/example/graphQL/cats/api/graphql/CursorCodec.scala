package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.shared.pagination.{ApplicationCursor, ApplicationEventCursor, JobCursor}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.{UserCursor}
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.{Decoder, DecodingFailure, Encoder}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.security.MessageDigest
import java.util.{Base64, UUID}
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import scala.util.Try

private[cats] final class CursorCodec private (key: Array[Byte]) {
  import CursorCodec.*

  def encodeJob(cursor: JobCursor): String =
    encode(Cursor(CurrentVersion, CursorKind.Job, Some(cursor.createdAt), None, cursor.id.value))

  def encodeApplication(cursor: ApplicationCursor): String =
    encode(Cursor(CurrentVersion, CursorKind.Application, Some(cursor.createdAt), None, cursor.id.value))

  def encodeEvent(cursor: ApplicationEventCursor): String =
    encode(Cursor(CurrentVersion, CursorKind.ApplicationEvent, None, Some(cursor.occurredAt), cursor.id.value))

  def encodeUser(cursor: UserCursor): String =
    encode(Cursor(CurrentVersion, CursorKind.User, Some(cursor.createdAt), None, cursor.id.value))

  def decodeJob(value: String): Either[CursorError, JobCursor] =
    decodeCursor(value).flatMap {
      case Cursor(_, CursorKind.Job, Some(createdAt), None, id) => Right(JobCursor(createdAt, JobId(id)))
      case Cursor(_, CursorKind.Job, _, _, _) => Left(CursorError.Malformed("Invalid job cursor shape"))
      case Cursor(_, kind, _, _, _) => Left(CursorError.WrongKind(kind.value))
    }

  def decodeApplication(value: String): Either[CursorError, ApplicationCursor] =
    decodeCursor(value).flatMap {
      case Cursor(_, CursorKind.Application, Some(createdAt), None, id) => Right(ApplicationCursor(createdAt, ApplicationId(id)))
      case Cursor(_, CursorKind.Application, _, _, _) => Left(CursorError.Malformed("Invalid application cursor shape"))
      case Cursor(_, kind, _, _, _) => Left(CursorError.WrongKind(kind.value))
    }

  def decodeEvent(value: String): Either[CursorError, ApplicationEventCursor] =
    decodeCursor(value).flatMap {
      case Cursor(_, CursorKind.ApplicationEvent, None, Some(occurredAt), id) => Right(ApplicationEventCursor(occurredAt, ApplicationEventId(id)))
      case Cursor(_, CursorKind.ApplicationEvent, _, _, _) => Left(CursorError.Malformed("Invalid event cursor shape"))
      case Cursor(_, kind, _, _, _) => Left(CursorError.WrongKind(kind.value))
    }

  def decodeUser(value: String): Either[CursorError, UserCursor] =
    decodeCursor(value).flatMap {
      case Cursor(_, CursorKind.User, Some(createdAt), None, id) => Right(UserCursor(createdAt, UserId(id)))
      case Cursor(_, CursorKind.User, _, _, _) => Left(CursorError.Malformed("Invalid user cursor shape"))
      case Cursor(_, kind, _, _, _) => Left(CursorError.WrongKind(kind.value))
    }

  private def encode(cursor: Cursor): String = {
    val payload = base64(cursor.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
    val signature = base64(sign(payload))
    s"$payload.$signature"
  }

  private def decodeCursor(value: String): Either[CursorError, Cursor] =
    value.split("\\.", -1).toList match {
      case payload :: signature :: Nil if payload.nonEmpty && signature.nonEmpty =>
        for {
          provided <- decodeBase64(signature)
          _ <- Either.cond(MessageDigest.isEqual(sign(payload), provided), (), CursorError.Malformed("Invalid cursor signature"))
          bytes <- decodeBase64(payload)
          cursor <- decode[Cursor](String(bytes, StandardCharsets.UTF_8)).left.map(error => CursorError.Malformed(error.getMessage))
        } yield cursor
      case _ => Left(CursorError.Malformed("Invalid signed cursor"))
    }

  private def sign(payload: String): Array[Byte] = {
    val mac = Mac.getInstance(HmacAlgorithm)
    mac.init(new SecretKeySpec(key, HmacAlgorithm))
    mac.doFinal(payload.getBytes(StandardCharsets.UTF_8))
  }
}

private[cats] object CursorCodec {
  private val CurrentVersion = 2
  private val HmacAlgorithm = "HmacSHA256"
  private val KeyDerivationLabel = "hiring-platform:graphql-cursor:v2"

  enum CursorError {
    case Malformed(message: String)
    case WrongKind(expected: String)
  }

  private enum CursorKind(val value: String) {
    case Job extends CursorKind("job")
    case Application extends CursorKind("application")
    case ApplicationEvent extends CursorKind("applicationEvent")
    case User extends CursorKind("user")
  }

  private final case class Cursor(version: Int, kind: CursorKind, createdAt: Option[Instant], occurredAt: Option[Instant], id: UUID)

  def fromSecret(jwtSecret: String): CursorCodec =
    new CursorCodec(deriveKey(jwtSecret))

  private given Encoder[CursorKind] = Encoder.encodeString.contramap(_.value)

  private given Decoder[CursorKind] = Decoder.decodeString.emap { raw =>
    CursorKind.values.find(_.value == raw).toRight(s"Unknown cursor kind: $raw")
  }

  private given Encoder[Cursor] = Encoder.forProduct5("v", "kind", "createdAt", "occurredAt", "id")(cursor =>
    (cursor.version, cursor.kind, cursor.createdAt.map(_.toString), cursor.occurredAt.map(_.toString), cursor.id.toString)
  )

  private given Decoder[Cursor] = Decoder.instance { cursor =>
    for {
      version <- cursor.downField("v").as[Int]
      _ <- Either.cond(version == CurrentVersion, (), io.circe.DecodingFailure(s"Unsupported cursor version: $version", Nil))
      kind <- cursor.downField("kind").as[CursorKind]
      createdAt <- cursor.downField("createdAt").as[Option[String]].flatMap(decodeInstant)
      occurredAt <- cursor.downField("occurredAt").as[Option[String]].flatMap(decodeInstant)
      id <- cursor.downField("id").as[String].flatMap(decodeUuid)
    } yield Cursor(version, kind, createdAt, occurredAt, id)
  }

  private def deriveKey(secret: String): Array[Byte] = {
    val mac = Mac.getInstance(HmacAlgorithm)
    mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), HmacAlgorithm))
    mac.doFinal(KeyDerivationLabel.getBytes(StandardCharsets.UTF_8))
  }

  private def base64(bytes: Array[Byte]): String =
    Base64.getUrlEncoder.withoutPadding().encodeToString(bytes)

  private def decodeBase64(value: String): Either[CursorError, Array[Byte]] =
    Try(Base64.getUrlDecoder.decode(value)).toEither.left.map(error => CursorError.Malformed(error.getMessage))

  private def decodeInstant(value: Option[String]): Decoder.Result[Option[Instant]] =
    decodeOptional(value, Instant.parse, "Invalid cursor timestamp")

  private def decodeUuid(value: String): Decoder.Result[UUID] =
    Try(UUID.fromString(value)).toEither.left.map(_ => DecodingFailure("Invalid cursor id", Nil))

  private def decodeOptional[A](
                                 value: Option[String],
                                 decode: String => A, message: String
                               ): Decoder.Result[Option[A]] =
    value match {
      case Some(raw) => Try(decode(raw)).toEither.left.map(_ => DecodingFailure(message, Nil)).map(Some(_))
      case None => Right(None)
    }
}
