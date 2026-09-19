package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.shared.pagination.{ApplicationCursor, ApplicationEventCursor, JobCursor}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.UserCursor
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

private[cats] trait CursorCodec[A] {
  def encode(value: A): String
  def decode(value: String): Either[CursorCodec.CursorError, A]
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

  final class CursorCodecs private[CursorCodec] (key: Array[Byte]) {
    given jobCursorCodec: CursorCodec[JobCursor] = typed(
      CursorKind.Job,
      "job",
      cursor => cursor.createdAt,
      cursor => cursor.id.value,
      (timestamp, id) => JobCursor(timestamp, JobId(id))
    )

    given applicationCursorCodec: CursorCodec[ApplicationCursor] = typed(
      CursorKind.Application,
      "application",
      cursor => cursor.createdAt,
      cursor => cursor.id.value,
      (timestamp, id) => ApplicationCursor(timestamp, ApplicationId(id))
    )

    given eventCursorCodec: CursorCodec[ApplicationEventCursor] = typed(
      CursorKind.ApplicationEvent,
      "event",
      cursor => cursor.occurredAt,
      cursor => cursor.id.value,
      (timestamp, id) => ApplicationEventCursor(timestamp, ApplicationEventId(id))
    )

    given userCursorCodec: CursorCodec[UserCursor] = typed(
      CursorKind.User,
      "user",
      cursor => cursor.createdAt,
      cursor => cursor.id.value,
      (timestamp, id) => UserCursor(timestamp, UserId(id))
    )

    private def typed[A](
        kind: CursorKind,
        label: String,
        timestamp: A => Instant,
        id: A => UUID,
        build: (Instant, UUID) => A
    ): CursorCodec[A] = new CursorCodec[A] {
      def encode(value: A): String = {
        val encodedTimestamp = timestamp(value)
        val cursor = Cursor(
          CurrentVersion,
          kind,
          Option.when(kind != CursorKind.ApplicationEvent)(encodedTimestamp),
          Option.when(kind == CursorKind.ApplicationEvent)(encodedTimestamp),
          id(value)
        )
        encodeCursor(cursor)
      }

      def decode(value: String): Either[CursorError, A] =
        CursorCodec.decodeCursor(key, value).flatMap {
          case Cursor(_, actualKind, _, _, _) if actualKind != kind =>
            Left(CursorError.WrongKind(actualKind.value))
          case Cursor(_, _, createdAt, occurredAt, cursorId) =>
            val expectedTimestamp = if kind == CursorKind.ApplicationEvent then occurredAt else createdAt
            val unexpectedTimestamp = if kind == CursorKind.ApplicationEvent then createdAt else occurredAt
            (expectedTimestamp, unexpectedTimestamp) match {
              case (Some(value), None) => Right(build(value, cursorId))
              case _ => Left(CursorError.Malformed(s"Invalid $label cursor shape"))
            }
        }
    }

    private def encodeCursor(cursor: Cursor): String = CursorCodec.encodeCursor(key, cursor)
  }

  def fromSecret(jwtSecret: String): CursorCodecs =
    new CursorCodecs(deriveKey(jwtSecret))

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

  private def encodeCursor(key: Array[Byte], cursor: Cursor): String = {
    val payload = base64(cursor.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
    val signature = base64(sign(key, payload))
    s"$payload.$signature"
  }

  private def decodeCursor(key: Array[Byte], value: String): Either[CursorError, Cursor] =
    value.split("\\.", -1).toList match {
      case payload :: signature :: Nil if payload.nonEmpty && signature.nonEmpty =>
        for {
          provided <- decodeBase64(signature)
          _ <- Either.cond(MessageDigest.isEqual(sign(key, payload), provided), (), CursorError.Malformed("Invalid cursor signature"))
          bytes <- decodeBase64(payload)
          cursor <- decode[Cursor](String(bytes, StandardCharsets.UTF_8)).left.map(error => CursorError.Malformed(error.getMessage))
        } yield cursor
      case _ => Left(CursorError.Malformed("Invalid signed cursor"))
    }

  private def sign(key: Array[Byte], payload: String): Array[Byte] = {
    val mac = Mac.getInstance(HmacAlgorithm)
    mac.init(new SecretKeySpec(key, HmacAlgorithm))
    mac.doFinal(payload.getBytes(StandardCharsets.UTF_8))
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
     decode: String => A,
     message: String
  ): Decoder.Result[Option[A]] =
    value match {
      case Some(raw) => Try(decode(raw)).toEither.left.map(_ => DecodingFailure(message, Nil)).map(Some(_))
      case None => Right(None)
    }
}
