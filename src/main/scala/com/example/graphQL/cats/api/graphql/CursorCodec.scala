package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.shared.pagination.{ApplicationCursor, ApplicationEventCursor, JobCursor}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId, UserId}
import com.example.graphQL.cats.domain.model.UserCursor
import io.circe.generic.semiauto.*
import io.circe.syntax.*
import io.circe.{Decoder, Encoder}
import java.time.Instant
import java.util.UUID
import pdi.jwt.{JwtAlgorithm, JwtCirce}
import scala.util.Try

private[cats] trait CursorCodec[A] {
  def encode(value: A): String
  def decode(value: String): Either[CursorCodec.CursorError, A]
}

private[cats] object CursorCodec {
  private val CurrentVersion = 2

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

  private final case class Cursor(v: Int, kind: CursorKind, createdAt: Option[Instant], occurredAt: Option[Instant], id: UUID)

  final class CursorCodecs private[CursorCodec] (secret: String) {
    val jobCursorCodec: CursorCodec[JobCursor] = typed(
      CursorKind.Job,
      "job",
      cursor => cursor.createdAt,
      cursor => cursor.id.value,
      (timestamp, id) => JobCursor(timestamp, JobId(id))
    )

    val applicationCursorCodec: CursorCodec[ApplicationCursor] = typed(
      CursorKind.Application,
      "application",
      cursor => cursor.createdAt,
      cursor => cursor.id.value,
      (timestamp, id) => ApplicationCursor(timestamp, ApplicationId(id))
    )

    val eventCursorCodec: CursorCodec[ApplicationEventCursor] = typed(
      CursorKind.ApplicationEvent,
      "event",
      cursor => cursor.occurredAt,
      cursor => cursor.id.value,
      (timestamp, id) => ApplicationEventCursor(timestamp, ApplicationEventId(id))
    )

    val userCursorCodec: CursorCodec[UserCursor] = typed(
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
        val (createdAt, occurredAt) =
          if kind == CursorKind.ApplicationEvent then (None, Some(encodedTimestamp))
          else (Some(encodedTimestamp), None)
        val cursor = Cursor(
          CurrentVersion,
          kind,
          createdAt,
          occurredAt,
          id(value)
        )
        JwtCirce.encode(cursor.asJson, secret, JwtAlgorithm.HS256)
      }

      def decode(value: String): Either[CursorError, A] =
        CursorCodec.decodeCursor(secret, value).flatMap {
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

  }

  def fromSecret(jwtSecret: String): CursorCodecs =
    new CursorCodecs(jwtSecret)

  private given Encoder[CursorKind] = Encoder.encodeString.contramap(_.value)

  private given Decoder[CursorKind] = Decoder.decodeString.emap { raw =>
    CursorKind.values.find(_.value == raw).toRight(s"Unknown cursor kind: $raw")
  }

  private given Encoder[Instant] = Encoder.encodeString.contramap(_.toString)

  private given Decoder[Instant] = Decoder.decodeString.emap { raw =>
    Try(Instant.parse(raw)).toEither.left.map(_ => "Invalid cursor timestamp")
  }

  private given Encoder[UUID] = Encoder.encodeString.contramap(_.toString)

  private given Decoder[UUID] = Decoder.decodeString.emap { raw =>
    Try(UUID.fromString(raw)).toEither.left.map(_ => "Invalid cursor id")
  }

  private given Encoder[Cursor] = deriveEncoder[Cursor]

  private given Decoder[Cursor] = deriveDecoder[Cursor].ensure(_.v == CurrentVersion, s"Unsupported cursor version: $CurrentVersion")

  private def decodeCursor(secret: String, value: String): Either[CursorError, Cursor] =
    JwtCirce.decodeJson(value, secret, Seq(JwtAlgorithm.HS256)).toEither
      .left.map(error => CursorError.Malformed(error.getMessage))
      .flatMap(_.as[Cursor].left.map(error => CursorError.Malformed(error.getMessage)))
}
