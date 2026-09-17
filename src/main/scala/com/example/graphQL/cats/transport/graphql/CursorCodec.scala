package com.example.graphQL.cats.transport.graphql

import com.example.graphQL.cats.shared.pagination.{ApplicationCursor, ApplicationEventCursor, JobCursor}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId}
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.{Decoder, DecodingFailure, Encoder}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.{Base64, UUID}
import scala.util.Try

private[graphql] object CursorCodec {
  private enum CursorKind(val value: String) {
    case Job extends CursorKind("job")
    case Application extends CursorKind("application")
    case ApplicationEvent extends CursorKind("applicationEvent")
  }

  private final case class Cursor(kind: CursorKind, createdAt: Option[Instant], occurredAt: Option[Instant], id: UUID)

  private given Encoder[CursorKind] = Encoder.encodeString.contramap(_.value)

  private given Decoder[CursorKind] = Decoder.decodeString.emap {
    case CursorKind.Job.value => Right(CursorKind.Job)
    case CursorKind.Application.value => Right(CursorKind.Application)
    case CursorKind.ApplicationEvent.value => Right(CursorKind.ApplicationEvent)
    case other => Left(s"Unknown cursor kind: $other")
  }

  private given Encoder[Cursor] = Encoder.forProduct4("kind", "createdAt", "occurredAt", "id")(cursor =>
    (cursor.kind, cursor.createdAt.map(_.toString), cursor.occurredAt.map(_.toString), cursor.id.toString)
  )

  private given Decoder[Cursor] = Decoder.instance { cursor =>
    for {
      kind <- cursor.downField("kind").as[CursorKind]
      createdAt <- cursor.downField("createdAt").as[Option[String]].flatMap(decodeInstant)
      occurredAt <- cursor.downField("occurredAt").as[Option[String]].flatMap(decodeInstant)
      id <- cursor.downField("id").as[String].flatMap(decodeUuid)
    } yield Cursor(kind, createdAt, occurredAt, id)
  }

  def encodeJob(cursor: JobCursor): String =
    encode(Cursor(CursorKind.Job, Some(cursor.createdAt), None, cursor.id.value))

  def encodeApplication(cursor: ApplicationCursor): String =
    encode(Cursor(CursorKind.Application, Some(cursor.createdAt), None, cursor.id.value))

  def encodeEvent(cursor: ApplicationEventCursor): String =
    encode(Cursor(CursorKind.ApplicationEvent, None, Some(cursor.occurredAt), cursor.id.value))

  def decodeJob(value: String): Option[JobCursor] =
    decodeCursor(value).flatMap {
      case Cursor(CursorKind.Job, Some(createdAt), None, id) => Some(JobCursor(createdAt, JobId(id)))
      case _ => None
    }

  def decodeApplication(value: String): Option[ApplicationCursor] =
    decodeCursor(value).flatMap {
      case Cursor(CursorKind.Application, Some(createdAt), None, id) => Some(ApplicationCursor(createdAt, ApplicationId(id)))
      case _ => None
    }

  def decodeEvent(value: String): Option[ApplicationEventCursor] =
    decodeCursor(value).flatMap {
      case Cursor(CursorKind.ApplicationEvent, None, Some(occurredAt), id) => Some(ApplicationEventCursor(occurredAt, ApplicationEventId(id)))
      case _ => None
    }

  private def encode(cursor: Cursor): String =
    Base64.getUrlEncoder.withoutPadding().encodeToString(cursor.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))

  private def decodeCursor(value: String): Option[Cursor] =
    Try(String(Base64.getUrlDecoder.decode(value), StandardCharsets.UTF_8)).toOption.flatMap(decode[Cursor](_).toOption)

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
