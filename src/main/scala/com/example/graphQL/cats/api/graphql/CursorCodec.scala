package com.example.graphQL.cats.api.graphql

import com.example.graphQL.cats.application.port.{ApplicationCursor, ApplicationEventCursor, JobCursor}
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationEventId, ApplicationId, JobId}
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.{Decoder, Encoder}
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.{Base64, UUID}
import scala.util.Try

private[graphql] object CursorCodec {
  private final case class Cursor(createdAt: Option[Instant], occurredAt: Option[Instant], id: UUID)

  private given Encoder[Cursor] = Encoder.forProduct3("createdAt", "occurredAt", "id")(cursor =>
    (cursor.createdAt.map(_.toString), cursor.occurredAt.map(_.toString), cursor.id.toString)
  )

  private given Decoder[Cursor] = Decoder.forProduct3("createdAt", "occurredAt", "id")(
    (createdAt: Option[String], occurredAt: Option[String], id: String) =>
      Cursor(createdAt.map(Instant.parse), occurredAt.map(Instant.parse), UUID.fromString(id))
  )

  def encodeJob(cursor: JobCursor): String =
    encode(Cursor(Some(cursor.createdAt), None, cursor.id.value))

  def encodeApplication(cursor: ApplicationCursor): String =
    encode(Cursor(Some(cursor.createdAt), None, cursor.id.value))

  def encodeEvent(cursor: ApplicationEventCursor): String =
    encode(Cursor(None, Some(cursor.occurredAt), cursor.id.value))

  def decodeJob(value: String): Option[JobCursor] =
    decodeCursor(value).flatMap(cursor => cursor.createdAt.map(JobCursor(_, JobId(cursor.id))))

  def decodeApplication(value: String): Option[ApplicationCursor] =
    decodeCursor(value).flatMap(cursor => cursor.createdAt.map(ApplicationCursor(_, ApplicationId(cursor.id))))

  def decodeEvent(value: String): Option[ApplicationEventCursor] =
    decodeCursor(value).flatMap(cursor => cursor.occurredAt.map(ApplicationEventCursor(_, ApplicationEventId(cursor.id))))

  private def encode(cursor: Cursor): String =
    Base64.getUrlEncoder.withoutPadding().encodeToString(cursor.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))

  private def decodeCursor(value: String): Option[Cursor] =
    Try(String(Base64.getUrlDecoder.decode(value), StandardCharsets.UTF_8)).toOption.flatMap(decode[Cursor](_).toOption)
}
