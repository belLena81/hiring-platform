package com.example.graphQL.cats.infrastructure.events

import cats.syntax.all.*
import com.example.graphQL.cats.service.events.OperationalEventEnvelope
import fs2.kafka.Headers
import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.util.Locale

/** Pure Kafka record-header contract for operational event records.
  *
  * Headers are optional metadata and the JSON body is authoritative. Producers stamp content type, schema version,
  * event type and event id; consumers verify what is present against the decoded envelope. An absent schema version
  * header means version 1, and an absent contract header is not an error. Unknown headers (for example trace context)
  * are ignored, including an unknown header whose value is null (Kafka permits null header values; the Spark analytics
  * reader ignores it too). A null contract header value is malformed.
  *
  * Refusals: a duplicate header key or a value longer than [[MaxHeaderValueBytes]] (any header), a contract header
  * whose value is not valid UTF-8 ([[HeaderRefusal.MalformedHeaders]]); a present version other than
  * [[CurrentSchemaVersion]] ([[HeaderRefusal.UnsupportedVersion]]); a present content type, event type or event id that
  * disagrees with the body.
  *
  * Headers are a sequence of pairs, not a map, so duplicates stay visible to [[verify]].
  */
object OperationalEventHeaders {
  type RecordHeaders = Seq[(String, Array[Byte])]

  val ContentType: String = "content-type"
  val ContentTypeJson: String = "application/json"
  val SchemaVersion: String = "hiring-schema-version"
  val CurrentSchemaVersion: String = "1"
  val EventType: String = "hiring-event-type"
  val EventId: String = "hiring-event-id"
  val TraceParent: String = "traceparent"
  val MaxHeaderValueBytes: Int = 256

  private[events] val ContractKeys: Set[String] = Set(ContentType, SchemaVersion, EventType, EventId)

  enum HeaderRefusal {
    case UnsupportedVersion
    case InterviewStepMismatch
    case EventTypeMismatch
    case EventIdMismatch
    case ContentTypeMismatch
    case MalformedHeaders
  }

  /** Fixed headers followed by `extra`; `extra` (for example propagated trace headers) cannot replace fixed keys. */
  def withFixed(fixed: List[(String, String)], extra: List[(String, String)]): List[(String, Array[Byte])] = {
    val reserved = fixed.map(_._1).toSet
    (fixed ++ extra.filterNot((key, _) => reserved.contains(key))).map((key, value) =>
      key -> value.getBytes(StandardCharsets.UTF_8)
    )
  }

  def operational(
      envelope: OperationalEventEnvelope,
      extra: List[(String, String)] = Nil
  ): List[(String, Array[Byte])] =
    withFixed(
      List(
        ContentType -> ContentTypeJson,
        SchemaVersion -> CurrentSchemaVersion,
        EventType -> envelope.eventType.toString,
        EventId -> envelope.eventId.toString
      ),
      extra
    )

  private def text(value: Array[Byte]): Option[String] =
    Option(value).flatMap(bytes =>
      scala.util
        .Try(
          StandardCharsets.UTF_8
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
            .decode(ByteBuffer.wrap(bytes))
            .toString
        )
        .toOption
    )

  /** The first schema version header as text; None when absent or not valid UTF-8. */
  def version(headers: RecordHeaders): Option[String] =
    headers.collectFirst { case (SchemaVersion, value) => value }.flatMap(text)

  def fromKafka(headers: Headers): List[(String, Array[Byte])] =
    headers.toChain.toList.map(header => header.key -> header.value)

  def verify(headers: RecordHeaders, envelope: OperationalEventEnvelope): Either[HeaderRefusal, Unit] =
    verifyContract(
      headers,
      ContractKeys,
      List(
        (EventType, envelope.eventType.toString, HeaderRefusal.EventTypeMismatch),
        (EventId, envelope.eventId.toString, HeaderRefusal.EventIdMismatch)
      )
    )

  /** The shared header rules: well-formedness of every header, the supported version, a JSON content type, then each
    * `(key, expected value, refusal)` check, where an absent header always passes.
    */
  private[events] def verifyContract(
      headers: RecordHeaders,
      contractKeys: Set[String],
      checks: List[(String, String, HeaderRefusal)]
  ): Either[HeaderRefusal, Unit] = {
    val keys = headers.map(_._1)
    val malformed =
      keys.distinct.size != keys.size ||
        headers.exists((_, value) => value != null && value.length > MaxHeaderValueBytes) ||
        headers.exists((key, value) => contractKeys.contains(key) && text(value).isEmpty)
    // The event id is UUID text: compared case-insensitively, as the Spark reader does.
    def comparable(key: String, value: String): String = if (key == EventId) value.toLowerCase(Locale.ROOT) else value
    def present(key: String): Option[String] = headers.collectFirst { case (`key`, value) => value }.flatMap(text)
    for {
      _ <- Either.cond(!malformed, (), HeaderRefusal.MalformedHeaders)
      _ <- Either.cond(
        present(SchemaVersion).forall(_ == CurrentSchemaVersion),
        (),
        HeaderRefusal.UnsupportedVersion
      )
      _ <- Either.cond(present(ContentType).forall(_ == ContentTypeJson), (), HeaderRefusal.ContentTypeMismatch)
      _ <- checks.traverse_ { (key, expected, refusal) =>
        Either.cond(present(key).forall(found => comparable(key, found) == comparable(key, expected)), (), refusal)
      }
    } yield ()
  }
}
