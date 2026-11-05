package com.example.graphQL.cats.infrastructure.events

import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.infrastructure.events.OperationalEventHeaders.HeaderRefusal
import com.example.graphQL.cats.service.events.*
import fs2.kafka.Headers
import munit.FunSuite
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID

class OperationalEventHeadersSpec extends FunSuite {
  private val H = OperationalEventHeaders
  private val actor = UserId(new UUID(0L, 502L))
  private val event = OperationalEventEnvelope.fromPayload(
    new UUID(0L, 501L),
    Instant.parse("2026-09-19T10:15:30Z"),
    actor,
    OperationalEventPayload.ApplicationCreated(
      new UUID(0L, 503L),
      actor.value,
      new UUID(0L, 504L),
      ApplicationStatus.Created
    )
  )
  private def bytes(value: String) = value.getBytes(StandardCharsets.UTF_8)
  private def own = H.operational(event)
  private def replace(key: String, value: Array[Byte]) = own.map((k, v) => if (k == key) k -> value else k -> v)
  private def text(headers: Seq[(String, Array[Byte])], key: String) =
    headers.collectFirst { case (`key`, v) => new String(v, StandardCharsets.UTF_8) }

  test("KFH-03.3 operational headers carry content type, version, event type and id and verify against the body") {
    assertEquals(text(own, H.ContentType), Some("application/json"))
    assertEquals(text(own, H.SchemaVersion), Some("1"))
    assertEquals(text(own, H.EventType), Some(event.eventType.toString))
    assertEquals(text(own, H.EventId), Some(event.eventId.toString))
    assertEquals(H.verify(own, event), Right(()))
  }

  test("KFH-03.3 a header event type that disagrees with the body is refused") {
    assertEquals(
      H.verify(replace(H.EventType, bytes("JOB_CREATED")), event),
      Left(HeaderRefusal.EventTypeMismatch)
    )
  }

  test("KFH-03.3 a header event id that disagrees with the body is refused") {
    assertEquals(
      H.verify(replace(H.EventId, bytes(new UUID(0L, 9L).toString)), event),
      Left(HeaderRefusal.EventIdMismatch)
    )
  }

  test("KFH-03.3 an upper-case header event id matches the body, as in the Spark reader") {
    assertEquals(H.verify(replace(H.EventId, bytes(event.eventId.toString.toUpperCase)), event), Right(()))
  }

  test("KFH-03.3 a header content type other than JSON is refused") {
    assertEquals(
      H.verify(replace(H.ContentType, bytes("text/plain")), event),
      Left(HeaderRefusal.ContentTypeMismatch)
    )
  }

  test("KFH-03.7 a duplicate header key is refused as malformed") {
    assertEquals(
      H.verify(own :+ (H.EventId -> bytes(event.eventId.toString)), event),
      Left(HeaderRefusal.MalformedHeaders)
    )
    assertEquals(H.verify(Seq("x" -> bytes("a"), "x" -> bytes("b")), event), Left(HeaderRefusal.MalformedHeaders))
  }

  test("KFH-03.7 a non-UTF-8 contract header value is refused as malformed") {
    assertEquals(H.verify(replace(H.EventId, Array(0xff.toByte)), event), Left(HeaderRefusal.MalformedHeaders))
    assertEquals(H.verify(replace(H.SchemaVersion, Array(0xff.toByte)), event), Left(HeaderRefusal.MalformedHeaders))
  }

  test("KFH-03.7 a header value over 256 bytes is refused as malformed") {
    assertEquals(
      H.verify(own :+ ("x-big" -> Array.fill[Byte](257)('a'.toByte)), event),
      Left(HeaderRefusal.MalformedHeaders)
    )
    assertEquals(H.verify(own :+ ("x-big" -> Array.fill[Byte](256)('a'.toByte)), event), Right(()))
  }

  test("KFH-03.7 a null-valued contract header is malformed") {
    List(H.ContentType, H.SchemaVersion, H.EventType, H.EventId).foreach { key =>
      assertEquals(H.verify(replace(key, null), event), Left(HeaderRefusal.MalformedHeaders), key)
    }
    assertEquals(H.version(Seq(H.SchemaVersion -> null)), None)
  }

  // Decision: a null unknown header is ignored, matching the Spark reader (analytics treats only a null contract
  // header as invalid); the analytics header parity spec owns the Spark side.
  test("KFH-03.7 a null-valued unknown header is ignored, not malformed") {
    assertEquals(H.verify(own :+ ("x-null" -> null), event), Right(()))
    assertEquals(H.verify(Seq(H.TraceParent -> null), event), Right(()))
  }

  test("KFH-03.7 a Kafka header with a null value is read without throwing") {
    val headers = H.fromKafka(Headers.empty.append("x-null", null: Array[Byte]))
    assertEquals(headers.map(_._1), List("x-null"))
    assertEquals(H.verify(headers, event), Right(()))
  }

  test("KFH-03.3 unknown headers are ignored, even if not UTF-8") {
    assertEquals(H.verify(own :+ ("x-other" -> Array(0xff.toByte)), event), Right(()))
  }

  test("KFH-03.4 a schema version other than 1 is UnsupportedVersion") {
    assertEquals(
      H.verify(replace(H.SchemaVersion, bytes("2")), event),
      Left(HeaderRefusal.UnsupportedVersion)
    )
  }

  test("KFH-03.4 absent headers verify and an absent version means 1") {
    assertEquals(H.verify(Nil, event), Right(()))
    assertEquals(H.verify(Seq(H.TraceParent -> bytes("t")), event), Right(()))
    assertEquals(H.version(Nil), None)
  }

  test("KFH-03.3 extra headers carry trace context but cannot override fixed keys") {
    val headers = H.operational(
      event,
      List(H.TraceParent -> "00-abc-def-01", H.SchemaVersion -> "9", H.EventType -> "JOB_CLOSED")
    )
    assertEquals(headers.count(_._1 == H.SchemaVersion), 1)
    assertEquals(headers.count(_._1 == H.EventType), 1)
    assertEquals(text(headers, H.SchemaVersion), Some("1"))
    assertEquals(text(headers, H.TraceParent), Some("00-abc-def-01"))
    assertEquals(H.verify(headers, event), Right(()))
  }

  test("KFH-03.3 interview headers carry content type, version and step and cannot be overridden") {
    val headers = InterviewMessageHeaders.interview("SCREENING", List(H.TraceParent -> "t", "hiring-step" -> "other"))
    assertEquals(text(headers, InterviewMessageHeaders.InterviewStep), Some("SCREENING"))
    assertEquals(headers.count(_._1 == "hiring-step"), 1)
    assertEquals(text(headers, H.SchemaVersion), Some("1"))
    assertEquals(text(headers, H.ContentType), Some("application/json"))
    assertEquals(text(headers, H.TraceParent), Some("t"))
  }

  test("KFH-03.4 fs2-kafka headers convert to pairs and expose the version") {
    val kafka = own.foldLeft(Headers.empty)((acc, kv) => acc.append(kv._1, kv._2))
    val converted = H.fromKafka(kafka)
    assertEquals(H.version(converted), Some("1"))
    assertEquals(H.verify(converted, event), Right(()))
  }
}
