package com.example.graphQL.cats.infrastructure.kafka

import java.nio.charset.StandardCharsets
import com.example.graphQL.cats.service.port.{InterviewMessage, InterviewStep}
import java.time.Instant
import java.util.UUID
import munit.FunSuite

final class InterviewTransportQuarantineSpec extends FunSuite {
  private val topic = "a" * 249
  private val payload = Some("invalid".getBytes(StandardCharsets.UTF_8))
  private val reason = "invalid interview message"

  test("transport rejection identities fit quarantine storage for maximum-length Kafka topics") {
    val identity = InterviewKafkaRuntime.rejectionIdentity(topic, Int.MaxValue, Long.MaxValue, payload, reason)
    assertEquals(identity.length, "transport:".length + 64)
    assert(identity.startsWith("transport:"))
    assert(identity.length <= 256)
    assert(!identity.contains(topic))
  }

  test("transport identity is deterministic and separates topic, partition, offset, payload and rejection") {
    def identity(t: String = topic, p: Int = 2, o: Long = 3L, b: Option[Array[Byte]] = payload, r: String = reason) =
      InterviewKafkaRuntime.rejectionIdentity(t, p, o, b, r)
    val original = identity()
    assertEquals(identity(), original)
    List(
      identity(t = "b" * 249),
      identity(p = 3),
      identity(o = 4L),
      identity(b = Some(Array[Byte](1))),
      identity(r = "workflow key mismatch")
    ).foreach(value => assertNotEquals(value, original))
    assertNotEquals(
      identity(b = None, r = "invalid null interview message"),
      identity(b = Some(Array.emptyByteArray))
    )
  }

  private val message = InterviewMessage(
    UUID.randomUUID(),
    UUID.randomUUID(),
    "reserve-command",
    InterviewStep.Reserve,
    0L,
    UUID.randomUUID(),
    Instant.parse("2026-10-06T15:00:00Z"),
    None,
    Instant.parse("2026-10-06T15:00:00Z")
  )
  private val body = InterviewMessageCodec.bytes(message)
  private val key = message.workflowId.toString
  private def header(name: String, value: String): (String, Array[Byte]) =
    name -> value.getBytes(StandardCharsets.UTF_8)

  test("KFH-03.3 interview header step disagreeing with the body is refused") {
    val headers = List(header("hiring-step", InterviewStep.CancelSlot.toString))
    assertEquals(InterviewKafkaRuntime.accept(key, headers, body), Left("interview step header mismatch"))
    assertEquals(
      InterviewKafkaRuntime.accept(key, List(header("hiring-step", message.step.toString)), body),
      Right(message)
    )
  }

  test("KFH-03.3 interview content type other than JSON is refused and absent contract headers pass") {
    assertEquals(
      InterviewKafkaRuntime.accept(key, List(header("content-type", "text/plain")), body),
      Left("interview content type mismatch")
    )
    assertEquals(InterviewKafkaRuntime.accept(key, Nil, body), Right(message))
  }

  test("KFH-03.7 interview duplicate header is refused") {
    val headers = List(header("traceparent", "a"), header("traceparent", "b"))
    assertEquals(InterviewKafkaRuntime.accept(key, headers, body), Left("malformed interview headers"))
  }

  test("KFH-03.7 interview oversized or non-UTF-8 contract header is refused") {
    assertEquals(
      InterviewKafkaRuntime.accept(key, List(header("traceparent", "x" * 257)), body),
      Left("malformed interview headers")
    )
    assertEquals(
      InterviewKafkaRuntime.accept(key, List("hiring-step" -> Array[Byte](0xc3.toByte, 0x28)), body),
      Left("malformed interview headers")
    )
  }

  test("KFH-03.7 interview null-valued contract header is refused as malformed and a null unknown header is ignored") {
    assertEquals(
      InterviewKafkaRuntime.accept(key, List("hiring-step" -> (null: Array[Byte])), body),
      Left("malformed interview headers")
    )
    assertEquals(InterviewKafkaRuntime.accept(key, List("x-null" -> (null: Array[Byte])), body), Right(message))
  }

  test("KFH-03.4 interview unsupported version header is refused") {
    assertEquals(
      InterviewKafkaRuntime.accept(key, List(header("hiring-schema-version", "2")), body),
      Left("unsupported interview schema version")
    )
  }
}
