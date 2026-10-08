package com.example.graphQL.cats.infrastructure.kafka

import java.nio.charset.StandardCharsets
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
}
