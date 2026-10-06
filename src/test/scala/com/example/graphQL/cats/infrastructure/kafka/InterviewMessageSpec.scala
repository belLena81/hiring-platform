package com.example.graphQL.cats.infrastructure.kafka

import com.example.graphQL.cats.service.port.{InterviewMessage, InterviewStep}

import munit.FunSuite
import java.time.Instant
import java.util.UUID

class InterviewMessageSpec extends FunSuite {
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
  test("coordination envelope round trips without hiring details") {
    val bytes = InterviewMessageCodec.bytes(message)
    assertEquals(InterviewMessageCodec.parse(bytes), Right(message))
    val json = new String(bytes, java.nio.charset.StandardCharsets.UTF_8)
    assert(!json.contains("candidate"))
    assert(!json.contains("startsAt"))
  }
  test("invalid step and oversized messages fail closed") {
    val json = new String(InterviewMessageCodec.bytes(message), java.nio.charset.StandardCharsets.UTF_8)
    assert(InterviewMessageCodec.parse(json.replace("Reserve", "Unknown").getBytes).isLeft)
    assert(InterviewMessageCodec.parse(null).isLeft)
    assert(InterviewMessageCodec.parse(Array.fill[Byte](65537)(0)).isLeft)
    assert(InterviewMessageCodec.parse(InterviewMessageCodec.bytes(message.copy(revision = -1))).isLeft)
  }
}
