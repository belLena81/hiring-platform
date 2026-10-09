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

  test("DHW-28 every step, including the cancellation and rescheduling steps, travels in the same envelope") {
    InterviewStep.values.foreach { step =>
      val value = message.copy(step = step)
      assertEquals(InterviewMessageCodec.parse(InterviewMessageCodec.bytes(value)), Right(value), step.toString)
    }
    List(
      "CancelSlot",
      "LookupCancellation",
      "HoldReplacement",
      "LookupReplacementHold",
      "CommitReschedule",
      "LookupRescheduleCommit",
      "ExpireProposal"
    ).foreach(name => assert(InterviewStep.values.exists(_.toString == name), name))
  }
  test("DHW-28 the envelope keeps the workflow-id key and the same field names") {
    val json = new String(
      InterviewMessageCodec.bytes(message.copy(step = InterviewStep.CancelSlot)),
      java.nio.charset.StandardCharsets.UTF_8
    )
    val fields = io.circe.parser.parse(json).toOption.flatMap(_.asObject).map(_.keys.toSet)
    assertEquals(
      fields,
      Some(
        Set("messageId", "workflowId", "stepId", "step", "revision", "causationId", "deadline", "result", "occurredAt")
      )
    )
  }
  test("DHW-28 an unknown or malformed step is quarantined by the decoder, never executed") {
    val json = new String(
      InterviewMessageCodec.bytes(message.copy(step = InterviewStep.CancelSlot)),
      java.nio.charset.StandardCharsets.UTF_8
    )
    assert(InterviewMessageCodec.parse(json.replace("CancelSlot", "CancelEverything").getBytes).isLeft)
    assert(InterviewMessageCodec.parse(json.replace("CancelSlot", "").getBytes).isLeft)
  }
}
