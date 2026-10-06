package com.example.graphQL.cats.service.application

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.*
import java.time.Instant
import java.util.UUID
import munit.FunSuite
import scala.concurrent.duration.*

final class InterviewMessagePolicySpec extends FunSuite {
  private val now = Instant.parse("2026-10-06T12:00:00Z")
  private val workflow = InterviewWorkflow(
    InterviewWorkflowId(new UUID(0L, 1L)),
    ApplicationId(new UUID(0L, 2L)),
    UserId(new UUID(0L, 3L)),
    UserId(new UUID(0L, 4L)),
    InterviewInterval(now.plusSeconds(600), now.plusSeconds(1200)),
    now.plusSeconds(300),
    new UUID(0L, 5L),
    0L,
    InterviewWorkflowPhase.ReservationPending,
    Set.empty,
    UserId(new UUID(0L, 4L))
  )
  private val command = InterviewWorkflowCommandRecord(
    workflow.id,
    "reservation-step",
    0L,
    InterviewWorkflow.initialCommand(workflow),
    InterviewWorkflowCommandState.Published,
    1,
    now,
    now
  )
  private val commandId = InterviewMessagePolicy.stableId(command.stepId)
  private val message = InterviewMessage(
    commandId,
    workflow.id.value,
    command.stepId,
    InterviewStep.Reserve,
    0L,
    workflow.id.value,
    workflow.preCommitDeadline,
    None,
    now
  )

  test("replay boundaries are inclusive and use only the supplied observation time") {
    assertEquals(InterviewMessagePolicy.replayDisposition(message, now, 7.days), InterviewReplayDisposition.Timely)
    assertEquals(
      InterviewMessagePolicy
        .replayDisposition(message.copy(occurredAt = now.minusSeconds(7.days.toSeconds)), now, 7.days),
      InterviewReplayDisposition.Timely
    )
    assertEquals(
      InterviewMessagePolicy
        .replayDisposition(message.copy(occurredAt = now.minusSeconds(7.days.toSeconds).minusNanos(1)), now, 7.days),
      InterviewReplayDisposition.Expired
    )
    assertEquals(
      InterviewMessagePolicy.replayDisposition(message.copy(occurredAt = now.plusNanos(1)), now, 7.days),
      InterviewReplayDisposition.Future
    )
  }

  test("expired receipt reconciliation deliberately differs from timely result admission") {
    val stored = command.copy(result = Some(InterviewCommandResult.Succeeded))
    val mismatched = message.copy(
      messageId = InterviewMessagePolicy.stableId(s"$commandId:result"),
      causationId = commandId,
      result = Some(InterviewResult.Rejected)
    )
    assertEquals(
      InterviewMessagePolicy.resultAdmission(Some(workflow), Some(stored), mismatched),
      Left(InterviewMessageRejection.Inconsistent)
    )
    assertEquals(
      InterviewMessagePolicy.expiredDecision(Some(workflow), Some(stored), mismatched).map(_._1.phase),
      Right(InterviewWorkflowPhase.RepairRequired)
    )
  }

  test("a superseded command cannot execute or make expired workflow progress") {
    val stored = command.copy(state = InterviewWorkflowCommandState.Superseded)
    assertEquals(
      InterviewMessagePolicy.commandAdmission(Some(workflow), Some(stored), message),
      InterviewCommandAdmission.Quarantine(InterviewMessageRejection.Inconsistent)
    )
    assertEquals(
      InterviewMessagePolicy.expiredDecision(Some(workflow), Some(stored), message),
      Left(InterviewMessageRejection.Expired)
    )
  }

  test("identity, occurrence and deadline mismatches are rejected before execution") {
    val mismatches = List(
      message.copy(messageId = new UUID(0L, 99L)),
      message.copy(causationId = new UUID(0L, 99L)),
      message.copy(revision = 1L),
      message.copy(step = InterviewStep.CommitHiring),
      message.copy(occurredAt = now.minusSeconds(1)),
      message.copy(deadline = workflow.preCommitDeadline.plusSeconds(1)),
      message.copy(result = Some(InterviewResult.Succeeded))
    )
    mismatches.foreach { delivery =>
      assertEquals(
        InterviewMessagePolicy.commandAdmission(Some(workflow), Some(command), delivery),
        InterviewCommandAdmission.Quarantine(InterviewMessageRejection.Inconsistent)
      )
    }
  }

  test("unknown outcomes reconcile while known success remains successful after retry exhaustion") {
    assertEquals(
      InterviewMessagePolicy.resultEvent(message.copy(result = Some(InterviewResult.OutcomeUnknown)), 4L, 5),
      Some(InterviewWorkflowEvent.ReservationOutcomeUnknown)
    )
    assertEquals(
      InterviewMessagePolicy.resultEvent(message.copy(result = Some(InterviewResult.OutcomeUnknown)), 5L, 5),
      Some(InterviewWorkflowEvent.RetryExhausted("provider"))
    )
    assertEquals(
      InterviewMessagePolicy.resultEvent(message.copy(result = Some(InterviewResult.Succeeded)), 5L, 5),
      Some(InterviewWorkflowEvent.ReservationConfirmed)
    )
    assertEquals(InterviewMessagePolicy.event(message), None)
  }

  test("both notification participants and lookup outcomes keep their own workflow events") {
    val cases = List(
      (
        InterviewStep.NotifyCandidate,
        InterviewResult.Succeeded,
        InterviewWorkflowEvent.NotificationDelivered(InterviewParticipant.Candidate)
      ),
      (
        InterviewStep.NotifyRecruiter,
        InterviewResult.OutcomeUnknown,
        InterviewWorkflowEvent.NotificationOutcomeUnknown(InterviewParticipant.Recruiter)
      ),
      (
        InterviewStep.LookupNotifyCandidate,
        InterviewResult.Found,
        InterviewWorkflowEvent.NotificationLookupFound(InterviewParticipant.Candidate)
      ),
      (
        InterviewStep.LookupNotifyRecruiter,
        InterviewResult.Absent,
        InterviewWorkflowEvent.NotificationLookupAbsent(InterviewParticipant.Recruiter)
      )
    )
    cases.foreach { case (step, result, expected) =>
      assertEquals(InterviewMessagePolicy.event(message.copy(step = step, result = Some(result))), Some(expected))
    }
  }

  test("retry deadlines are values and successful results do not request backoff") {
    assertEquals(
      InterviewMessagePolicy.retryAvailableAt(Some(InterviewResult.Absent), 3L, now, 1000L, 30000L),
      Some(now.plusSeconds(4))
    )
    assertEquals(
      InterviewMessagePolicy.retryAvailableAt(Some(InterviewResult.OutcomeUnknown), 63L, now, 1000L, 30000L),
      Some(now.plusSeconds(30))
    )
    assertEquals(InterviewMessagePolicy.retryAvailableAt(Some(InterviewResult.Succeeded), 3L, now, 1000L, 30000L), None)
  }
}
