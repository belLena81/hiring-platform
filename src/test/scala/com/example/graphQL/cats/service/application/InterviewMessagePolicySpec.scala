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

  test("bounded clock skew defers through the inclusive tolerance") {
    assertEquals(
      InterviewMessagePolicy.replayDisposition(message.copy(occurredAt = now.plusSeconds(5)), now, 7.days, 5.seconds),
      InterviewReplayDisposition.Deferred
    )
    assertEquals(
      InterviewMessagePolicy
        .replayDisposition(message.copy(occurredAt = now.plusSeconds(5).plusNanos(1)), now, 7.days, 5.seconds),
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
      InterviewMessagePolicy.expiredDecision(Some(workflow), Some(stored), mismatched).map(_.workflow.phase),
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

  private val lifecycleWorkflow = workflow.copy(phase = InterviewWorkflowPhase.CancelPending, revision = 4L)
  private def lifecycleRecord(command: InterviewLifecycleCommand) =
    InterviewWorkflowCommandRecord(
      workflow.id,
      "cancel-step",
      4L,
      command,
      InterviewWorkflowCommandState.Published,
      1,
      now,
      now
    )

  test("DHW-28 every lifecycle intent has exactly one step and notifications keep the participant steps") {
    import InterviewLifecycleCommand as C
    val cancelKey = InterviewWorkflow.cancellationKey(workflow.id, 0)
    val holdKey = InterviewWorkflow.reservationKey(workflow.id, 1)
    val samples: List[(InterviewLifecycleCommand, InterviewStep)] = List(
      C.CancelCalendarSlot(cancelKey) -> InterviewStep.CancelSlot,
      C.LookupCalendarCancellation(cancelKey) -> InterviewStep.LookupCancellation,
      C.HoldReplacementSlot(holdKey, workflow.interval) -> InterviewStep.HoldReplacement,
      C.LookupReplacementHold(holdKey) -> InterviewStep.LookupReplacementHold,
      C.CommitRescheduledInterval(workflow.interval, 1) -> InterviewStep.CommitReschedule,
      C.LookupRescheduleCommitReceipt(workflow.id, 1) -> InterviewStep.LookupRescheduleCommit,
      C.ExpireProposal(now) -> InterviewStep.ExpireProposal,
      C.Notify(
        InterviewNotificationKind.Cancelled,
        InterviewParticipant.Candidate,
        "k"
      ) -> InterviewStep.NotifyCandidate,
      C.Notify(InterviewNotificationKind.RescheduleExpired, InterviewParticipant.Recruiter, "k") ->
        InterviewStep.NotifyRecruiter,
      C.LookupNotificationReceipt(InterviewNotificationKind.Rescheduled, InterviewParticipant.Candidate, "k") ->
        InterviewStep.LookupNotifyCandidate,
      C.LookupNotificationReceipt(InterviewNotificationKind.Cancelled, InterviewParticipant.Recruiter, "k") ->
        InterviewStep.LookupNotifyRecruiter,
      C.RequireRepair("x") -> InterviewStep.RequireRepair
    )
    samples.foreach((command, step) => assertEquals(InterviewMessagePolicy.step(command), step, command.toString))
  }

  test("DHW-18 a lifecycle command is admitted for execution only while the workflow still needs it") {
    val command =
      lifecycleRecord(InterviewLifecycleCommand.CancelCalendarSlot(InterviewWorkflow.cancellationKey(workflow.id, 0)))
    val commandId = InterviewMessagePolicy.stableId(command.stepId)
    val cancelMessage = message.copy(
      messageId = commandId,
      stepId = command.stepId,
      step = InterviewStep.CancelSlot,
      revision = 4L
    )
    assertEquals(
      InterviewMessagePolicy.commandAdmission(Some(lifecycleWorkflow), Some(command), cancelMessage),
      InterviewCommandAdmission.Execute(lifecycleWorkflow, command)
    )
    val moved = lifecycleWorkflow.copy(phase = InterviewWorkflowPhase.CancelNotificationsPending, revision = 5L)
    assertEquals(
      InterviewMessagePolicy.commandAdmission(Some(moved), Some(command), cancelMessage),
      InterviewCommandAdmission.Quarantine(InterviewMessageRejection.Inconsistent)
    )
  }

  test("DHW-18 a late lifecycle message fails its step through the policy, a late notification only its own command") {
    val cancelKey = InterviewWorkflow.cancellationKey(workflow.id, 0)
    val command = lifecycleRecord(InterviewLifecycleCommand.CancelCalendarSlot(cancelKey))
    val cancelMessage = message.copy(
      messageId = InterviewMessagePolicy.stableId(command.stepId),
      stepId = command.stepId,
      step = InterviewStep.CancelSlot,
      revision = 4L
    )
    val failure = InterviewMessagePolicy.lifecycleReplayFailure(
      Some(lifecycleWorkflow),
      Some(command),
      cancelMessage,
      "replay_expired",
      InterviewMessageRejection.Expired,
      now
    )
    assertEquals(
      failure,
      Right(
        InterviewMessagePolicy.LifecycleReplayFailure.Workflow(InterviewLifecycleEvent.RetryExhausted("replay_expired"))
      )
    )
    val informational = lifecycleRecord(
      InterviewLifecycleCommand.Notify(
        InterviewNotificationKind.RescheduleDeclined,
        InterviewParticipant.Recruiter,
        s"${workflow.id.value}:rescheduleDeclined:r4:notify:Recruiter"
      )
    )
    val informationalMessage = cancelMessage.copy(
      messageId = InterviewMessagePolicy.stableId(informational.stepId),
      step = InterviewStep.NotifyRecruiter
    )
    assertEquals(
      InterviewMessagePolicy.lifecycleReplayFailure(
        Some(workflow.copy(phase = InterviewWorkflowPhase.Completed, revision = 4L)),
        Some(informational),
        informationalMessage,
        "replay_expired",
        InterviewMessageRejection.Expired,
        now
      ),
      Right(InterviewMessagePolicy.LifecycleReplayFailure.Notification)
    )
    assertEquals(
      InterviewMessagePolicy
        .lifecycleReplayFailure(None, Some(command), cancelMessage, "x", InterviewMessageRejection.Future, now),
      Left(InterviewMessageRejection.Future)
    )
  }

  test("DHW-20 a failed expiry expires the proposal instead of entering repair, and an early one is only deferred") {
    val proposing = workflow.copy(
      phase = InterviewWorkflowPhase.ProposalPending,
      revision = 4L,
      proposal = Some(InterviewRescheduleProposal(workflow.interval, workflow.recruiterId, now))
    )
    val expiry = lifecycleRecord(InterviewLifecycleCommand.ExpireProposal(now))
    val expiryMessage = message.copy(
      messageId = InterviewMessagePolicy.stableId(expiry.stepId),
      stepId = expiry.stepId,
      step = InterviewStep.ExpireProposal,
      revision = 4L
    )
    def failure(rejection: InterviewMessageRejection, at: Instant) =
      InterviewMessagePolicy.lifecycleReplayFailure(
        Some(proposing),
        Some(expiry),
        expiryMessage,
        "replay_expired",
        rejection,
        at
      )
    assertEquals(
      failure(InterviewMessageRejection.Expired, now.plusSeconds(60)),
      Right(
        InterviewMessagePolicy.LifecycleReplayFailure.Workflow(
          InterviewLifecycleEvent.ProposalExpired(now.plusSeconds(60))
        )
      )
    )
    assertEquals(
      failure(InterviewMessageRejection.Future, now.plusSeconds(60)),
      Right(InterviewMessagePolicy.LifecycleReplayFailure.Defer)
    )
    // The give-up event of every step except expiry is repair; expiry is expiry.
    assertEquals(
      InterviewCommands.giveUpEvent(expiry.command, "publication_exhausted", now),
      InterviewLifecycleEvent.ProposalExpired(now)
    )
    assertEquals(
      InterviewCommands.giveUpEvent(
        InterviewLifecycleCommand.CancelCalendarSlot(InterviewWorkflow.cancellationKey(workflow.id, 0)),
        "publication_exhausted",
        now
      ),
      InterviewLifecycleEvent.RetryExhausted("publication_exhausted")
    )
  }
}
