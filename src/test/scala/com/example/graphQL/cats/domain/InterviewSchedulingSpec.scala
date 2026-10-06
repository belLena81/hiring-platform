package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.{InterviewCommandResult, InterviewResult}
import java.time.Instant
import java.util.UUID
import munit.FunSuite

class InterviewSchedulingSpec extends FunSuite {
  private val now = Instant.parse("2026-10-01T12:00:00Z")
  private val applicationId = ApplicationId(UUID.fromString("00000000-0000-0000-0000-000000000001"))
  private val candidateId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000002"))
  private val recruiterId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000003"))
  private val workflowId = InterviewWorkflowId(UUID.fromString("00000000-0000-0000-0000-000000000004"))
  private val idempotencyKey = UUID.fromString("00000000-0000-0000-0000-000000000005")

  private def right[A](value: Either[InterviewWorkflowError, A]): A =
    value.fold(error => fail(s"unexpected workflow error: $error"), identity)

  private def workflow(status: ApplicationStatus = ApplicationStatus.Accepted): InterviewWorkflow =
    right(
      InterviewWorkflow.create(
        workflowId,
        applicationId,
        candidateId,
        recruiterId,
        InterviewInterval(Instant.parse("2026-10-04T12:00:00Z"), Instant.parse("2026-10-04T13:00:00Z")),
        Instant.parse("2026-10-01T12:05:00Z"),
        idempotencyKey,
        status
      )
    )

  private def decide(
      current: InterviewWorkflow,
      revision: Long,
      event: InterviewWorkflowEvent
  ): (InterviewWorkflow, List[InterviewWorkflowCommand]) =
    right(InterviewWorkflow.decide(current, revision, event))

  test("interval requires a future start and an end after the start") {
    assertEquals(
      InterviewInterval.validate(now, now.plusSeconds(60), now),
      Left(InterviewWorkflowError.StartMustBeInFuture)
    )
    assertEquals(
      InterviewInterval.validate(now.plusSeconds(60), now.plusSeconds(60), now),
      Left(InterviewWorkflowError.EndMustFollowStart)
    )
  }

  test("intervals use persistent millisecond precision and reject collapsed submillisecond durations") {
    val start = now.plusSeconds(60).plusNanos(100)
    assertEquals(
      InterviewInterval.validate(start, start.plusNanos(100), now),
      Left(InterviewWorkflowError.EndMustFollowStart)
    )
    assertEquals(
      InterviewInterval.validate(start, now.plusSeconds(120).plusNanos(999), now),
      Right(InterviewInterval(now.plusSeconds(60), now.plusSeconds(120)))
    )
    assertEquals(
      InterviewInterval.validate(now.plusNanos(100), now.plusSeconds(60), now),
      Left(InterviewWorkflowError.StartMustBeInFuture)
    )
  }

  test("a workflow can only start for an Accepted application") {
    assertEquals(
      InterviewWorkflow.create(
        workflowId,
        applicationId,
        candidateId,
        recruiterId,
        InterviewInterval(now.plusSeconds(60), now.plusSeconds(120)),
        now.plusSeconds(30),
        idempotencyKey,
        ApplicationStatus.Created
      ),
      Left(InterviewWorkflowError.ApplicationMustBeAccepted)
    )
  }

  test("command applicability fences by phase and revision while allowing outstanding notification revisions") {
    val base = workflow()
    val reserve = InterviewWorkflowCommand.ReserveCalendarSlot("reserve")
    val notify = InterviewWorkflowCommand.Notify(InterviewParticipant.Candidate, "notify")
    val reservationPending = base.copy(revision = 3L, phase = InterviewWorkflowPhase.ReservationPending)
    val notificationsPending = base.copy(revision = 4L, phase = InterviewWorkflowPhase.NotificationsPending)

    assert(InterviewWorkflow.commandIsApplicable(reservationPending, 3L, reserve))
    assert(!InterviewWorkflow.commandIsApplicable(reservationPending, 2L, reserve))
    assert(!InterviewWorkflow.commandIsApplicable(reservationPending, 3L, notify))
    assert(InterviewWorkflow.commandIsApplicable(notificationsPending, 3L, notify))
    assert(InterviewWorkflow.commandIsApplicable(notificationsPending, 4L, notify))
    assert(!InterviewWorkflow.commandIsApplicable(notificationsPending, 5L, notify))
  }

  test("Saga result conversion is exhaustive and round trips through the transport boundary") {
    InterviewCommandResult.values.foreach { result =>
      val transport = InterviewResult.fromCommandResult(result)
      assertEquals(InterviewCommandResult.fromTransportResult(transport), result)
    }
  }

  test("reservation commits before the guarded status transition and notifications") {
    val started = workflow()
    assertEquals(
      InterviewWorkflow.initialCommand(started),
      InterviewWorkflowCommand.ReserveCalendarSlot(s"${workflowId.value}:reserve")
    )
    val (statusPending, statusCommands) = decide(started, 0L, InterviewWorkflowEvent.ReservationConfirmed)
    assertEquals(statusPending.phase, InterviewWorkflowPhase.StatusCommitPending)
    assertEquals(statusCommands, List(InterviewWorkflowCommand.CommitAcceptedToInterview(ApplicationStatus.Accepted)))

    val (notifying, notificationCommands) = decide(statusPending, 1L, InterviewWorkflowEvent.StatusCommitted)
    assertEquals(notifying.phase, InterviewWorkflowPhase.NotificationsPending)
    assertEquals(notificationCommands.size, 2)
    assert(notificationCommands.forall(_.isInstanceOf[InterviewWorkflowCommand.Notify]))
  }

  test("status rejection compensates the reservation and exposes repair after release") {
    val statusPending = decide(workflow(), 0L, InterviewWorkflowEvent.ReservationConfirmed)._1
    val (compensating, commands) = decide(statusPending, 1L, InterviewWorkflowEvent.StatusCommitRejected)
    assertEquals(compensating.phase, InterviewWorkflowPhase.CompensationPending)
    assertEquals(commands.size, 1)
    assert(commands.head.isInstanceOf[InterviewWorkflowCommand.ReleaseCalendarSlot])

    val (repair, repairCommands) = decide(compensating, 2L, InterviewWorkflowEvent.ReservationReleased)
    assertEquals(repair.phase, InterviewWorkflowPhase.RepairRequired)
    assert(repairCommands.head.isInstanceOf[InterviewWorkflowCommand.RequireRepair])
  }

  test("uncertain external outcomes are reconciled before retry or compensation") {
    val (reserving, calendarLookup) = decide(workflow(), 0L, InterviewWorkflowEvent.ReservationOutcomeUnknown)
    assertEquals(reserving.phase, InterviewWorkflowPhase.ReservationPending)
    assertEquals(calendarLookup, List(InterviewWorkflowCommand.LookupCalendarReservation(workflowId)))
    val (statusPending, statusCommit) = decide(reserving, 1L, InterviewWorkflowEvent.ReservationLookupFound)
    assertEquals(statusCommit, List(InterviewWorkflowCommand.CommitAcceptedToInterview(ApplicationStatus.Accepted)))

    val (checkingStatus, statusLookup) = decide(statusPending, 2L, InterviewWorkflowEvent.StatusCommitOutcomeUnknown)
    assertEquals(statusLookup, List(InterviewWorkflowCommand.LookupStatusCommitReceipt(workflowId)))
    val (compensating, releaseCommands) = decide(checkingStatus, 3L, InterviewWorkflowEvent.StatusLookupAbsent)
    assertEquals(compensating.phase, InterviewWorkflowPhase.StatusCommitPending)
    assertEquals(releaseCommands.size, 1)

    val (notifying, commands) = decide(statusPending, 2L, InterviewWorkflowEvent.StatusCommitted)
    val (checkingReceipt, lookupCommands) = decide(
      notifying,
      3L,
      InterviewWorkflowEvent.NotificationOutcomeUnknown(InterviewParticipant.Candidate)
    )
    assertEquals(checkingReceipt.phase, InterviewWorkflowPhase.NotificationsPending)
    assertEquals(lookupCommands.size, 1)
    assert(lookupCommands.head.isInstanceOf[InterviewWorkflowCommand.LookupNotificationReceipt])
    assertEquals(commands.size, 2)
  }

  test("both independent notification receipts are required for completion") {
    val notifications = decide(
      decide(workflow(), 0L, InterviewWorkflowEvent.ReservationConfirmed)._1,
      1L,
      InterviewWorkflowEvent.StatusCommitted
    )._1
    val (firstReceipt, _) = decide(
      notifications,
      2L,
      InterviewWorkflowEvent.NotificationDelivered(InterviewParticipant.Candidate)
    )
    assertEquals(firstReceipt.phase, InterviewWorkflowPhase.NotificationsPending)
    val (complete, commands) = decide(
      firstReceipt,
      3L,
      InterviewWorkflowEvent.NotificationDelivered(InterviewParticipant.Recruiter)
    )
    assertEquals(complete.phase, InterviewWorkflowPhase.Completed)
    assertEquals(commands, Nil)
  }

  test("stale revisions and out-of-order events cannot advance a workflow") {
    val started = workflow()
    assertEquals(
      InterviewWorkflow.decide(started, 1L, InterviewWorkflowEvent.ReservationConfirmed),
      Left(InterviewWorkflowError.StaleRevision)
    )
    assertEquals(
      InterviewWorkflow.decide(started, 0L, InterviewWorkflowEvent.StatusCommitted),
      Left(InterviewWorkflowError.InvalidTransition)
    )
  }
  test("obsolete committed results and delivered participant work are superseded without repair") {
    val notifying = workflow().copy(revision = 2L, phase = InterviewWorkflowPhase.NotificationsPending)
    assertEquals(
      InterviewWorkflowPolicy.publicationDisposition(
        notifying,
        1L,
        InterviewWorkflowCommand.CommitAcceptedToInterview(ApplicationStatus.Accepted),
        true,
        false,
        6,
        5
      ),
      InterviewPublicationDisposition.Supersede
    )
    val delivered = notifying.copy(notified = Set(InterviewParticipant.Candidate))
    assertEquals(
      InterviewWorkflowPolicy.publicationDisposition(
        delivered,
        2L,
        InterviewWorkflowCommand.LookupNotificationReceipt(InterviewParticipant.Candidate, "key"),
        true,
        false,
        6,
        5
      ),
      InterviewPublicationDisposition.Supersede
    )
    assertEquals(notifying.phase, InterviewWorkflowPhase.NotificationsPending)
  }

  test("necessary results require repair beyond the publication budget and applied receipts are obsolete") {
    val pending = workflow()
    val command = InterviewWorkflow.initialCommand(pending)
    assertEquals(
      InterviewWorkflowPolicy.publicationDisposition(pending, 0L, command, true, false, 5, 5),
      InterviewPublicationDisposition.Send
    )
    assertEquals(
      InterviewWorkflowPolicy.publicationDisposition(pending, 0L, command, true, false, 6, 5),
      InterviewPublicationDisposition.RequireRepair
    )
    assertEquals(
      InterviewWorkflowPolicy.publicationDisposition(pending, 0L, command, true, true, 6, 5),
      InterviewPublicationDisposition.Supersede
    )
  }

  test("pure repair preserves delivered participants and emits typed participant reconciliation") {
    val repair = workflow().copy(
      revision = 4L,
      phase = InterviewWorkflowPhase.RepairRequired,
      notified = Set(InterviewParticipant.Candidate)
    )
    val result = InterviewWorkflowPolicy.repair(repair, 4L, true)
    assertEquals(result.map(_._1.phase), Right(InterviewWorkflowPhase.NotificationsPending))
    assertEquals(result.map(_._1.notified), Right(Set(InterviewParticipant.Candidate)))
    assertEquals(
      result.map(_._2),
      Right(
        List(
          InterviewWorkflowCommand.LookupNotificationReceipt(
            InterviewParticipant.Recruiter,
            s"${repair.id.value}:notify:Recruiter"
          )
        )
      )
    )
    assertEquals(InterviewWorkflowPolicy.repair(repair, 3L, true), Left(InterviewWorkflowError.StaleRevision))
    assertEquals(
      InterviewWorkflowPolicy.repair(repair.copy(phase = InterviewWorkflowPhase.Completed), 4L, true),
      Left(InterviewWorkflowError.InvalidTransition)
    )
  }

  test("only the typed Admin repair cause resets attempt accounting") {
    val marker = InterviewAdvanceCause.ResultReceipt("repair:untrusted-text")
    assert(!marker.resetsAttempts)
    assertEquals(marker.auditActor, None)
    val repair = InterviewAdvanceCause.AdminRepair(java.util.UUID.randomUUID(), workflow().recruiterId)
    assert(repair.resetsAttempts)
    assertEquals(repair.auditActor, Some(workflow().recruiterId))
  }

  test("retry backoff is pure, capped and cannot overflow for exhausted budgets") {
    assertEquals(InterviewWorkflowPolicy.backoffMillis(1L, 1000L, 30000L), 1000L)
    assertEquals(InterviewWorkflowPolicy.backoffMillis(3L, 1000L, 30000L), 4000L)
    assertEquals(InterviewWorkflowPolicy.backoffMillis(Long.MaxValue, 1000L, 30000L), 30000L)
    assertEquals(InterviewWorkflowPolicy.backoffMillis(2L, Long.MaxValue / 2L + 1L, Long.MaxValue), Long.MaxValue)
  }

}
