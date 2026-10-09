package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.InterviewLifecycleFixtures.*
import com.example.graphQL.cats.domain.workflow.*
import java.time.Instant
import munit.FunSuite

class InterviewCancellationSpec extends FunSuite {
  import InterviewLifecycleCommand as Command
  import InterviewLifecycleEvent as Event
  import InterviewWorkflowPhase as Phase

  private val at = Instant.parse("2026-10-01T12:30:00Z")
  private val key = (suffix: String) => s"${workflowId.value}:$suffix"
  private val bothNotified = Set(InterviewParticipant.Candidate, InterviewParticipant.Recruiter)

  private def withHold(phase: InterviewWorkflowPhase) =
    workflowIn(phase, _.copy(pendingInterval = Some(replacement)))

  private def inPhase(phase: InterviewWorkflowPhase): InterviewWorkflow = phase match {
    case Phase.ProposalPending => proposalPending()
    case Phase.RescheduleHoldPending | Phase.RescheduleSwapPending | Phase.RescheduleCompensationPending =>
      withHold(phase)
    case Phase.RescheduleCancelOldPending     => workflowIn(phase, _.copy(generation = 1))
    case Phase.RescheduleNotificationsPending => workflowIn(phase, _.copy(generation = 1))
    case other                                => workflowIn(other)
  }

  /** One well-formed instance of every lifecycle event, so unlisted phase/event pairs are checked for rejection. */
  private def events(w: InterviewWorkflow): List[InterviewLifecycleEvent] = {
    val expiry = w.proposal.fold(now)(_.expiresAt)
    List(
      Event.Cancel(InterviewCancellationInitiator.Candidate, now),
      Event.RequestReschedule(now),
      Event.DismissRescheduleRequest,
      propose(),
      Event.AcceptProposal(now),
      Event.DeclineProposal(now),
      Event.WithdrawProposal(now),
      Event.ProposalExpired(expiry),
      Event.CancelConfirmed(at),
      Event.CancelOutcomeUnknown,
      Event.CancelLookupConfirmed(at),
      Event.CancelLookupAbsent,
      Event.HoldConfirmed,
      Event.HoldRejected,
      Event.HoldOutcomeUnknown,
      Event.HoldLookupFound,
      Event.HoldLookupAbsent,
      Event.SwapCommitted,
      Event.SwapRejected,
      Event.SwapDeadlineElapsed,
      Event.SwapOutcomeUnknown,
      Event.SwapLookupFound,
      Event.SwapLookupAbsent,
      Event.NotificationDelivered(InterviewParticipant.Candidate),
      Event.NotificationOutcomeUnknown(InterviewParticipant.Candidate),
      Event.NotificationLookupFound(InterviewParticipant.Candidate),
      Event.NotificationLookupAbsent(InterviewParticipant.Candidate),
      Event.RetryExhausted("provider"),
      Event.Repair
    )
  }

  private val providerCancel =
    Set("CancelConfirmed", "CancelOutcomeUnknown", "CancelLookupConfirmed", "CancelLookupAbsent")
  private val notifications = Set(
    "NotificationDelivered",
    "NotificationOutcomeUnknown",
    "NotificationLookupFound",
    "NotificationLookupAbsent"
  )
  private val exhausted = Set("RetryExhausted")

  /** The transition table: every other phase/event pair must be rejected. */
  private val permitted: Map[InterviewWorkflowPhase, Set[String]] = Map(
    Phase.Completed -> Set("Cancel", "RequestReschedule", "Propose"),
    Phase.ProposalPending -> Set("Cancel", "AcceptProposal", "DeclineProposal", "WithdrawProposal", "ProposalExpired"),
    Phase.CancelPending -> (providerCancel ++ exhausted),
    Phase.CancelNotificationsPending -> (notifications ++ exhausted),
    Phase.RescheduleHoldPending -> Set(
      "HoldConfirmed",
      "HoldRejected",
      "HoldOutcomeUnknown",
      "HoldLookupFound",
      "HoldLookupAbsent",
      "RetryExhausted"
    ),
    Phase.RescheduleSwapPending -> Set(
      "SwapCommitted",
      "SwapRejected",
      "SwapDeadlineElapsed",
      "SwapOutcomeUnknown",
      "SwapLookupFound",
      "SwapLookupAbsent",
      "RetryExhausted"
    ),
    Phase.RescheduleCancelOldPending -> (providerCancel ++ exhausted),
    Phase.RescheduleCompensationPending -> (providerCancel ++ exhausted),
    Phase.RescheduleNotificationsPending -> (notifications ++ exhausted)
  )

  test("DHW-14 only the transition table's phase and event pairs succeed; every other pair is rejected") {
    for {
      phase <- InterviewWorkflowPhase.values.toList
      w = inPhase(phase)
      event <- events(w)
    } {
      val result = decide(w, event)
      val allowed = permitted.getOrElse(phase, Set.empty).contains(event.productPrefix)
      if (allowed) {
        val decision = result.fold(error => fail(s"$phase / $event rejected with $error"), identity)
        assertEquals(decision.workflow.revision, w.revision + 1L, clues(phase, event))
      } else assert(result.isLeft, clues(phase, event, result))
    }
  }

  test("DHW-14 DismissRescheduleRequest succeeds only for a completed workflow with a request flag") {
    val flagged = completed.copy(rescheduleRequestedAt = Some(now))
    val decision = decided(flagged, Event.DismissRescheduleRequest)
    assertEquals(decision.workflow, flagged.copy(revision = revision + 1L, rescheduleRequestedAt = None))
    assertEquals(decision.commands, Nil)
    assertEquals(decide(completed, Event.DismissRescheduleRequest), Left(InterviewWorkflowError.NoRescheduleRequest))
    assertEquals(
      decide(proposalPending(), Event.DismissRescheduleRequest),
      Left(InterviewWorkflowError.InvalidTransition)
    )
  }

  test("DHW-14 decisions are pure: equal inputs give equal outputs and the input workflow is untouched") {
    val before = proposalPending()
    val event = Event.AcceptProposal(now)
    assertEquals(decide(before, event), decide(before, event))
    assertEquals(before, proposalPending())
  }

  test("DHW-14 a stale or exhausted revision is rejected before any transition") {
    val event = Event.Cancel(InterviewCancellationInitiator.Admin, now)
    assertEquals(
      InterviewLifecyclePolicy.decide(completed, revision - 1L, event),
      Left(InterviewWorkflowError.StaleRevision)
    )
    val last = completed.copy(revision = Long.MaxValue)
    assertEquals(
      InterviewLifecyclePolicy.decide(last, Long.MaxValue, event),
      Left(InterviewWorkflowError.StaleRevision)
    )
  }

  private val legacySchedulingEvents: List[InterviewWorkflowEvent] = {
    import InterviewWorkflowEvent.*
    val participants = InterviewParticipant.values.toList
    List(
      ReservationConfirmed,
      ReservationRejected,
      ReservationOutcomeUnknown,
      ReservationLookupFound,
      ReservationLookupAbsent,
      StatusCommitted,
      StatusCommitRejected,
      StatusCommitOutcomeUnknown,
      StatusLookupFound,
      StatusLookupAbsent,
      ReservationReleased,
      ReservationReleaseOutcomeUnknown,
      RetryExhausted("x")
    ) ++ participants.flatMap(p =>
      List(
        NotificationDelivered(p),
        NotificationOutcomeUnknown(p),
        NotificationLookupFound(p),
        NotificationLookupAbsent(p)
      )
    )
  }

  test("DHW-14 scheduling policy still rejects every event in the new phases") {
    for {
      phase <- InterviewWorkflowPhase.values.toList if permitted.contains(phase)
      event <- legacySchedulingEvents
    } assertEquals(
      InterviewWorkflow.decide(inPhase(phase), revision, event),
      Left(InterviewWorkflowError.InvalidTransition),
      clues(phase, event)
    )
  }

  test("DHW-14 cancel from a completed interview issues one provider cancel and leaves the interval alone") {
    val decision = decided(completed, Event.Cancel(InterviewCancellationInitiator.Candidate, now))
    assertEquals(
      decision.workflow,
      completed.copy(revision = revision + 1L, phase = Phase.CancelPending)
    )
    assertEquals(decision.commands, List(Command.CancelCalendarSlot(key("cancel:g0"))))
    assertEquals(decision.workflow.cancelledAt, None)
  }

  test("DHW-14 cancel discards an open proposal and a reschedule request") {
    val open = proposalPending().copy(rescheduleRequestedAt = Some(now))
    val decision = decided(open, Event.Cancel(InterviewCancellationInitiator.Recruiter, now))
    assertEquals(decision.workflow.phase, Phase.CancelPending)
    assertEquals((decision.workflow.proposal, decision.workflow.rescheduleRequestedAt), (None, None))
    assertEquals(decision.commands, List(Command.CancelCalendarSlot(key("cancel:g0"))))
  }

  test("DHW-14 the slot is reported cancelled only after the provider confirms, then both participants are notified") {
    val pending = workflowIn(Phase.CancelPending)
    List[(String, InterviewLifecycleEvent)](
      "confirmed" -> Event.CancelConfirmed(at),
      "lookup confirmed" -> Event.CancelLookupConfirmed(at)
    ).foreach { case (label, event) =>
      val decision = decided(pending, event)
      assertEquals(decision.workflow.phase, Phase.CancelNotificationsPending, clues(label))
      assertEquals(decision.workflow.cancelledAt, Some(at), clues(label))
      assertEquals(decision.workflow.notified, Set.empty[InterviewParticipant], clues(label))
      assertEquals(
        decision.commands,
        List(
          Command.Notify(
            InterviewNotificationKind.Cancelled,
            InterviewParticipant.Candidate,
            key("cancelled:notify:Candidate")
          ),
          Command.Notify(
            InterviewNotificationKind.Cancelled,
            InterviewParticipant.Recruiter,
            key("cancelled:notify:Recruiter")
          )
        ),
        clues(label)
      )
    }
    val unknown = decided(pending, Event.CancelOutcomeUnknown)
    assertEquals(unknown.workflow.phase, Phase.CancelPending)
    assertEquals(unknown.workflow.cancelledAt, None)
    assertEquals(unknown.commands, List(Command.LookupCalendarCancellation(key("cancel:g0"))))
    assertEquals(
      decided(pending, Event.CancelLookupAbsent).commands,
      List(Command.CancelCalendarSlot(key("cancel:g0")))
    )
  }

  test("DHW-14 cancel notifications complete the workflow only after both deliveries") {
    val notifying = workflowIn(Phase.CancelNotificationsPending, _.copy(cancelledAt = Some(at)))
    val first = decided(notifying, Event.NotificationDelivered(InterviewParticipant.Candidate))
    assertEquals(first.workflow.phase, Phase.CancelNotificationsPending)
    assertEquals(first.workflow.notified, Set(InterviewParticipant.Candidate))
    val second = decided(first.workflow, Event.NotificationLookupFound(InterviewParticipant.Recruiter))
    assertEquals(second.workflow.phase, Phase.Cancelled)
    assertEquals(second.workflow.notified, bothNotified)
    assertEquals(second.commands, Nil)
    assertEquals(
      decided(notifying, Event.NotificationOutcomeUnknown(InterviewParticipant.Recruiter)).commands,
      List(
        Command.LookupNotificationReceipt(
          InterviewNotificationKind.Cancelled,
          InterviewParticipant.Recruiter,
          key("cancelled:notify:Recruiter")
        )
      )
    )
    assertEquals(
      decided(notifying, Event.NotificationLookupAbsent(InterviewParticipant.Recruiter)).commands,
      List(
        Command.Notify(
          InterviewNotificationKind.Cancelled,
          InterviewParticipant.Recruiter,
          key("cancelled:notify:Recruiter")
        )
      )
    )
  }

  test("DHW-14 exhausted retries require repair from every pending phase and never reopen a slot") {
    permitted.filter(_._2.contains("RetryExhausted")).keys.foreach { phase =>
      val decision = decided(inPhase(phase), Event.RetryExhausted("provider"))
      assertEquals(decision.workflow.phase, Phase.RepairRequired, clues(phase))
      assertEquals(decision.commands.map(_.productPrefix), List("RequireRepair"), clues(phase))
    }
  }

  test("DHW-14 an accepted reschedule holds first, swaps once, then cancels the old slot and notifies both") {
    val hold = decided(proposalPending(), Event.AcceptProposal(now))
    assertEquals(hold.workflow.phase, Phase.RescheduleHoldPending)
    assertEquals(hold.workflow.interval, interval)
    assertEquals(hold.workflow.pendingInterval, Some(replacement))
    assertEquals(hold.workflow.proposal, None)
    assertEquals(hold.workflow.generation, 0)
    assertEquals(hold.commands, List(Command.HoldReplacementSlot(key("reserve:g1"), replacement)))

    val swap = decided(hold.workflow, Event.HoldConfirmed)
    assertEquals(swap.workflow.phase, Phase.RescheduleSwapPending)
    assertEquals(swap.workflow.interval, interval, "the live interval moves only in the guarded swap")
    assertEquals(swap.commands, List(Command.CommitRescheduledInterval(replacement, 1)))

    val cancelOld = decided(swap.workflow, Event.SwapCommitted)
    assertEquals(cancelOld.workflow.phase, Phase.RescheduleCancelOldPending)
    assertEquals(cancelOld.workflow.interval, replacement)
    assertEquals(cancelOld.workflow.pendingInterval, None)
    assertEquals(cancelOld.workflow.generation, 1)
    assertEquals(cancelOld.commands, List(Command.CancelCalendarSlot(key("cancel:g0"))))

    val notify = decided(cancelOld.workflow, Event.CancelConfirmed(at))
    assertEquals(notify.workflow.phase, Phase.RescheduleNotificationsPending)
    assertEquals(
      notify.workflow.cancelledAt,
      cancelOld.workflow.cancelledAt,
      "rescheduling does not cancel the interview"
    )
    assertEquals(
      notify.commands,
      List(
        Command.Notify(
          InterviewNotificationKind.Rescheduled,
          InterviewParticipant.Candidate,
          key("rescheduled:g1:notify:Candidate")
        ),
        Command.Notify(
          InterviewNotificationKind.Rescheduled,
          InterviewParticipant.Recruiter,
          key("rescheduled:g1:notify:Recruiter")
        )
      )
    )

    val first = decided(notify.workflow, Event.NotificationDelivered(InterviewParticipant.Candidate))
    val done = decided(first.workflow, Event.NotificationDelivered(InterviewParticipant.Recruiter))
    assertEquals(done.workflow.phase, Phase.Completed)
    assertEquals(done.workflow.interval, replacement)
  }

  test("DHW-14 unknown or absent provider outcomes retry with the same keys during a reschedule") {
    val holding = withHold(Phase.RescheduleHoldPending)
    assertEquals(
      decided(holding, Event.HoldOutcomeUnknown).commands,
      List(Command.LookupReplacementHold(key("reserve:g1")))
    )
    assertEquals(
      decided(holding, Event.HoldLookupAbsent).commands,
      List(Command.HoldReplacementSlot(key("reserve:g1"), replacement))
    )
    assertEquals(decided(holding, Event.HoldLookupFound).workflow.phase, Phase.RescheduleSwapPending)
    val swapping = withHold(Phase.RescheduleSwapPending)
    assertEquals(
      decided(swapping, Event.SwapOutcomeUnknown).commands,
      List(Command.LookupRescheduleCommitReceipt(workflowId, 1))
    )
    assertEquals(
      decided(swapping, Event.SwapLookupAbsent).commands,
      List(Command.CommitRescheduledInterval(replacement, 1))
    )
    assertEquals(decided(swapping, Event.SwapLookupFound).workflow.interval, replacement)
    val cancelOld = workflowIn(Phase.RescheduleCancelOldPending, _.copy(generation = 1, interval = replacement))
    assertEquals(
      decided(cancelOld, Event.CancelOutcomeUnknown).commands,
      List(Command.LookupCalendarCancellation(key("cancel:g0")))
    )
    assertEquals(
      decided(cancelOld, Event.CancelLookupAbsent).commands,
      List(Command.CancelCalendarSlot(key("cancel:g0")))
    )
  }

  test("DHW-22 a conflicting replacement leaves the old interval valid and notifies both participants") {
    val decision = decided(withHold(Phase.RescheduleHoldPending), Event.HoldRejected)
    assertEquals(decision.workflow.phase, Phase.Completed)
    assertEquals(decision.workflow.interval, interval)
    assertEquals(decision.workflow.pendingInterval, None)
    assertEquals(decision.workflow.generation, 0)
    assertEquals(decision.commands.map(_.productPrefix), List("Notify", "Notify"))
    assertEquals(
      decision.commands.collect { case Command.Notify(kind, participant, _) => (kind, participant) }.toSet,
      InterviewParticipant.values.toSet.map(p => (InterviewNotificationKind.RescheduleUnavailable, p))
    )
  }

  test("DHW-22 a rejected swap or missed deadline cancels the unneeded new hold and keeps the old interval") {
    List(Event.SwapRejected, Event.SwapDeadlineElapsed).foreach { event =>
      val compensating = decided(withHold(Phase.RescheduleSwapPending), event)
      assertEquals(compensating.workflow.phase, Phase.RescheduleCompensationPending, clues(event))
      assertEquals(compensating.workflow.interval, interval, clues(event))
      assertEquals(compensating.workflow.pendingInterval, Some(replacement), "both holds stay visible until cancelled")
      assertEquals(compensating.commands, List(Command.CancelCalendarSlot(key("cancel:g1"))), clues(event))
      val done = decided(compensating.workflow, Event.CancelConfirmed(at))
      assertEquals(done.workflow.phase, Phase.Completed, clues(event))
      assertEquals(done.workflow.interval, interval, clues(event))
      assertEquals(done.workflow.pendingInterval, None, clues(event))
      assertEquals(done.commands.map(_.productPrefix), List("Notify", "Notify"), clues(event))
    }
  }

  test("DHW-22 failed compensation is visible repair that keeps both holds in the workflow") {
    val failed = decided(withHold(Phase.RescheduleCompensationPending), Event.RetryExhausted("provider"))
    assertEquals(failed.workflow.phase, Phase.RepairRequired)
    assertEquals(failed.workflow.interval, interval)
    assertEquals(failed.workflow.pendingInterval, Some(replacement))
    assertEquals(failed.commands, List(Command.RequireRepair("compensation failed: provider")))
  }

  test("DHW-19 an interval that has started cannot be cancelled, requested or rescheduled") {
    val started = interval.startsAt
    List[InterviewLifecycleEvent](
      Event.Cancel(InterviewCancellationInitiator.Candidate, started),
      Event.RequestReschedule(started),
      Event.Propose(started.plusSeconds(60), started.plusSeconds(120), recruiterId, started, ttl)
    ).foreach(event =>
      assertEquals(decide(completed, event), Left(InterviewWorkflowError.InterviewAlreadyStarted), clues(event))
    )
    assertEquals(
      decide(proposalPending(), Event.Cancel(InterviewCancellationInitiator.Admin, started.plusSeconds(1))),
      Left(InterviewWorkflowError.InterviewAlreadyStarted)
    )
  }

  test("DHW-19 an already cancelled or cancelling workflow reports cancelled for cancel, request and propose") {
    List(Phase.CancelPending, Phase.CancelNotificationsPending, Phase.Cancelled).foreach { phase =>
      List[InterviewLifecycleEvent](
        Event.Cancel(InterviewCancellationInitiator.Candidate, now),
        Event.RequestReschedule(now),
        propose()
      ).foreach(event =>
        assertEquals(
          decide(workflowIn(phase), event),
          Left(InterviewWorkflowError.InterviewAlreadyCancelled),
          clues(phase, event)
        )
      )
    }
  }

  test("DHW-19 an unsettled phase reports not settled for cancel, request and propose") {
    List(
      Phase.ReservationPending,
      Phase.StatusCommitPending,
      Phase.CompensationPending,
      Phase.NotificationsPending,
      Phase.RepairRequired,
      Phase.RescheduleHoldPending,
      Phase.RescheduleSwapPending,
      Phase.RescheduleCancelOldPending,
      Phase.RescheduleNotificationsPending,
      Phase.RescheduleCompensationPending
    ).foreach { phase =>
      List[InterviewLifecycleEvent](
        Event.Cancel(InterviewCancellationInitiator.Candidate, now),
        Event.RequestReschedule(now),
        propose()
      ).foreach(event =>
        assertEquals(
          decide(inPhase(phase), event),
          Left(InterviewWorkflowError.InterviewNotSettled),
          clues(phase, event)
        )
      )
    }
  }

  test("DHW-19 an open proposal blocks a second proposal and a reschedule request") {
    List[InterviewLifecycleEvent](propose(), Event.RequestReschedule(now)).foreach(event =>
      assertEquals(decide(proposalPending(), event), Left(InterviewWorkflowError.ProposalAlreadyOpen), clues(event))
    )
  }

  test("DHW-19 accept, decline, withdraw and expiry without an open proposal report none open") {
    List(Phase.Completed, Phase.CancelPending, Phase.Cancelled, Phase.RescheduleHoldPending).foreach { phase =>
      List[InterviewLifecycleEvent](
        Event.AcceptProposal(now),
        Event.DeclineProposal(now),
        Event.WithdrawProposal(now),
        Event.ProposalExpired(now.plus(ttl.duration))
      ).foreach(event =>
        assertEquals(decide(inPhase(phase), event), Left(InterviewWorkflowError.NoOpenProposal), clues(phase, event))
      )
    }
  }

  test("DHW-19 accept and decline at or after the expiry time report an expired proposal; withdraw still closes it") {
    val expiresAt = now.plus(ttl.duration)
    List(expiresAt, expiresAt.plusSeconds(1)).foreach { late =>
      assertEquals(
        decided(proposalPending(expiresAt), Event.WithdrawProposal(late)).workflow.phase,
        Phase.Completed,
        clue(late)
      )
      List[InterviewLifecycleEvent](
        Event.AcceptProposal(late),
        Event.DeclineProposal(late)
      ).foreach(event =>
        assertEquals(
          decide(proposalPending(expiresAt), event),
          Left(InterviewWorkflowError.ProposalExpired),
          clues(late, event)
        )
      )
    }
    assert(decide(proposalPending(expiresAt), Event.AcceptProposal(expiresAt.minusMillis(1))).isRight)
  }

  test("DHW-19 invalid proposed intervals and an unchanged interval are typed outcomes") {
    assertEquals(
      decide(completed, propose(proposed = InterviewInterval(now, now.plusSeconds(60)))),
      Left(InterviewWorkflowError.StartMustBeInFuture)
    )
    assertEquals(
      decide(completed, propose(proposed = InterviewInterval(replacement.startsAt, replacement.startsAt))),
      Left(InterviewWorkflowError.EndMustFollowStart)
    )
    assertEquals(
      decide(completed, propose(proposed = interval)),
      Left(InterviewWorkflowError.RescheduleIntervalUnchanged)
    )
  }

  test("DHW-19 a typed rejection yields no decision, so no command or revision change can be persisted") {
    val rejected = decide(workflowIn(Phase.Cancelled), Event.Cancel(InterviewCancellationInitiator.Admin, now))
    assertEquals(rejected.map(_.commands), Left(InterviewWorkflowError.InterviewAlreadyCancelled))
  }

  test("cancellation feedback is a fixed, non-blank, initiator-specific text without identifiers") {
    val texts = InterviewCancellationInitiator.values.toList.map(InterviewCancellationFeedback.text)
    assertEquals(texts.distinct.size, InterviewCancellationInitiator.values.length)
    texts.foreach { text =>
      assert(text.trim.nonEmpty, clues(text))
      assert(!text.exists(_.isDigit), clues(text))
    }
    assertEquals(
      InterviewCancellationFeedback.text(InterviewCancellationInitiator.Candidate),
      "Interview cancelled by the candidate."
    )
    assertEquals(
      InterviewCancellationFeedback.text(InterviewCancellationInitiator.Recruiter),
      "Interview cancelled by the recruiter."
    )
    assertEquals(
      InterviewCancellationFeedback.text(InterviewCancellationInitiator.Admin),
      "Interview cancelled by an administrator."
    )
  }
}
