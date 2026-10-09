package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.InterviewLifecycleFixtures.*
import com.example.graphQL.cats.domain.workflow.*
import java.time.{Duration, Instant}
import munit.FunSuite

class InterviewRescheduleProposalSpec extends FunSuite {
  import InterviewLifecycleCommand as Command
  import InterviewLifecycleEvent as Event
  import InterviewWorkflowPhase as Phase

  private val expiresAt = now.plus(ttl.duration)
  private val requested = Instant.parse("2026-10-01T11:00:00Z")

  private def notifications(decision: InterviewLifecycleDecision) =
    decision.commands.collect { case Command.Notify(kind, participant, _) => (kind, participant) }

  private def assertNoProviderEffect(decision: InterviewLifecycleDecision) =
    decision.commands.foreach {
      case Command.Notify(_, _, _) | Command.ExpireProposal(_) => ()
      case other                                               => fail(s"unexpected provider command $other")
    }

  test("DHW-20 a proposal waits for the candidate: no hold, old interval untouched, expiry scheduled, candidate told") {
    val decision = decided(completed, propose())
    assertEquals(decision.workflow.phase, Phase.ProposalPending)
    assertEquals(decision.workflow.interval, interval)
    assertEquals(decision.workflow.pendingInterval, None)
    assertEquals(decision.workflow.generation, 0)
    assertEquals(decision.workflow.proposal, Some(InterviewRescheduleProposal(replacement, recruiterId, expiresAt)))
    assertEquals(decision.commands.head, Command.ExpireProposal(expiresAt))
    assertEquals(
      notifications(decision),
      List((InterviewNotificationKind.RescheduleProposed, InterviewParticipant.Candidate))
    )
    assertNoProviderEffect(decision)
  }

  test("DHW-20 the proposal expires at the earliest of now plus ttl, the current start and the proposed start") {
    val currentStart = interval.startsAt
    val farFuture = InterviewInterval(
      currentStart.plus(Duration.ofDays(30)),
      currentStart.plus(Duration.ofDays(30)).plusSeconds(3600)
    )
    val nearer = InterviewInterval(now.plusSeconds(7200), now.plusSeconds(10800))
    for {
      duration <- List(InterviewProposalTtl.Min, Duration.ofHours(72), InterviewProposalTtl.Max)
      proposed <- List(replacement, farFuture, nearer)
    } {
      val expected = List(now.plus(duration), currentStart, proposed.startsAt).min
      val decision = decided(completed, propose(proposed = proposed, duration = ttlOf(duration)))
      assertEquals(decision.workflow.proposal.map(_.expiresAt), Some(expected), clues(duration, proposed))
      assertEquals(decision.commands.head, Command.ExpireProposal(expected), clues(duration, proposed))
    }
    assertEquals(
      decided(completed, propose()).workflow.proposal.map(_.expiresAt),
      Some(expiresAt),
      "72 hours is the earliest bound here"
    )
  }

  test("DHW-20 proposal ttl is validated to 1 hour through 14 days and defaults to 72 hours") {
    assertEquals(InterviewProposalTtl.Default.duration, Duration.ofHours(72))
    List(3600L, 3601L, 259200L, 1209600L).foreach(seconds =>
      assertEquals(
        InterviewProposalTtl.fromSeconds(seconds).map(_.duration),
        Right(Duration.ofSeconds(seconds)),
        clues(seconds)
      )
    )
    List(Long.MinValue, -1L, 0L, 3599L, 1209601L, Long.MaxValue).foreach(seconds =>
      assertEquals(
        InterviewProposalTtl.fromSeconds(seconds),
        Left(InterviewWorkflowError.ProposalTtlOutOfRange),
        clues(seconds)
      )
    )
  }

  test("DHW-20 one open proposal is enforced; a proposal is possible again after decline, withdrawal or expiry") {
    val open = decided(completed, propose()).workflow
    assertEquals(decide(open, propose()), Left(InterviewWorkflowError.ProposalAlreadyOpen))
    List[InterviewLifecycleEvent](
      Event.DeclineProposal(now),
      Event.WithdrawProposal(now),
      Event.ProposalExpired(expiresAt)
    ).foreach { event =>
      val back = decided(open, event).workflow
      assertEquals(back.phase, Phase.Completed, clues(event))
      assert(decide(back, propose()).isRight, clues(event))
    }
  }

  test("DHW-20 decline, withdrawal and expiry return to Completed with the old slot intact and no provider call") {
    val open = decided(completed, propose()).workflow
    List[(InterviewLifecycleEvent, List[(InterviewNotificationKind, InterviewParticipant)])](
      Event.DeclineProposal(now) -> List(
        (InterviewNotificationKind.RescheduleDeclined, InterviewParticipant.Recruiter)
      ),
      Event.WithdrawProposal(now) -> List(
        (InterviewNotificationKind.RescheduleWithdrawn, InterviewParticipant.Candidate)
      ),
      Event.ProposalExpired(expiresAt) -> List(
        (InterviewNotificationKind.RescheduleExpired, InterviewParticipant.Candidate),
        (InterviewNotificationKind.RescheduleExpired, InterviewParticipant.Recruiter)
      )
    ).foreach { case (event, recipients) =>
      val decision = decided(open, event)
      assertEquals(decision.workflow.phase, Phase.Completed, clues(event))
      assertEquals(decision.workflow.proposal, None, clues(event))
      assertEquals(decision.workflow.interval, interval, clues(event))
      assertEquals(decision.workflow.pendingInterval, None, clues(event))
      assertEquals(decision.workflow.generation, 0, clues(event))
      assertEquals(notifications(decision), recipients, clues(event))
      assertNoProviderEffect(decision)
    }
  }

  test("DHW-20 expiry is durable work that fires only once the expiry time is reached") {
    val open = proposalPending(expiresAt)
    assertEquals(
      decide(open, Event.ProposalExpired(expiresAt.minusMillis(1))),
      Left(InterviewWorkflowError.InvalidTransition)
    )
    assertEquals(
      decided(open, Event.ProposalExpired(expiresAt.plus(Duration.ofDays(1)))).workflow.phase,
      Phase.Completed
    )
  }

  test("DHW-20 informational notification keys carry the kind and the new revision") {
    val keys = decided(completed, propose()).commands.collect { case Command.Notify(_, _, key) => key }
    assertEquals(keys, List(s"${workflowId.value}:rescheduleProposed:r${revision + 1}:notify:Candidate"))
  }

  test("DHW-20 accepting before expiry starts the hold-first flow and clears the proposal") {
    val decision = decided(proposalPending(expiresAt), Event.AcceptProposal(expiresAt.minusSeconds(1)))
    assertEquals(decision.workflow.phase, Phase.RescheduleHoldPending)
    assertEquals(decision.workflow.proposal, None)
    assertEquals(decision.workflow.interval, interval)
    assertEquals(decision.commands.map(_.productPrefix), List("HoldReplacementSlot"))
  }

  test("DHW-20 a candidate request is a flag that notifies the recruiter and changes no slot") {
    val decision = decided(completed, Event.RequestReschedule(requested))
    assertEquals(decision.workflow, completed.copy(revision = revision + 1L, rescheduleRequestedAt = Some(requested)))
    assertEquals(
      notifications(decision),
      List((InterviewNotificationKind.RescheduleRequested, InterviewParticipant.Recruiter))
    )
    assertNoProviderEffect(decision)
  }

  test("DHW-20 a repeated request is idempotent: nothing changes and nobody is notified again") {
    val flagged = decided(completed, Event.RequestReschedule(requested)).workflow
    val repeat = decided(flagged, Event.RequestReschedule(now))
    assertEquals(repeat.workflow, flagged)
    assertEquals(repeat.commands, Nil)
  }

  test("DHW-20 a request is cleared by a proposal or by dismissal, and rejected while a proposal is open") {
    val flagged = decided(completed, Event.RequestReschedule(requested)).workflow
    assertEquals(decided(flagged, propose()).workflow.rescheduleRequestedAt, None)
    val dismissed = decided(flagged, Event.DismissRescheduleRequest)
    assertEquals(dismissed.workflow.rescheduleRequestedAt, None)
    assertEquals(dismissed.workflow.phase, Phase.Completed)
    assertEquals(dismissed.commands, Nil)
    val open = decided(completed, propose()).workflow
    assertEquals(decide(open, Event.RequestReschedule(now)), Left(InterviewWorkflowError.ProposalAlreadyOpen))
  }

  test("DHW-20 cancel wins over an open proposal and a later accept finds none open") {
    val open = decided(completed, propose()).workflow
    val cancelled = decided(open, Event.Cancel(InterviewCancellationInitiator.Recruiter, now)).workflow
    assertEquals(cancelled.phase, Phase.CancelPending)
    assertEquals(decide(cancelled, Event.AcceptProposal(now)), Left(InterviewWorkflowError.NoOpenProposal))
  }

  test("DHW-20 only the first of competing exits succeeds because each exit needs the current revision") {
    val open = proposalPending(expiresAt)
    val first = decided(open, Event.DeclineProposal(now))
    assertEquals(
      InterviewLifecyclePolicy.decide(open, open.revision, Event.AcceptProposal(now)),
      InterviewLifecyclePolicy.decide(open, open.revision, Event.AcceptProposal(now))
    )
    assertEquals(
      InterviewLifecyclePolicy.decide(first.workflow, open.revision, Event.AcceptProposal(now)),
      Left(InterviewWorkflowError.StaleRevision)
    )
  }
}
