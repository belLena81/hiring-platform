package com.example.graphQL.cats.domain.workflow.characterization

import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.domain.workflow.characterization.InterviewCharacterizationFixtures.*
import java.time.{Duration, Instant}

/** SAG-01.2: the current `InterviewLifecyclePolicy.decide` output (repair resume included) for every lifecycle state
  * and event, frozen before any engine exists.
  */
class InterviewLifecycleDecisionCharacterizationSpec extends GoldenTableSuite {
  import InterviewLifecycleEvent as Event
  import InterviewWorkflowPhase as Phase

  private val phases = InterviewWorkflowPhase.values.toVector
  private val codes = InterviewFailureCode.values.toVector

  private val atStart = interval.startsAt
  private val afterStart = Instant.parse("2026-10-05T00:00:00Z")
  private val afterExpiry = Instant.parse("2026-10-03T00:00:00Z")
  private val later = InterviewInterval(Instant.parse("2026-10-06T12:00:00Z"), Instant.parse("2026-10-06T13:00:00Z"))
  private val earlier = InterviewInterval(Instant.parse("2026-10-02T12:00:00Z"), Instant.parse("2026-10-02T13:00:00Z"))

  private def ttl(duration: Duration): InterviewProposalTtl =
    InterviewProposalTtl.from(duration).fold(error => throw new IllegalStateException(s"invalid ttl $error"), identity)

  private def propose(
      candidate: InterviewInterval,
      by: com.example.graphQL.cats.domain.model.Identifiers.UserId = recruiterId,
      at: Instant = now,
      proposalTtl: InterviewProposalTtl = InterviewProposalTtl.Default
  ): InterviewLifecycleEvent = Event.Propose(candidate.startsAt, candidate.endsAt, by, at, proposalTtl)

  private val events: Vector[InterviewLifecycleEvent] =
    Vector[InterviewLifecycleEvent](
      Event.Cancel(InterviewCancellationInitiator.Candidate, now),
      Event.Cancel(InterviewCancellationInitiator.Recruiter, now),
      Event.Cancel(InterviewCancellationInitiator.Admin, now),
      Event.Cancel(InterviewCancellationInitiator.Candidate, atStart),
      Event.Cancel(InterviewCancellationInitiator.Candidate, afterStart),
      Event.RequestReschedule(now),
      Event.RequestReschedule(atStart),
      Event.RequestReschedule(afterStart),
      Event.DismissRescheduleRequest,
      propose(later),
      propose(earlier),
      propose(later, proposalTtl = ttl(Duration.ofHours(1))),
      propose(later, proposalTtl = ttl(Duration.ofDays(14))),
      propose(earlier, proposalTtl = ttl(Duration.ofDays(14))),
      propose(InterviewInterval(Instant.parse("2026-09-30T12:00:00Z"), Instant.parse("2026-09-30T13:00:00Z"))),
      propose(InterviewInterval(now, later.endsAt)),
      propose(InterviewInterval(later.startsAt, later.startsAt)),
      propose(InterviewInterval(later.startsAt, earlier.endsAt)),
      propose(interval),
      propose(
        InterviewInterval(Instant.parse("2026-10-06T12:00:00.0009Z"), Instant.parse("2026-10-06T13:00:00.0009Z"))
      ),
      propose(
        InterviewInterval(Instant.parse("2026-10-04T12:00:00.0009Z"), Instant.parse("2026-10-04T13:00:00.0009Z"))
      ),
      propose(later, at = atStart),
      propose(later, by = candidateId),
      Event.AcceptProposal(now),
      Event.AcceptProposal(proposalExpiry),
      Event.AcceptProposal(afterExpiry),
      Event.DeclineProposal(now),
      Event.DeclineProposal(proposalExpiry),
      Event.DeclineProposal(afterExpiry),
      Event.WithdrawProposal(now),
      Event.WithdrawProposal(afterExpiry),
      Event.ProposalExpired(now),
      Event.ProposalExpired(proposalExpiry),
      Event.ProposalExpired(afterExpiry),
      Event.CancelConfirmed(now),
      Event.CancelOutcomeUnknown,
      Event.CancelLookupConfirmed(now),
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
      Event.SwapLookupAbsent
    ) ++
      participants.flatMap(p =>
        Vector[InterviewLifecycleEvent](
          Event.NotificationDelivered(p),
          Event.NotificationOutcomeUnknown(p),
          Event.NotificationLookupFound(p),
          Event.NotificationLookupAbsent(p)
        )
      ) ++
      codes.map(Event.RetryExhausted.apply) :+
      Event.Repair

  private def base(phase: Phase): InterviewWorkflow = phase match {
    case Phase.ProposalPending => workflow(phase, proposal = Some(openProposal))
    case Phase.RescheduleHoldPending | Phase.RescheduleSwapPending | Phase.RescheduleCompensationPending =>
      workflow(phase, pendingInterval = Some(replacement))
    case _ => workflow(phase)
  }

  private def withGenerations(w: InterviewWorkflow): InterviewWorkflow = w.copy(generation = 2, skippedGenerations = 1)

  private val states: Vector[InterviewWorkflow] = {
    val general = phases.filter(_ != Phase.RepairRequired).flatMap(p => Vector(base(p), withGenerations(base(p))))
    val extras = Vector(
      workflow(Phase.Completed, rescheduleRequestedAt = Some(now)),
      workflow(Phase.Completed, proposal = Some(openProposal)),
      workflow(Phase.Completed, generation = 2, skippedGenerations = 1, rescheduleRequestedAt = Some(now)),
      workflow(Phase.ProposalPending),
      workflow(Phase.ProposalPending, proposal = Some(openProposal), generation = Int.MaxValue - 1),
      workflow(Phase.ProposalPending, proposal = Some(openProposal), generation = Int.MaxValue - 2),
      workflow(Phase.RescheduleHoldPending),
      workflow(Phase.RescheduleSwapPending),
      workflow(Phase.RescheduleCompensationPending),
      workflow(Phase.RescheduleSwapPending, pendingInterval = Some(replacement), cancelledAt = Some(now))
    ) ++
      Vector(Phase.NotificationsPending, Phase.CancelNotificationsPending, Phase.RescheduleNotificationsPending)
        .flatMap(p =>
          Vector(
            workflow(p, notified = Set(InterviewParticipant.Candidate)),
            workflow(p, notified = Set(InterviewParticipant.Recruiter)),
            workflow(p, notified = participants.toSet),
            workflow(p, notified = Set(InterviewParticipant.Candidate), generation = 2, skippedGenerations = 1)
          )
        )
    val repairs = (None +: phases.map(Option.apply)).flatMap { origin =>
      Vector(
        workflow(Phase.RepairRequired, repairOrigin = origin),
        workflow(Phase.RepairRequired, repairOrigin = origin, generation = 2, skippedGenerations = 1)
      )
    } ++ Vector(Phase.CancelNotificationsPending, Phase.RescheduleNotificationsPending).map(origin =>
      workflow(Phase.RepairRequired, repairOrigin = Some(origin), notified = Set(InterviewParticipant.Candidate))
    )
    general ++ extras ++ repairs
  }

  private def label(w: InterviewWorkflow, expected: Long, event: InterviewLifecycleEvent): String =
    s"${describe(w)} expected=$expected event=$event"

  private def decideAt(w: InterviewWorkflow, expected: Long, event: InterviewLifecycleEvent): String =
    renderLifecycleDecision(w, InterviewLifecyclePolicy.decide(w, expected, event))

  private val decisions: GoldenSection = {
    val inputs = for { w <- states; event <- events } yield (w, event)
    GoldenSection.of("InterviewLifecyclePolicy.decide state x event (expected revision matches)", inputs) {
      case (w, event) => label(w, w.revision, event)
    } { case (w, event) => decideAt(w, w.revision, event) }
  }

  private val revisionRule: GoldenSection = {
    val revisionEvents: Vector[InterviewLifecycleEvent] = Vector(
      Event.Cancel(InterviewCancellationInitiator.Candidate, now),
      Event.CancelConfirmed(now),
      Event.HoldConfirmed,
      Event.SwapCommitted,
      Event.NotificationDelivered(InterviewParticipant.Candidate),
      Event.RetryExhausted(InterviewFailureCode.Provider),
      Event.Repair
    )
    val representatives = phases.map {
      case Phase.RepairRequired => workflow(Phase.RepairRequired, repairOrigin = Some(Phase.CancelPending))
      case phase                => base(phase)
    }
    val revisions: Vector[(InterviewWorkflow => InterviewWorkflow, InterviewWorkflow => Long)] = Vector(
      (identity, w => w.revision - 1L),
      (identity, w => w.revision + 1L),
      (_.copy(revision = Long.MaxValue), _ => Long.MaxValue),
      (_.copy(revision = Long.MaxValue - 1L), w => w.revision),
      (_.copy(revision = 0L), _ => 0L)
    )
    val inputs = for {
      representative <- representatives
      (adjust, expectedOf) <- revisions
      event <- revisionEvents
    } yield {
      val w = adjust(representative)
      (w, expectedOf(w), event)
    }
    GoldenSection.of("InterviewLifecyclePolicy.decide revision rule", inputs) { case (w, expected, event) =>
      label(w, expected, event)
    } { case (w, expected, event) => decideAt(w, expected, event) }
  }

  test("SAG-01.2 generator covers every InterviewLifecycleEvent case, failure code and phase") {
    assertEveryCaseGenerated("InterviewLifecycleEvent", GoldenCases.labels[InterviewLifecycleEvent], events)
    assertEquals(codes.toSet, events.collect { case Event.RetryExhausted(step) => step }.toSet)
    assertEquals(
      InterviewCancellationInitiator.values.toSet,
      events.collect { case Event.Cancel(initiator, _) => initiator }.toSet
    )
    assertEquals(
      participants.toSet,
      events.collect { case Event.NotificationDelivered(p) => p }.toSet,
      "notification events must be generated per participant"
    )
    assertEquals(phases.toSet, states.map(_.phase).toSet, "every phase needs at least one lifecycle state")
    val origins = states.flatMap(_.repairOrigin).toSet
    assertEquals(phases.toSet, origins, "RepairRequired must be generated for every repair origin")
    assert(states.exists(s => s.phase == Phase.RepairRequired && s.repairOrigin.isEmpty), "repair without origin")
    assertEquals(decisions.rows.size, states.size * events.size)
  }

  test("SAG-01.2 InterviewLifecyclePolicy.decide matches the committed golden table") {
    assertGolden("interview-lifecycle-decisions.txt", Vector(decisions, revisionRule))
  }
}
