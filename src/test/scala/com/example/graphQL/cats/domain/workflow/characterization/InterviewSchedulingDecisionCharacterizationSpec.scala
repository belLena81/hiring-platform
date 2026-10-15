package com.example.graphQL.cats.domain.workflow.characterization

import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.domain.workflow.characterization.InterviewCharacterizationFixtures.*

/** SAG-01.1: the current `InterviewWorkflow.decide` output for every phase and event, frozen before any engine exists.
  */
class InterviewSchedulingDecisionCharacterizationSpec extends GoldenTableSuite {
  import InterviewWorkflowEvent as Event

  private val phases = InterviewWorkflowPhase.values.toVector
  private val codes = InterviewFailureCode.values.toVector

  private val events: Vector[InterviewWorkflowEvent] =
    Vector(
      Event.ReservationConfirmed,
      Event.ReservationRejected,
      Event.ReservationOutcomeUnknown,
      Event.ReservationLookupFound,
      Event.ReservationLookupAbsent,
      Event.StatusCommitted,
      Event.StatusCommitRejected,
      Event.StatusCommitOutcomeUnknown,
      Event.StatusLookupFound,
      Event.StatusLookupAbsent,
      Event.ReservationReleased,
      Event.ReservationReleaseOutcomeUnknown
    ) ++
      participants.flatMap(p =>
        Vector(
          Event.NotificationDelivered(p),
          Event.NotificationOutcomeUnknown(p),
          Event.NotificationLookupFound(p),
          Event.NotificationLookupAbsent(p)
        )
      ) ++
      codes.map(Event.RetryExhausted.apply)

  /** One representative event per family, used for the revision-rule table. */
  private val revisionEvents: Vector[InterviewWorkflowEvent] =
    Vector(
      Event.ReservationConfirmed,
      Event.ReservationRejected,
      Event.ReservationOutcomeUnknown,
      Event.StatusCommitted,
      Event.StatusCommitRejected,
      Event.ReservationReleased,
      Event.NotificationDelivered(InterviewParticipant.Candidate),
      Event.RetryExhausted(InterviewFailureCode.Provider)
    )

  /** Fields the decision must ignore: generation, pending interval, proposal, request flag, origin, cancel time. */
  private def decorated(phase: InterviewWorkflowPhase): InterviewWorkflow =
    workflow(
      phase,
      generation = 2,
      skippedGenerations = 1,
      pendingInterval = Some(replacement),
      proposal = Some(openProposal),
      rescheduleRequestedAt = Some(now),
      repairOrigin = Some(InterviewWorkflowPhase.ReservationPending),
      cancelledAt = Some(now)
    )

  private def shapes(phase: InterviewWorkflowPhase): Vector[InterviewWorkflow] =
    notifiedSets.map(set => workflow(phase, notified = set)) :+ decorated(phase)

  private def label(w: InterviewWorkflow, expected: Long, event: InterviewWorkflowEvent): String =
    s"${describe(w)} expected=$expected event=$event"

  private def decideAt(w: InterviewWorkflow, expected: Long, event: InterviewWorkflowEvent): String =
    renderSchedulingDecision(w, InterviewWorkflow.decide(w, expected, event))

  private val decisions: GoldenSection = {
    val inputs = for {
      phase <- phases
      shape <- shapes(phase)
      event <- events
    } yield (shape, event)
    GoldenSection.of("InterviewWorkflow.decide phase x notified x event (expected revision matches)", inputs) {
      case (w, event) => label(w, w.revision, event)
    } { case (w, event) => decideAt(w, w.revision, event) }
  }

  private val revisionRule: GoldenSection = {
    val revisions: Vector[(InterviewWorkflow => InterviewWorkflow, InterviewWorkflow => Long)] = Vector(
      (identity, w => w.revision - 1L), // caller read an older revision
      (identity, w => w.revision + 1L), // caller read a newer revision
      (_.copy(revision = Long.MaxValue), _ => Long.MaxValue), // equal but not incrementable
      (_.copy(revision = Long.MaxValue - 1L), w => w.revision), // last incrementable revision
      (_.copy(revision = 0L), _ => 0L) // first revision
    )
    val inputs = for {
      phase <- phases
      (adjust, expectedOf) <- revisions
      event <- revisionEvents
    } yield (adjust(workflow(phase)), expectedOf(adjust(workflow(phase))), event)
    GoldenSection.of("InterviewWorkflow.decide revision rule", inputs) { case (w, expected, event) =>
      label(w, expected, event)
    } { case (w, expected, event) => decideAt(w, expected, event) }
  }

  test("SAG-01.1 generator covers every InterviewWorkflowEvent case and every phase") {
    assertEveryCaseGenerated(
      "InterviewWorkflowEvent",
      GoldenCases.labels[InterviewWorkflowEvent],
      events
    )
    assertEquals(
      codes.toSet,
      events.collect { case Event.RetryExhausted(step) => step }.toSet,
      "RetryExhausted must be generated for every InterviewFailureCode"
    )
    assertEquals(
      participants.size * 4,
      events.count {
        case Event.NotificationDelivered(_) | Event.NotificationOutcomeUnknown(_) | Event.NotificationLookupFound(_) |
            Event.NotificationLookupAbsent(_) =>
          true
        case _ => false
      },
      "notification events must be generated per participant"
    )
    assertEquals(decisions.rows.size, phases.size * 5 * events.size)
  }

  test("SAG-01.1 InterviewWorkflow.decide matches the committed golden table") {
    assertGolden("interview-scheduling-decisions.txt", Vector(decisions, revisionRule))
  }
}
