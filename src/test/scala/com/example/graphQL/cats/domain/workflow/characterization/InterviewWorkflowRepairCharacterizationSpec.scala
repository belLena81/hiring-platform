package com.example.graphQL.cats.domain.workflow.characterization

import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.domain.workflow.characterization.InterviewCharacterizationFixtures.*

/** SAG-01.5: Admin repair of a stalled scheduling workflow and the persisted repair-reason texts. */
class InterviewWorkflowRepairCharacterizationSpec extends GoldenTableSuite {

  private val phases = InterviewWorkflowPhase.values.toVector

  private val repairDecisions: GoldenSection = {
    val revisions: Vector[(String, InterviewWorkflow => Long)] = Vector(
      "matching" -> (_.revision),
      "older" -> (_.revision - 1L),
      "newer" -> (_.revision + 1L)
    )
    val inputs = for {
      phase <- phases
      notified <- notifiedSets
      hiringCommitted <- Vector(false, true)
      (relation, expectedOf) <- revisions
    } yield (workflow(phase, notified = notified), hiringCommitted, relation, expectedOf)
    GoldenSection.of("InterviewWorkflowPolicy.repair", inputs) { case (w, committed, relation, _) =>
      s"${describe(w)} hiringCommitted=$committed expected=$relation"
    } { case (w, committed, _, expectedOf) =>
      renderSchedulingDecision(w, InterviewWorkflowPolicy.repair(w, expectedOf(w), committed))
    }
  }

  private val maxRevision: GoldenSection = {
    val inputs = for { committed <- Vector(false, true); revision <- Vector(Long.MaxValue, Long.MaxValue - 1L) } yield {
      (workflow(InterviewWorkflowPhase.RepairRequired, revision = revision), committed)
    }
    GoldenSection.of("InterviewWorkflowPolicy.repair at the revision limit", inputs) { case (w, committed) =>
      s"${describe(w)} hiringCommitted=$committed expected=${w.revision}"
    } { case (w, committed) => renderSchedulingDecision(w, InterviewWorkflowPolicy.repair(w, w.revision, committed)) }
  }

  private val reasons: Vector[InterviewRepairReason] =
    Vector(InterviewRepairReason.CalendarReservationRejected, InterviewRepairReason.StatusNotCommitted) ++
      InterviewFailureCode.values.toVector.flatMap(code =>
        Vector(InterviewRepairReason.RetryExhausted(code), InterviewRepairReason.CompensationFailed(code))
      )

  private val reasonTexts: GoldenSection =
    GoldenSection.of("InterviewRepairReason.text and parse round trip", reasons)(_.toString) { reason =>
      s"text=\"${reason.text}\" parse=${InterviewRepairReason.parse(reason.text)}"
    }

  private val failureWire: GoldenSection =
    GoldenSection.of("InterviewFailureCode.wire", InterviewFailureCode.values.toVector)(_.toString)(_.wire)

  private val unknownReasonText: GoldenSection =
    GoldenSection.of(
      "InterviewRepairReason.parse of unknown text",
      Vector("", "retry exhausted: nope", "Calendar reservation rejected")
    )(text => s"\"$text\"")(text => InterviewRepairReason.parse(text).toString)

  test("SAG-01.5 generator covers every repair reason case and failure code") {
    assertEveryCaseGenerated("InterviewRepairReason", GoldenCases.labels[InterviewRepairReason], reasons)
    assertEquals(
      InterviewFailureCode.values.toSet,
      reasons.collect { case InterviewRepairReason.CompensationFailed(step) => step }.toSet
    )
    assertEquals(repairDecisions.rows.size, phases.size * notifiedSets.size * 2 * 3)
  }

  test("SAG-01.5 workflow repair and repair reasons match the committed golden table") {
    assertGolden(
      "interview-workflow-repair-decisions.txt",
      Vector(repairDecisions, maxRevision, reasonTexts, failureWire, unknownReasonText)
    )
  }
}
