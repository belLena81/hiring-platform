package com.example.graphQL.cats.domain.workflow.characterization

import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.domain.workflow.characterization.InterviewCharacterizationFixtures.*

/** SAG-01.5: pure command fencing, publication disposition and give-up helpers around the interview decisions. */
class InterviewCommandPolicyCharacterizationSpec extends GoldenTableSuite {
  import InterviewWorkflowPhase as Phase

  private val phases = InterviewWorkflowPhase.values.toVector
  private val kinds = InterviewNotificationKind.values.toVector
  private val codes = InterviewFailureCode.values.toVector
  private val maxAttempts = 3

  private def cancelKeys = Vector(0, 1, 3, 4).map(InterviewWorkflow.cancellationKey(workflowId, _)) :+ "unrelated-key"

  private val schedulingCommands: Vector[InterviewCommand] =
    Vector[InterviewCommand](
      InterviewWorkflowCommand.ReserveCalendarSlot(InterviewWorkflow.reservationKey(workflowId, 0)),
      InterviewWorkflowCommand.LookupCalendarReservation(workflowId),
      InterviewWorkflowCommand.CommitAcceptedToInterview(
        com.example.graphQL.cats.domain.model.ApplicationStatus.Accepted
      ),
      InterviewWorkflowCommand.LookupStatusCommitReceipt(workflowId),
      InterviewWorkflowCommand.ReleaseCalendarSlot(s"${workflowId.value}:release"),
      InterviewWorkflowCommand.RequireRepair(InterviewRepairReason.StatusNotCommitted)
    ) ++ participants.flatMap(p =>
      Vector[InterviewCommand](
        InterviewWorkflowCommand.Notify(p, InterviewWorkflow.notificationKey(workflowId, p)),
        InterviewWorkflowCommand.LookupNotificationReceipt(p, InterviewWorkflow.notificationKey(workflowId, p))
      )
    )

  private val lifecycleCommands: Vector[InterviewCommand] =
    cancelKeys.flatMap(key =>
      Vector[InterviewCommand](
        InterviewLifecycleCommand.CancelCalendarSlot(key),
        InterviewLifecycleCommand.LookupCalendarCancellation(key)
      )
    ) ++ Vector[InterviewCommand](
      InterviewLifecycleCommand.HoldReplacementSlot("hold-key", replacement),
      InterviewLifecycleCommand.LookupReplacementHold("hold-key"),
      InterviewLifecycleCommand.CommitRescheduledInterval(replacement, 1),
      InterviewLifecycleCommand.LookupRescheduleCommitReceipt(workflowId, 1),
      InterviewLifecycleCommand.ExpireProposal(proposalExpiry),
      InterviewLifecycleCommand.RequireRepair(InterviewRepairReason.CalendarReservationRejected)
    ) ++ kinds.flatMap(kind =>
      participants.flatMap(p =>
        Vector[InterviewCommand](
          InterviewLifecycleCommand.Notify(kind, p, s"notify-${kind.keyName}-$p"),
          InterviewLifecycleCommand.LookupNotificationReceipt(kind, p, s"lookup-${kind.keyName}-$p")
        )
      )
    )

  private val commands: Vector[InterviewCommand] = schedulingCommands ++ lifecycleCommands

  /** Workflow contexts: the commands' revision is taken relative to the workflow revision. */
  private val contexts: Vector[(String, InterviewWorkflow => Long, InterviewWorkflow => Boolean)] = Vector(
    ("equal", _.revision, _ => true),
    ("older", _.revision - 1L, _ => true),
    ("newer", _.revision + 1L, _ => true)
  )

  private def workflowShapes(phase: Phase): Vector[(InterviewWorkflow, Vector[String])] = Vector(
    (workflow(phase), Vector("equal", "older", "newer")),
    (workflow(phase, notified = Set(InterviewParticipant.Candidate)), Vector("equal")),
    (workflow(phase, notified = participants.toSet), Vector("equal")),
    (workflow(phase, generation = 2, skippedGenerations = 1), Vector("equal"))
  )

  private def revisionFor(name: String, w: InterviewWorkflow): Long = contexts.find(_._1 == name).get._2(w)

  private def disposition(
      w: InterviewWorkflow,
      revision: Long,
      command: InterviewCommand,
      hasResult: Boolean = false,
      resultApplied: Boolean = false,
      attempts: Int = maxAttempts
  ): InterviewPublicationDisposition =
    InterviewWorkflowPolicy.publicationDisposition(
      w,
      revision,
      command,
      hasResult,
      resultApplied,
      attempts,
      maxAttempts
    )

  private val applicability: GoldenSection = {
    val inputs = for {
      phase <- phases
      (w, relations) <- workflowShapes(phase)
      relation <- relations
      command <- commands
    } yield (w, relation, command)
    GoldenSection.of(
      "InterviewCommands.isApplicable and publicationDisposition(no result, attempts at the limit)",
      inputs
    ) { case (w, relation, command) =>
      s"${describe(w)} commandRevision=${revisionFor(relation, w)} command=$command"
    } { case (w, relation, command) =>
      val revision = revisionFor(relation, w)
      s"applicable=${InterviewCommands.isApplicable(w, revision, command)} " +
        s"disposition=${disposition(w, revision, command)}"
    }
  }

  private val attemptsAndResults: GoldenSection = {
    val representatives: Vector[InterviewCommand] = Vector(
      schedulingCommands.head,
      InterviewWorkflowCommand.Notify(InterviewParticipant.Candidate, "key"),
      InterviewLifecycleCommand.HoldReplacementSlot("hold-key", replacement),
      InterviewLifecycleCommand.Notify(InterviewNotificationKind.Cancelled, InterviewParticipant.Candidate, "key"),
      InterviewLifecycleCommand.RequireRepair(InterviewRepairReason.StatusNotCommitted)
    )
    val flags = Vector((false, false), (true, false), (true, true), (false, true))
    val inputs = for {
      phase <- phases
      command <- representatives
      (hasResult, resultApplied) <- flags
      attempts <- Vector(maxAttempts, maxAttempts + 1)
    } yield (workflow(phase), command, hasResult, resultApplied, attempts)
    GoldenSection.of(
      s"InterviewWorkflowPolicy.publicationDisposition result and attempts (maxAttempts=$maxAttempts)",
      inputs
    ) { case (w, command, hasResult, resultApplied, attempts) =>
      s"${describe(w)} commandRevision=${w.revision} command=$command hasResult=$hasResult " +
        s"resultApplied=$resultApplied attempts=$attempts"
    } { case (w, command, hasResult, resultApplied, attempts) =>
      disposition(w, w.revision, command, hasResult, resultApplied, attempts).toString
    }
  }

  private val classification: GoldenSection =
    GoldenSection.of("InterviewCommands.isInformational and isExpiry", commands)(_.toString) { command =>
      s"informational=${InterviewCommands.isInformational(command)} expiry=${InterviewCommands.isExpiry(command)}"
    }

  private val roundKinds: GoldenSection =
    GoldenSection.of("InterviewCommands.isRoundKind", kinds)(_.toString)(kind =>
      InterviewCommands.isRoundKind(kind).toString
    )

  private val giveUp: GoldenSection = {
    val inputs = for { command <- commands; code <- codes } yield (command, code)
    GoldenSection.of("InterviewCommands.giveUpEvent", inputs) { case (command, code) =>
      s"command=$command step=$code"
    } { case (command, code) =>
      InterviewCommands.giveUpEvent(command, code, now).toString
    }
  }

  test("SAG-01.5 generator covers every interview command case and notification kind") {
    assertEveryCaseGenerated(
      "InterviewWorkflowCommand",
      GoldenCases.labels[InterviewWorkflowCommand],
      commands.collect { case c: InterviewWorkflowCommand => c }
    )
    assertEveryCaseGenerated(
      "InterviewLifecycleCommand",
      GoldenCases.labels[InterviewLifecycleCommand],
      commands.collect { case c: InterviewLifecycleCommand => c }
    )
    assertEquals(
      kinds.toSet,
      commands.collect { case InterviewLifecycleCommand.Notify(kind, _, _) => kind }.toSet,
      "Notify must be generated for every notification kind"
    )
    assertEquals(
      kinds.toSet,
      commands.collect { case InterviewLifecycleCommand.LookupNotificationReceipt(kind, _, _) => kind }.toSet
    )
    assertEquals(
      participants.toSet,
      commands.collect { case InterviewWorkflowCommand.Notify(p, _) => p }.toSet,
      "scheduling notifications must be generated per participant"
    )
    assertEquals(
      InterviewPublicationDisposition.values.toSet,
      applicability.rows.map(_._2.split("disposition=").last).toSet.map(InterviewPublicationDisposition.valueOf) ++
        attemptsAndResults.rows.map(_._2).toSet.map(InterviewPublicationDisposition.valueOf),
      "the tables must exercise every publication disposition"
    )
  }

  test("SAG-01.5 command fencing, publication disposition and give-up events match the committed golden table") {
    assertGolden(
      "interview-command-policy.txt",
      Vector(applicability, attemptsAndResults, classification, roundKinds, giveUp)
    )
  }
}
