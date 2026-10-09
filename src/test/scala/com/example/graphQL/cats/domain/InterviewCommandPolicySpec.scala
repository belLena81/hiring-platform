package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.InterviewLifecycleFixtures.*
import com.example.graphQL.cats.domain.workflow.*
import munit.FunSuite

/** A command may only run while the workflow still needs exactly that effect. */
class InterviewCommandPolicySpec extends FunSuite {
  import InterviewLifecycleCommand as Command
  import InterviewWorkflowPhase as Phase

  private def key(suffix: String) = s"${workflowId.value}:$suffix"
  private val notified = Set(InterviewParticipant.Candidate)

  private def applicable(w: InterviewWorkflow, command: InterviewCommand, revision: Long = revision) =
    InterviewCommands.isApplicable(w, revision, command)

  test("cancel and lookup commands match the generation the phase requires cancelled") {
    val cancelling = workflowIn(Phase.CancelPending)
    assert(applicable(cancelling, Command.CancelCalendarSlot(key("cancel:g0"))))
    assert(applicable(cancelling, Command.LookupCalendarCancellation(key("cancel:g0"))))
    assert(!applicable(cancelling, Command.CancelCalendarSlot(key("cancel:g1"))), "another generation")
    assert(!applicable(cancelling, Command.CancelCalendarSlot(key("cancel:g0")), revision - 1), "an older revision")
    val old = workflowIn(Phase.RescheduleCancelOldPending, _.copy(generation = 1))
    assert(applicable(old, Command.CancelCalendarSlot(key("cancel:g0"))))
    assert(!applicable(old, Command.CancelCalendarSlot(key("cancel:g1"))), "the new hold is not to be cancelled")
    val compensating = workflowIn(Phase.RescheduleCompensationPending, _.copy(pendingInterval = Some(replacement)))
    assert(applicable(compensating, Command.CancelCalendarSlot(key("cancel:g1"))))
    assert(
      !applicable(compensating, Command.CancelCalendarSlot(key("cancel:g0"))),
      "the old hold is not to be cancelled"
    )
    assert(!applicable(workflowIn(Phase.Completed), Command.CancelCalendarSlot(key("cancel:g0"))))
  }

  test("hold, swap and expiry commands run only in their own phase and revision") {
    val holding = workflowIn(Phase.RescheduleHoldPending, _.copy(pendingInterval = Some(replacement)))
    assert(applicable(holding, Command.HoldReplacementSlot(key("reserve:g1"), replacement)))
    assert(applicable(holding, Command.LookupReplacementHold(key("reserve:g1"))))
    assert(!applicable(workflowIn(Phase.Completed), Command.HoldReplacementSlot(key("reserve:g1"), replacement)))
    val swapping = workflowIn(Phase.RescheduleSwapPending, _.copy(pendingInterval = Some(replacement)))
    assert(applicable(swapping, Command.CommitRescheduledInterval(replacement, 1)))
    assert(applicable(swapping, Command.LookupRescheduleCommitReceipt(workflowId, 1)))
    assert(!applicable(holding, Command.CommitRescheduledInterval(replacement, 1)))
    val proposing = proposalPending()
    assert(applicable(proposing, Command.ExpireProposal(proposing.proposal.map(_.expiresAt).getOrElse(now))))
    assert(!applicable(workflowIn(Phase.Completed), Command.ExpireProposal(now)), "a settled proposal never expires")
    assert(!applicable(proposing, Command.ExpireProposal(now), revision - 1), "a newer proposal supersedes it")
  }

  test("round notifications gate their phase and tolerate older revisions; delivered recipients are done") {
    val cancelNotifying = workflowIn(Phase.CancelNotificationsPending, _.copy(notified = notified))
    val toRecruiter =
      Command.Notify(
        InterviewNotificationKind.Cancelled,
        InterviewParticipant.Recruiter,
        key("cancelled:notify:Recruiter")
      )
    val toCandidate =
      Command.Notify(
        InterviewNotificationKind.Cancelled,
        InterviewParticipant.Candidate,
        key("cancelled:notify:Candidate")
      )
    assert(applicable(cancelNotifying, toRecruiter, revision - 1))
    assert(!applicable(cancelNotifying, toCandidate), "already delivered")
    assert(!applicable(workflowIn(Phase.Completed), toRecruiter))
    val rescheduled = Command.Notify(
      InterviewNotificationKind.Rescheduled,
      InterviewParticipant.Recruiter,
      key("rescheduled:g1:notify:Recruiter")
    )
    assert(applicable(workflowIn(Phase.RescheduleNotificationsPending), rescheduled))
    assert(!applicable(cancelNotifying, rescheduled), "a different round")
  }

  test("informational notifications never gate a phase and stay deliverable in every phase, repair included") {
    val informational = Command.Notify(
      InterviewNotificationKind.RescheduleDeclined,
      InterviewParticipant.Recruiter,
      key("rescheduleDeclined:r4:notify:Recruiter")
    )
    assert(InterviewCommands.isInformational(informational))
    Phase.values.foreach { phase =>
      assert(applicable(workflowIn(phase), informational, revision - 2), clue(phase))
    }
    assert(InterviewNotificationFence.permits(InterviewNotificationKind.RescheduleDeclined, Phase.RepairRequired))
    assert(
      !InterviewCommands.isInformational(
        Command.Notify(
          InterviewNotificationKind.Cancelled,
          InterviewParticipant.Candidate,
          key("cancelled:notify:Candidate")
        )
      )
    )
  }

  test("scheduling commands keep their own fencing") {
    val pending = workflowIn(Phase.ReservationPending)
    assert(applicable(pending, InterviewWorkflowCommand.LookupCalendarReservation(workflowId)))
    assert(!applicable(workflowIn(Phase.Completed), InterviewWorkflowCommand.LookupCalendarReservation(workflowId)))
  }
}
