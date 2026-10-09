package com.example.graphQL.cats.service.application

import com.example.graphQL.cats.domain.InterviewLifecycleFixtures.*
import com.example.graphQL.cats.domain.workflow.*
import munit.FunSuite

/** DHW-24: who is told about what, and when. Delivery itself (receipts, crash windows, repair) runs against the real
  * ledgers in the worker integration specs; this pins the recipient rules the executor sends from.
  */
final class InterviewNotificationDeliverySpec extends FunSuite {
  import InterviewNotificationKind as Kind
  import InterviewParticipant.{Candidate, Recruiter}
  import InterviewWorkflowPhase as Phase

  private val both = Set(Candidate, Recruiter)

  private def recipients(workflow: InterviewWorkflow, event: InterviewLifecycleEvent) =
    decided(workflow, event).commands.collect { case InterviewLifecycleCommand.Notify(kind, who, _) =>
      (kind, who)
    }.toSet

  test("DHW-24 each event tells exactly the parties the specification names, only after it is durable") {
    val cancelling = workflowIn(Phase.CancelPending)
    val rescheduling = workflowIn(Phase.RescheduleCancelOldPending, _.copy(generation = 1, interval = replacement))
    val held = workflowIn(Phase.RescheduleHoldPending, _.copy(pendingInterval = Some(replacement)))
    val swapped = workflowIn(Phase.RescheduleCompensationPending, _.copy(pendingInterval = Some(replacement)))
    val rows: List[(String, Set[(InterviewNotificationKind, InterviewParticipant)])] = List(
      "cancel confirmed" -> recipients(cancelling, InterviewLifecycleEvent.CancelConfirmed(now)),
      "proposal made" -> recipients(completed, propose()),
      "proposal declined" -> recipients(proposalPending(), InterviewLifecycleEvent.DeclineProposal(now)),
      "proposal withdrawn" -> recipients(proposalPending(), InterviewLifecycleEvent.WithdrawProposal(now)),
      "proposal expired" -> recipients(
        proposalPending(now.plusSeconds(60)),
        InterviewLifecycleEvent.ProposalExpired(now.plusSeconds(60))
      ),
      "reschedule requested" -> recipients(completed, InterviewLifecycleEvent.RequestReschedule(now)),
      "reschedule done" -> recipients(rescheduling, InterviewLifecycleEvent.CancelConfirmed(now)),
      "replacement conflicts" -> recipients(held, InterviewLifecycleEvent.HoldRejected),
      "compensation done" -> recipients(swapped, InterviewLifecycleEvent.CancelConfirmed(now))
    ).map((label, set) => label -> set)
    assertEquals(
      rows.toMap,
      Map(
        "cancel confirmed" -> both.map(Kind.Cancelled -> _),
        "proposal made" -> Set(Kind.RescheduleProposed -> Candidate),
        "proposal declined" -> Set(Kind.RescheduleDeclined -> Recruiter),
        "proposal withdrawn" -> Set(Kind.RescheduleWithdrawn -> Candidate),
        "proposal expired" -> both.map(Kind.RescheduleExpired -> _),
        "reschedule requested" -> Set(Kind.RescheduleRequested -> Recruiter),
        "reschedule done" -> both.map(Kind.Rescheduled -> _),
        "replacement conflicts" -> both.map(Kind.RescheduleUnavailable -> _),
        "compensation done" -> both.map(Kind.RescheduleUnavailable -> _)
      )
    )
  }

  test("DHW-24 nothing is announced before the provider cancel is confirmed") {
    val cancelling = workflowIn(Phase.CancelPending)
    List(
      InterviewLifecycleEvent.CancelOutcomeUnknown,
      InterviewLifecycleEvent.CancelLookupAbsent
    ).foreach(event => assertEquals(recipients(cancelling, event), Set.empty, event.toString))
    assertEquals(
      recipients(completed, InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Candidate, now)),
      Set.empty
    )
  }

  test("DHW-24 informational notifications are keyed by kind, revision and recipient so a resend is the same message") {
    val keys = decided(proposalPending(), InterviewLifecycleEvent.WithdrawProposal(now)).commands.collect {
      case InterviewLifecycleCommand.Notify(_, _, key) => key
    }
    assertEquals(keys, List(s"${workflowId.value}:rescheduleWithdrawn:r${revision + 1}:notify:Candidate"))
  }
}
