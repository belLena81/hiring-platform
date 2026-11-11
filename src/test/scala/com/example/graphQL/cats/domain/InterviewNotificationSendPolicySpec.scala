package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.workflow.*
import munit.FunSuite

/** DHW-26 / Security finding F1: the pure send policy mirrors the ledger's four checks, in order. */
class InterviewNotificationSendPolicySpec extends FunSuite {
  import InterviewNotificationSendRefusal.*

  private def facts(
      claim: InterviewClaimStanding = InterviewClaimStanding.Held,
      active: Boolean = true,
      phase: InterviewWorkflowPhase = InterviewWorkflowPhase.NotificationsPending,
      committed: Boolean = true
  ) = InterviewNotificationSendFacts(claim, active, phase, committed)

  test("DHW-26 F1 all four checks pass: Scheduled in NotificationsPending with the hiring committed") {
    assertEquals(InterviewNotificationSendPolicy.decide(InterviewNotificationKind.Scheduled, facts()), Right(()))
  }

  test("DHW-26 F1 a lost claim and a missing claim both refuse; a replay of a lost claim never sends") {
    List(InterviewClaimStanding.Lost, InterviewClaimStanding.Absent).foreach { standing =>
      assertEquals(
        InterviewNotificationSendPolicy.decide(InterviewNotificationKind.Scheduled, facts(claim = standing)),
        Left(ClaimNotHeld)
      )
    }
  }

  test("DHW-26 F1 an inactive participant refuses every kind") {
    InterviewNotificationKind.values.foreach { kind =>
      assertEquals(
        InterviewNotificationSendPolicy.decide(kind, facts(active = false)),
        Left(ParticipantInactive),
        kind.toString
      )
    }
  }

  test("DHW-26 F1 a stale Scheduled after cancellation is refused, the Cancelled notice is allowed in its phase") {
    val cancelling = InterviewWorkflowPhase.CancelNotificationsPending
    assertEquals(
      InterviewNotificationSendPolicy.decide(InterviewNotificationKind.Scheduled, facts(phase = cancelling)),
      Left(KindNotAllowedInPhase)
    )
    assertEquals(
      InterviewNotificationSendPolicy.decide(InterviewNotificationKind.Cancelled, facts(phase = cancelling)),
      Right(())
    )
  }

  test("DHW-26 F1 the policy agrees with InterviewNotificationFence.permits for every kind and phase") {
    for {
      kind <- InterviewNotificationKind.values
      phase <- InterviewWorkflowPhase.values
    } assertEquals(
      InterviewNotificationSendPolicy.decide(kind, facts(phase = phase)).isRight,
      InterviewNotificationFence.permits(kind, phase),
      s"$kind in $phase"
    )
  }

  test("DHW-26 F1 only Scheduled requires the hiring commit before it is sent") {
    assertEquals(
      InterviewNotificationSendPolicy.decide(InterviewNotificationKind.Scheduled, facts(committed = false)),
      Left(HiringNotCommitted)
    )
    for {
      kind <- InterviewNotificationKind.values.filterNot(_ == InterviewNotificationKind.Scheduled)
      phase <- InterviewWorkflowPhase.values.filter(InterviewNotificationFence.permits(kind, _))
    } assertEquals(
      InterviewNotificationSendPolicy.decide(kind, facts(phase = phase, committed = false)),
      Right(()),
      s"$kind in $phase"
    )
    assert(InterviewNotificationFence.requiresHiringCommit(InterviewNotificationKind.Scheduled))
  }

  test("DHW-26 F1 checks are applied in order: claim, participants, phase, hiring commit") {
    val allFailing = facts(InterviewClaimStanding.Lost, active = false, InterviewWorkflowPhase.Completed, false)
    assertEquals(
      InterviewNotificationSendPolicy.decide(InterviewNotificationKind.Scheduled, allFailing),
      Left(ClaimNotHeld)
    )
    assertEquals(
      InterviewNotificationSendPolicy
        .decide(InterviewNotificationKind.Scheduled, allFailing.copy(claim = InterviewClaimStanding.Held)),
      Left(ParticipantInactive)
    )
    assertEquals(
      InterviewNotificationSendPolicy.decide(
        InterviewNotificationKind.Scheduled,
        allFailing.copy(claim = InterviewClaimStanding.Held, participantsActive = true)
      ),
      Left(KindNotAllowedInPhase)
    )
  }
}
