package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.model.UserRole
import com.example.graphQL.cats.domain.workflow.*
import java.time.Instant
import java.util.UUID
import munit.FunSuite

class InterviewActorPolicySpec extends FunSuite {
  private val actor = UserId(UUID.fromString("00000000-0000-0000-0000-0000000000a1"))
  private val now = Instant.parse("2026-10-10T10:00:00Z")

  test("a person drives only the user events their role allows and the system drives the others") {
    val cancel = InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.Recruiter, now)
    assert(InterviewActorPolicy.admits(Some((UserRole.Recruiter, actor)), cancel))
    assert(!InterviewActorPolicy.admits(Some((UserRole.Candidate, actor)), cancel))
    assert(!InterviewActorPolicy.admits(None, cancel))
    assert(InterviewActorPolicy.admits(None, InterviewLifecycleEvent.CancelConfirmed(now)))
    assert(!InterviewActorPolicy.admits(Some((UserRole.Admin, actor)), InterviewLifecycleEvent.CancelConfirmed(now)))
  }

  test("a cancellation initiator is valid only for the role it names") {
    UserRole.values.foreach { role =>
      val initiator = InterviewCancellationInitiator.of(role)
      assert(InterviewActorPolicy.permits(role, actor, InterviewLifecycleEvent.Cancel(initiator, now)))
      InterviewCancellationInitiator.values.filter(_ != initiator).foreach { other =>
        assert(!InterviewActorPolicy.permits(role, actor, InterviewLifecycleEvent.Cancel(other, now)))
      }
    }
  }

  test("repair reasons round trip through their persisted text and unknown text is refused") {
    val every = InterviewFailureCode.values.toList.flatMap(code =>
      List(InterviewRepairReason.RetryExhausted(code), InterviewRepairReason.CompensationFailed(code))
    ) ++ List(InterviewRepairReason.CalendarReservationRejected, InterviewRepairReason.StatusNotCommitted)
    every.foreach(reason => assertEquals(InterviewRepairReason.parse(reason.text), Some(reason)))
    assertEquals(
      InterviewRepairReason.RetryExhausted(InterviewFailureCode.PublicationExhausted).text,
      "retry exhausted: publication_exhausted"
    )
    assertEquals(InterviewRepairReason.parse("retry exhausted: something else"), None)
  }
}
