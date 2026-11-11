package com.example.graphQL.cats.service.port

import com.example.graphQL.cats.domain.workflow.{
  InterviewNotificationKind,
  InterviewNotificationSendRefusal,
  InterviewWorkflowId
}

/** The pre-send check an external notification provider must pass immediately before its client call.
  *
  * It applies the same predicates the receipt ledger applies before recording a delivery (see
  * [[com.example.graphQL.cats.domain.workflow.InterviewNotificationSendPolicy]]) but records nothing: the claim is
  * still held by the caller, the candidate, the recruiter and the initiating actor are active, the kind is allowed in
  * the workflow phase, and hiring is committed before `Scheduled`.
  *
  * Residual window (cannot be closed for an external send): a cancellation, deletion or lost claim committed between
  * this check and the client call can still produce one message.
  */
trait InterviewNotificationSendFence[F[_]] {
  def verify(
      workflowId: InterviewWorkflowId,
      kind: InterviewNotificationKind,
      execution: Option[ClaimedInterviewWorkflowCommand]
  ): F[Either[InterviewNotificationSendRefusal, Unit]]
}
