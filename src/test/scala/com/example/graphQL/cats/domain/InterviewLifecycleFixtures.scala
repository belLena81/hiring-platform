package com.example.graphQL.cats.domain

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import java.time.{Duration, Instant}
import java.util.UUID
import munit.Assertions

/** Shared values for the interview cancellation and reschedule specifications. */
object InterviewLifecycleFixtures extends Assertions {
  val now: Instant = Instant.parse("2026-10-01T12:00:00Z")
  val candidateId: UserId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000002"))
  val recruiterId: UserId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000003"))
  val workflowId: InterviewWorkflowId = InterviewWorkflowId(UUID.fromString("00000000-0000-0000-0000-000000000004"))
  val interval: InterviewInterval =
    InterviewInterval(Instant.parse("2026-10-04T12:00:00Z"), Instant.parse("2026-10-04T13:00:00Z"))
  val replacement: InterviewInterval =
    InterviewInterval(Instant.parse("2026-10-05T12:00:00Z"), Instant.parse("2026-10-05T13:00:00Z"))
  val ttl: InterviewProposalTtl = InterviewProposalTtl.Default
  def ttlOf(duration: Duration): InterviewProposalTtl =
    InterviewProposalTtl.from(duration).fold(error => fail(s"invalid ttl $error"), identity)
  val revision: Long = 3L

  def workflowIn(
      phase: InterviewWorkflowPhase,
      customize: InterviewWorkflow => InterviewWorkflow = identity
  ): InterviewWorkflow =
    customize(
      InterviewWorkflow(
        id = workflowId,
        applicationId = ApplicationId(UUID.fromString("00000000-0000-0000-0000-000000000001")),
        candidateId = candidateId,
        recruiterId = recruiterId,
        interval = interval,
        preCommitDeadline = Instant.parse("2026-10-01T12:05:00Z"),
        idempotencyKey = UUID.fromString("00000000-0000-0000-0000-000000000005"),
        revision = revision,
        phase = phase,
        notified = Set.empty,
        initiatedBy = recruiterId
      )
    )

  def completed: InterviewWorkflow = workflowIn(InterviewWorkflowPhase.Completed)

  def proposalPending(expiresAt: Instant = now.plus(ttl.duration)): InterviewWorkflow =
    workflowIn(
      InterviewWorkflowPhase.ProposalPending,
      _.copy(proposal = Some(InterviewRescheduleProposal(replacement, recruiterId, expiresAt)))
    )

  def decide(
      workflow: InterviewWorkflow,
      event: InterviewLifecycleEvent
  ): Either[InterviewWorkflowError, InterviewLifecycleDecision] =
    InterviewLifecyclePolicy.decide(workflow, workflow.revision, event)

  def decided(workflow: InterviewWorkflow, event: InterviewLifecycleEvent): InterviewLifecycleDecision =
    decide(workflow, event).fold(error => fail(s"unexpected $error for ${workflow.phase} / $event"), identity)

  def propose(at: Instant = now, proposed: InterviewInterval = replacement, duration: InterviewProposalTtl = ttl) =
    InterviewLifecycleEvent.Propose(proposed.startsAt, proposed.endsAt, recruiterId, at, duration)
}
