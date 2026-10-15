package com.example.graphQL.cats.domain.workflow.characterization

import com.example.graphQL.cats.domain.model.Identifiers.{ApplicationId, UserId}
import com.example.graphQL.cats.domain.workflow.*
import java.time.Instant
import java.util.UUID

/** Fixed identifiers, instants and renderings so every characterization table is deterministic. */
object InterviewCharacterizationFixtures {
  val workflowId: InterviewWorkflowId = InterviewWorkflowId(UUID.fromString("00000000-0000-0000-0000-000000000004"))
  val applicationId: ApplicationId = ApplicationId(UUID.fromString("00000000-0000-0000-0000-000000000001"))
  val candidateId: UserId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000002"))
  val recruiterId: UserId = UserId(UUID.fromString("00000000-0000-0000-0000-000000000003"))

  val now: Instant = Instant.parse("2026-10-01T12:00:00Z")
  val interval: InterviewInterval =
    InterviewInterval(Instant.parse("2026-10-04T12:00:00Z"), Instant.parse("2026-10-04T13:00:00Z"))
  val replacement: InterviewInterval =
    InterviewInterval(Instant.parse("2026-10-05T12:00:00Z"), Instant.parse("2026-10-05T13:00:00Z"))
  val proposalExpiry: Instant = Instant.parse("2026-10-02T12:00:00Z")
  val baseRevision: Long = 3L

  val participants: Vector[InterviewParticipant] = InterviewParticipant.values.toVector
  val notifiedSets: Vector[Set[InterviewParticipant]] =
    Vector(Set.empty, Set(InterviewParticipant.Candidate), Set(InterviewParticipant.Recruiter), participants.toSet)

  def workflow(
      phase: InterviewWorkflowPhase,
      notified: Set[InterviewParticipant] = Set.empty,
      generation: Int = 0,
      skippedGenerations: Int = 0,
      pendingInterval: Option[InterviewInterval] = None,
      proposal: Option[InterviewRescheduleProposal] = None,
      rescheduleRequestedAt: Option[Instant] = None,
      repairOrigin: Option[InterviewWorkflowPhase] = None,
      cancelledAt: Option[Instant] = None,
      revision: Long = baseRevision
  ): InterviewWorkflow =
    InterviewWorkflow(
      id = workflowId,
      applicationId = applicationId,
      candidateId = candidateId,
      recruiterId = recruiterId,
      interval = interval,
      preCommitDeadline = Instant.parse("2026-10-01T12:05:00Z"),
      idempotencyKey = UUID.fromString("00000000-0000-0000-0000-000000000005"),
      revision = revision,
      phase = phase,
      notified = notified,
      initiatedBy = recruiterId,
      generation = generation,
      pendingInterval = pendingInterval,
      cancelledAt = cancelledAt,
      proposal = proposal,
      rescheduleRequestedAt = rescheduleRequestedAt,
      repairOrigin = repairOrigin,
      skippedGenerations = skippedGenerations
    )

  def openProposal: InterviewRescheduleProposal = InterviewRescheduleProposal(replacement, recruiterId, proposalExpiry)

  def participantSet(set: Set[InterviewParticipant]): String =
    participants.filter(set).mkString("[", ",", "]")

  /** Revision, phase and every field that differs from the fixture defaults, in a fixed order. */
  def describe(w: InterviewWorkflow): String =
    List(
      Some(s"r${w.revision}"),
      Some(w.phase.toString),
      Option.when(w.notified.nonEmpty)(s"notified=${participantSet(w.notified)}"),
      Option.when(w.generation != 0)(s"gen=${w.generation}"),
      Option.when(w.skippedGenerations != 0)(s"skipped=${w.skippedGenerations}"),
      Option.when(w.interval != interval)(s"interval=${w.interval.startsAt}..${w.interval.endsAt}"),
      w.pendingInterval.map(i => s"pending=${i.startsAt}..${i.endsAt}"),
      w.cancelledAt.map(at => s"cancelledAt=$at"),
      w.proposal.map(p =>
        s"proposal=${p.interval.startsAt}..${p.interval.endsAt}/by=${p.proposedBy.value}/exp=${p.expiresAt}"
      ),
      w.rescheduleRequestedAt.map(at => s"requested=$at"),
      w.repairOrigin.map(origin => s"origin=$origin")
    ).flatten.mkString(" ")

  def commands(list: List[?]): String = list.mkString("[", "; ", "]")

  /** Fails (and thereby the table) if a decision altered a field no policy is allowed to touch. */
  def requireSameIdentity(before: InterviewWorkflow, after: InterviewWorkflow): Unit =
    require(
      before.id == after.id && before.applicationId == after.applicationId && before.candidateId == after.candidateId &&
        before.recruiterId == after.recruiterId && before.preCommitDeadline == after.preCommitDeadline &&
        before.idempotencyKey == after.idempotencyKey && before.initiatedBy == after.initiatedBy,
      "a decision changed an identity field"
    )

  def renderSchedulingDecision(
      before: InterviewWorkflow,
      result: Either[InterviewWorkflowError, InterviewWorkflowDecision]
  ): String = result match {
    case Left(error)     => s"Left($error)"
    case Right(decision) =>
      requireSameIdentity(before, decision.workflow)
      s"${describe(decision.workflow)} | commands=${commands(decision.commands)}"
  }

  def renderLifecycleDecision(
      before: InterviewWorkflow,
      result: Either[InterviewWorkflowError, InterviewLifecycleDecision]
  ): String = result match {
    case Left(error)     => s"Left($error)"
    case Right(decision) =>
      requireSameIdentity(before, decision.workflow)
      s"${describe(decision.workflow)} | commands=${commands(decision.commands)}"
  }
}
