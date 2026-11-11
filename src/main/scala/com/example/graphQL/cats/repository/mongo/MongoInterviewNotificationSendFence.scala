package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.*
import com.mongodb.client.model.Projections

/** Read-only counterpart of the checks [[MongoInterviewNotificationLedger]] applies in `deliverOnce`: it reads the
  * command claim, the participants' account state, the workflow phase and the hiring commit, then applies the pure
  * [[InterviewNotificationSendPolicy]]. It writes nothing (no fence counter, no receipt), so it neither records a
  * delivery nor contends with the workflow's own transactions. A store failure is `CheckUnavailable`, never a send.
  */
final class MongoInterviewNotificationSendFence private[mongo] (store: MongoInterviewWorkflowStore)
    extends InterviewNotificationSendFence[IO] {
  import MongoInterviewFields.*
  import MongoInterviewWorkflowCodec.commandId
  import store.*

  override def verify(
      workflowId: InterviewWorkflowId,
      kind: InterviewNotificationKind,
      execution: Option[ClaimedInterviewWorkflowCommand]
  ): IO[Either[InterviewNotificationSendRefusal, Unit]] =
    MongoRepositorySupport
      .repositoryGuard(diagnostics, "interviewNotification.sendFence")(facts(workflowId, execution))
      .value
      .map {
        case Left(_)               => Left(InterviewNotificationSendRefusal.CheckUnavailable)
        case Right(None)           => Left(InterviewNotificationSendRefusal.WorkflowNotFound)
        case Right(Some(gathered)) => InterviewNotificationSendPolicy.decide(kind, gathered)
      }

  private def facts(
      workflowId: InterviewWorkflowId,
      execution: Option[ClaimedInterviewWorkflowCommand]
  ): RepositoryF[IO, Option[InterviewNotificationSendFacts]] =
    for {
      standing <- claimStanding(workflowId, execution)
      found <- loadWorkflow(workflowId, None)
      gathered <- found.traverse { workflow =>
        for {
          active <- participantsActive(participantsOf(workflow))
          committed <- isHiringCommitted(None, workflow.id)
        } yield InterviewNotificationSendFacts(standing, active, workflow.phase, committed)
      }
    } yield gathered

  /** The command is still `Executing` under this owner and token with an unexpired lease (the ledger's claim guard). */
  private def claimStanding(
      workflowId: InterviewWorkflowId,
      execution: Option[ClaimedInterviewWorkflowCommand]
  ): RepositoryF[IO, InterviewClaimStanding] = execution match {
    case None                                                 => MongoF.pure(InterviewClaimStanding.Absent)
    case Some(claim) if claim.record.workflowId != workflowId => MongoF.pure(InterviewClaimStanding.Lost)
    case Some(claim)                                          =>
      RepositoryF
        .lift(
          MongoSessionOperations.findOne(
            commands,
            None,
            MongoFilter.and(
              MongoFilter.eq(MongoFields.Id, commandId(workflowId, claim.record.stepId)),
              MongoFilter.eq(CommandStateField, InterviewWorkflowCommandState.Executing.toString),
              MongoFilter.eq(Lease.owner, claim.owner),
              MongoFilter.eq(Lease.token, claim.fencingToken.toString),
              Lease.held
            ),
            Some(Projections.include(MongoFields.Id))
          )
        )
        .map(found => if (found.nonEmpty) InterviewClaimStanding.Held else InterviewClaimStanding.Lost)
  }

  private def participantsActive(subjects: List[UserId]): RepositoryF[IO, Boolean] =
    subjects.distinct
      .traverse(id =>
        RepositoryF.lift(
          MongoSessionOperations.findOne(
            users,
            None,
            MongoFilter.and(
              MongoFilter.eq(MongoFields.Id, id.value.toString),
              MongoFilter.eq(MongoFields.AccountStatus, "Active")
            ),
            Some(Projections.include(MongoFields.Id))
          )
        )
      )
      .map(_.forall(_.nonEmpty))
}
