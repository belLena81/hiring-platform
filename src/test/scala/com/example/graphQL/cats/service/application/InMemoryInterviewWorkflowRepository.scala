package com.example.graphQL.cats.service.application

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.domain.model.ApplicationStatus
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.*
import java.time.Instant
import java.util.UUID

/** What a lifecycle transition committed: a test double for the guarded transaction, with the same observable rules. */
private[cats] final case class InterviewLifecycleStore(
    workflows: Map[InterviewWorkflowId, InterviewWorkflow],
    requestReceipts: Map[(UserId, UUID), (InterviewWorkflowId, String)],
    causeReceipts: Set[String],
    commands: Vector[(InterviewWorkflowId, Long, InterviewLifecycleCommand)],
    // Every application status change the cancellation transaction made: actor, previous, new, generated feedback.
    history: Vector[(UserId, ApplicationStatus, ApplicationStatus, String)]
)

/** In-memory `applyLifecycle` honoring the production contract: replay by actor and key, participant predicate,
  * revision CAS through the pure policy, the application transition on cancel, and all-or-nothing writes. Everything
  * else stays unexpected, as in the base double.
  */
private[cats] final class InMemoryInterviewWorkflowRepository(
    val store: Ref[IO, InterviewLifecycleStore],
    val applicationStatus: Ref[IO, ApplicationStatus]
) extends TestInterviewWorkflowRepository {

  private def visible(workflow: InterviewWorkflow, origin: InterviewLifecycleOrigin): Boolean = origin match {
    case InterviewLifecycleOrigin.Actor(access, _, _) =>
      access.role match {
        case com.example.graphQL.cats.domain.model.UserRole.Candidate => workflow.candidateId == access.actorId
        case com.example.graphQL.cats.domain.model.UserRole.Recruiter => workflow.recruiterId == access.actorId
        case com.example.graphQL.cats.domain.model.UserRole.Admin     => true
      }
    case InterviewLifecycleOrigin.Internal(_) => true
  }

  override def findForActor(workflowId: InterviewWorkflowId, access: InterviewWorkflowAccess) =
    RepositoryIO.lift(
      store.get.map(
        _.workflows
          .get(workflowId)
          .filter(workflow =>
            visible(
              workflow,
              InterviewLifecycleOrigin
                .Actor(access, UUID.randomUUID(), MutationReceiptFingerprint.fromCanonicalInput(""))
            )
              && access.role != com.example.graphQL.cats.domain.model.UserRole.Admin
          )
      )
    )

  override def findForAdmin(workflowId: InterviewWorkflowId) =
    RepositoryIO.lift(store.get.map(_.workflows.get(workflowId)))

  override def applyLifecycle(
      workflowId: InterviewWorkflowId,
      expectedRevision: Long,
      event: InterviewLifecycleEvent,
      origin: InterviewLifecycleOrigin,
      now: Instant,
      availableAt: Option[Instant]
  ) = RepositoryIO
    .lift(for {
      status <- applicationStatus.get
      outcome <- store.modify(state => transition(state, status, workflowId, expectedRevision, event, origin))
      _ <- outcome match {
        case (_, true) => applicationStatus.set(ApplicationStatus.Rejected)
        case _         => IO.unit
      }
    } yield outcome._1)
    .flatMap(either => RepositoryIO.fromEither(either))

  private def transition(
      state: InterviewLifecycleStore,
      status: ApplicationStatus,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      event: InterviewLifecycleEvent,
      origin: InterviewLifecycleOrigin
  ): (InterviewLifecycleStore, ((Either[RepositoryError, InterviewLifecycleOutcome]), Boolean)) = {
    def refuse(outcome: InterviewLifecycleOutcome) = (state, (Right(outcome), false))
    val replay: Option[Either[RepositoryError, InterviewLifecycleOutcome]] = origin match {
      case InterviewLifecycleOrigin.Actor(access, key, fingerprint) =>
        state.requestReceipts.get((access.actorId, key)).map {
          case (stored, existing) if stored == id && existing == fingerprint.value =>
            state.workflows
              .get(id)
              .toRight(RepositoryError.InvalidStoredData)
              .map(InterviewLifecycleOutcome.Duplicate.apply)
          case _ => Left(RepositoryError.Conflict)
        }
      case InterviewLifecycleOrigin.Internal(cause) =>
        Option.when(state.causeReceipts(cause.receiptIdentity))(
          state.workflows
            .get(id)
            .toRight(RepositoryError.InvalidStoredData)
            .map(InterviewLifecycleOutcome.Duplicate.apply)
        )
    }
    replay match {
      case Some(answer) => (state, (answer, false))
      case None         =>
        state.workflows.get(id).filter(visible(_, origin)) match {
          case None          => refuse(InterviewLifecycleOutcome.NotVisible)
          case Some(current) =>
            InterviewLifecyclePolicy.decide(current, expectedRevision, event) match {
              case Left(error)     => refuse(InterviewLifecycleOutcome.Rejected(error))
              case Right(decision) =>
                val needsInterview = event match {
                  case InterviewLifecycleEvent.Cancel(_, _) | InterviewLifecycleEvent.Propose(_, _, _, _, _) |
                      InterviewLifecycleEvent.AcceptProposal(_) | InterviewLifecycleEvent.RequestReschedule(_) =>
                    true
                  case _ => false
                }
                if (needsInterview && status != ApplicationStatus.Interview)
                  refuse(InterviewLifecycleOutcome.ApplicationNotInterview(status))
                else {
                  val cancelHistory = (event, origin) match {
                    case (InterviewLifecycleEvent.Cancel(initiator, _), InterviewLifecycleOrigin.Actor(access, _, _)) =>
                      Vector(
                        (
                          access.actorId,
                          status,
                          ApplicationStatus.Rejected,
                          InterviewCancellationFeedback.text(initiator)
                        )
                      )
                    case _ => Vector.empty
                  }
                  val next = decision.workflow
                  val receipts = origin match {
                    case InterviewLifecycleOrigin.Actor(access, key, fingerprint) =>
                      (
                        state.requestReceipts.updated((access.actorId, key), (id, fingerprint.value)),
                        state.causeReceipts
                      )
                    case InterviewLifecycleOrigin.Internal(cause) =>
                      (state.requestReceipts, state.causeReceipts + cause.receiptIdentity)
                  }
                  val noOp = next.revision == current.revision
                  val updated =
                    if (noOp) state
                    else
                      state.copy(
                        workflows = state.workflows.updated(id, next),
                        requestReceipts = receipts._1,
                        causeReceipts = receipts._2,
                        commands = state.commands ++ decision.commands.map((id, next.revision, _)),
                        history = state.history ++ cancelHistory
                      )
                  val outcome: Either[RepositoryError, InterviewLifecycleOutcome] =
                    if (next.revision == current.revision) Right(InterviewLifecycleOutcome.Duplicate(current))
                    else Right(InterviewLifecycleOutcome.Applied(next))
                  (updated, (outcome, cancelHistory.nonEmpty))
                }
            }
        }
    }
  }
}

private[cats] object InMemoryInterviewWorkflowRepository {
  def create(
      initial: InterviewWorkflow,
      status: ApplicationStatus = ApplicationStatus.Interview
  ): IO[InMemoryInterviewWorkflowRepository] =
    for {
      store <- Ref.of[IO, InterviewLifecycleStore](
        InterviewLifecycleStore(Map(initial.id -> initial), Map.empty, Set.empty, Vector.empty, Vector.empty)
      )
      applicationStatus <- Ref.of[IO, ApplicationStatus](status)
    } yield new InMemoryInterviewWorkflowRepository(store, applicationStatus)
}
