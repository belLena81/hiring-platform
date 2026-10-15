package com.example.graphQL.cats.service.application

import cats.data.EitherT
import cats.Monad
import cats.effect.Clock
import cats.effect.std.UUIDGen
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{ApplicationStatus, User, UserRole}
import com.example.graphQL.cats.domain.model.Identifiers.ApplicationId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.{ActorContext, UseCaseError}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.protocol.{UseCaseF}
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

final class InterviewSchedulingService[F[_]](using F: Monad[F], clock: Clock[F], uuids: UUIDGen[F])(
    users: UserRepository[F],
    jobs: JobRepository[F],
    applications: ApplicationRepository[F],
    workflows: InterviewWorkflowRepository[F],
    preCommitWindow: FiniteDuration,
    currentTime: F[Instant] = clock.realTimeInstant,
    nextWorkflowId: F[InterviewWorkflowId] = uuids.randomUUID.map(InterviewWorkflowId.apply),
    proposalTtl: InterviewProposalTtl = InterviewProposalTtl.Default
) {
  private val authorization = ActorAuthorization(users)

  def schedule(
      actor: ActorContext,
      applicationId: ApplicationId,
      startsAt: Instant,
      endsAt: Instant,
      idempotencyKey: UUID
  ): UseCaseF[F, InterviewWorkflow] =
    for {
      user <- authorization.resolve(actor)
      application <- UseCaseF.found(applications.find(applicationId), "application")
      job <- UseCaseF.found(jobs.find(application.jobId), "job")
      _ <- UseCaseF.ensure[F](authorization.canManage(user, job), UseCaseError.Domain(DomainError.Forbidden))
      canonicalStart = InterviewInterval.canonical(startsAt)
      canonicalEnd = InterviewInterval.canonical(endsAt)
      fingerprint = MutationReceiptFingerprint.fromCanonicalInput(
        s"${applicationId.value}|$canonicalStart|$canonicalEnd"
      )
      replay <- UseCaseF.repository(workflows.findRequest(user.id, idempotencyKey, fingerprint))
      saved <- replay match {
        case Some(existing) => EitherT.rightT[F, UseCaseError](existing)
        case None           =>
          for {
            now <- EitherT.liftF(currentTime)
            interval <- EitherT.fromEither[F](
              InterviewInterval
                .validate(startsAt, endsAt, now)
                .leftMap(UseCaseError.InterviewRule(_))
            )
            id <- EitherT.liftF(nextWorkflowId)
            acceptedDeadline = now.plusMillis(preCommitWindow.toMillis)
            deadline = if (acceptedDeadline.isBefore(interval.startsAt)) acceptedDeadline else interval.startsAt
            workflow <- EitherT.fromEither[F](
              InterviewWorkflow
                .create(
                  id,
                  application.id,
                  application.candidateId,
                  job.recruiterId,
                  interval,
                  deadline,
                  idempotencyKey,
                  application.status,
                  Some(user.id)
                )
                .leftMap(_ =>
                  UseCaseError.Domain(
                    DomainError.InvalidStatusTransition(application.status, ApplicationStatus.Interview)
                  )
                )
            )
            result <- UseCaseF.repository(
              workflows.create(workflow, InterviewWorkflow.initialCommand(workflow), idempotencyKey, fingerprint, now)
            )
            saved <- result match {
              case InterviewWorkflowAdvanceResult.Applied             => EitherT.rightT[F, UseCaseError](workflow)
              case InterviewWorkflowAdvanceResult.Duplicate(existing) => EitherT.rightT[F, UseCaseError](existing)
              case InterviewWorkflowAdvanceResult.StaleRevision       =>
                EitherT.leftT[F, InterviewWorkflow](UseCaseError.Repository(RepositoryError.Conflict))
            }
          } yield saved
      }
    } yield saved

  def inspect(actor: ActorContext, id: InterviewWorkflowId): UseCaseF[F, InterviewWorkflow] =
    authorization.resolve(actor).flatMap(visibleTo(_, id))

  /** Admin sees every workflow; others only those they participate in, and a hidden one is simply not found. */
  private def visibleTo(user: User, id: InterviewWorkflowId): UseCaseF[F, InterviewWorkflow] =
    UseCaseF
      .repository(
        if (user.role == UserRole.Admin) workflows.findForAdmin(id)
        else workflows.findForActor(id, InterviewWorkflowAccess(user.id, user.role))
      )
      .subflatMap(_.toRight(UseCaseError.Domain(DomainError.NotFound("interviewWorkflow"))))

  def repair(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ): UseCaseF[F, InterviewWorkflow] =
    for {
      admin <- authorization.requireAdmin(actor)
      workflow <- inspect(actor, id)
      now <- EitherT.liftF(currentTime)
      repaired <- UseCaseF.repository(workflows.repair(workflow, expectedRevision, idempotencyKey, now, admin.id))
    } yield repaired

  /** Cancelling is a rejection of the application (`Interview -> Rejected`, system-generated feedback): the candidate,
    * the owning recruiter and Admin may do it. The provider cancel runs afterwards, from the durable intent.
    */
  def cancel(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ): InterviewActionF[F, InterviewWorkflow] =
    act(actor, id, expectedRevision, idempotencyKey, InterviewAction.Cancel, "") { (user, now) =>
      InterviewLifecycleEvent.Cancel(InterviewCancellationInitiator.of(user.role), now)
    }

  /** The candidate flags that a different time is wanted; no slot changes. */
  def requestReschedule(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ): InterviewActionF[F, InterviewWorkflow] =
    act(actor, id, expectedRevision, idempotencyKey, InterviewAction.RequestReschedule, "") { (_, now) =>
      InterviewLifecycleEvent.RequestReschedule(now)
    }

  def dismissRescheduleRequest(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ): InterviewActionF[F, InterviewWorkflow] =
    act(actor, id, expectedRevision, idempotencyKey, InterviewAction.DismissRequest, "") { (_, _) =>
      InterviewLifecycleEvent.DismissRescheduleRequest
    }

  /** A recruiter or Admin proposes another time; it only takes effect if the candidate accepts it in time. */
  def proposeReschedule(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      startsAt: Instant,
      endsAt: Instant,
      idempotencyKey: UUID
  ): InterviewActionF[F, InterviewWorkflow] = {
    val canonicalStart = InterviewInterval.canonical(startsAt)
    val canonicalEnd = InterviewInterval.canonical(endsAt)
    act(actor, id, expectedRevision, idempotencyKey, InterviewAction.Propose, s"$canonicalStart|$canonicalEnd") {
      (user, now) => InterviewLifecycleEvent.Propose(startsAt, endsAt, user.id, now, proposalTtl)
    }
  }

  def withdrawReschedule(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ): InterviewActionF[F, InterviewWorkflow] =
    act(actor, id, expectedRevision, idempotencyKey, InterviewAction.Withdraw, "") { (_, now) =>
      InterviewLifecycleEvent.WithdrawProposal(now)
    }

  /** Only the candidate consents: acceptance holds the replacement first, then swaps and cancels the old slot. */
  def acceptReschedule(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ): InterviewActionF[F, InterviewWorkflow] =
    act(actor, id, expectedRevision, idempotencyKey, InterviewAction.Accept, "") { (_, now) =>
      InterviewLifecycleEvent.AcceptProposal(now)
    }

  def declineReschedule(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ): InterviewActionF[F, InterviewWorkflow] =
    act(actor, id, expectedRevision, idempotencyKey, InterviewAction.Decline, "") { (_, now) =>
      InterviewLifecycleEvent.DeclineProposal(now)
    }

  /** Admin only: informational notifications whose delivery budget is spent, awaiting repair. */
  def notificationRepairs(
      actor: ActorContext,
      id: InterviewWorkflowId
  ): UseCaseF[F, List[InterviewWorkflowCommandRecord]] =
    for {
      _ <- authorization.requireAdmin(actor)
      records <- UseCaseF.repository(workflows.findNotificationRepairs(id))
    } yield records

  /** Admin only: requeues every spent informational notification of the workflow with a fresh budget. */
  def repairNotifications(
      actor: ActorContext,
      id: InterviewWorkflowId,
      idempotencyKey: UUID
  ): UseCaseF[F, Int] =
    for {
      admin <- authorization.requireAdmin(actor)
      now <- EitherT.liftF(currentTime)
      repaired <- UseCaseF.repository(workflows.repairNotifications(id, idempotencyKey, now, admin.id))
    } yield repaired

  private def useCase[A](value: UseCaseF[F, A]): InterviewActionF[F, A] =
    value.leftMap(InterviewActionError.UseCase.apply)

  /** The one path of every action: resolve the trusted actor, hide workflows the actor does not participate in, deny
    * roles the action is not for, then hand the decision to the guarded repository write, which carries the participant
    * predicate, the revision and the idempotent request receipt into the transaction.
    */
  private def act(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID,
      action: InterviewAction,
      canonicalInput: String
  )(
      event: (User, Instant) => InterviewLifecycleEvent
  ): InterviewActionF[F, InterviewWorkflow] =
    for {
      user <- useCase(authorization.resolve(actor))
      _ <- useCase(visibleTo(user, id))
      now <- EitherT.liftF[F, InterviewActionError, Instant](currentTime)
      decided = event(user, now)
      _ <- EitherT.cond[F](
        InterviewActorPolicy.permits(user.role, user.id, decided),
        (),
        InterviewActionError.UseCase(UseCaseError.Domain(DomainError.Forbidden))
      )
      origin = InterviewLifecycleOrigin.Actor(
        InterviewWorkflowAccess(user.id, user.role),
        idempotencyKey,
        MutationReceiptFingerprint.fromCanonicalInput(
          s"${action.name}|${id.value}|$expectedRevision|$canonicalInput"
        )
      )
      outcome <- useCase(
        UseCaseF.repository(workflows.applyLifecycle(id, expectedRevision, decided, origin, now, None))
      )
      workflow <- EitherT.fromEither[F](outcomeToWorkflow(action, outcome))
    } yield workflow

  private def outcomeToWorkflow(
      action: InterviewAction,
      outcome: InterviewLifecycleOutcome
  ): Either[InterviewActionError, InterviewWorkflow] = outcome match {
    case InterviewLifecycleOutcome.Applied(workflow)   => Right(workflow)
    case InterviewLifecycleOutcome.Duplicate(workflow) => Right(workflow)
    case InterviewLifecycleOutcome.Rejected(error)     => Left(InterviewActionError.Workflow(error))
    case InterviewLifecycleOutcome.NotVisible          =>
      Left(InterviewActionError.UseCase(UseCaseError.Domain(DomainError.NotFound("interviewWorkflow"))))
    case InterviewLifecycleOutcome.ApplicationNotInterview(status) =>
      Left(
        InterviewActionError.UseCase(
          UseCaseError.Domain(
            DomainError.InvalidStatusTransition(
              status,
              if (action == InterviewAction.Cancel) ApplicationStatus.Rejected else ApplicationStatus.Interview
            )
          )
        )
      )
  }
}

/** The cancel and reschedule actions; which role may drive each is `InterviewActorPolicy`. */
private enum InterviewAction(val name: String) {
  case Cancel extends InterviewAction("cancel")
  case RequestReschedule extends InterviewAction("requestReschedule")
  case DismissRequest extends InterviewAction("dismissRescheduleRequest")
  case Propose extends InterviewAction("proposeReschedule")
  case Withdraw extends InterviewAction("withdrawReschedule")
  case Accept extends InterviewAction("acceptReschedule")
  case Decline extends InterviewAction("declineReschedule")
}
