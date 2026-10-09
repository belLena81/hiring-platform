package com.example.graphQL.cats.service.application

import cats.data.EitherT
import cats.effect.{Clock, IO}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.error.DomainError
import com.example.graphQL.cats.domain.model.{ApplicationStatus, User, UserRole}
import com.example.graphQL.cats.domain.model.Identifiers.ApplicationId
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.{ActorContext, SearchError, UseCaseError}
import com.example.graphQL.cats.service.auth.ActorAuthorization
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.protocol.{UseCaseIO, UseCaseIO as UseCase}
import java.time.Instant
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

final class InterviewSchedulingService(
    users: UserRepository,
    jobs: JobRepository,
    applications: ApplicationRepository,
    workflows: InterviewWorkflowRepository,
    preCommitWindow: FiniteDuration,
    currentTime: IO[Instant] = Clock[IO].realTimeInstant,
    nextWorkflowId: IO[InterviewWorkflowId] = IO.randomUUID.map(InterviewWorkflowId.apply),
    proposalTtl: InterviewProposalTtl = InterviewProposalTtl.Default
) {
  private val authorization = ActorAuthorization(users)

  def schedule(
      actor: ActorContext,
      applicationId: ApplicationId,
      startsAt: Instant,
      endsAt: Instant,
      idempotencyKey: UUID
  ): UseCaseIO[InterviewWorkflow] =
    for {
      user <- authorization.resolve(actor)
      application <- UseCase.found(applications.find(applicationId), "application")
      job <- UseCase.found(jobs.find(application.jobId), "job")
      _ <- UseCase.ensure(authorization.canManage(user, job), UseCaseError.Domain(DomainError.Forbidden))
      canonicalStart = startsAt.truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
      canonicalEnd = endsAt.truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
      fingerprint = MutationReceiptFingerprint.fromCanonicalInput(
        s"${applicationId.value}|$canonicalStart|$canonicalEnd"
      )
      replay <- UseCase.repository(workflows.findRequest(user.id, idempotencyKey, fingerprint))
      saved <- replay match {
        case Some(existing) => EitherT.rightT[IO, UseCaseError](existing)
        case None           =>
          for {
            now <- EitherT.liftF(currentTime)
            interval <- EitherT.fromEither[IO](
              InterviewInterval
                .validate(startsAt, endsAt, now)
                .leftMap(_ => UseCaseError.Search(SearchError.InvalidFilter("interviewInterval")))
            )
            id <- EitherT.liftF(nextWorkflowId)
            acceptedDeadline = now.plusMillis(preCommitWindow.toMillis)
            deadline = if (acceptedDeadline.isBefore(interval.startsAt)) acceptedDeadline else interval.startsAt
            workflow <- EitherT.fromEither[IO](
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
            result <- UseCase.repository(
              workflows.create(workflow, InterviewWorkflow.initialCommand(workflow), idempotencyKey, fingerprint, now)
            )
            saved <- result match {
              case InterviewWorkflowAdvanceResult.Applied             => EitherT.rightT[IO, UseCaseError](workflow)
              case InterviewWorkflowAdvanceResult.Duplicate(existing) => EitherT.rightT[IO, UseCaseError](existing)
              case InterviewWorkflowAdvanceResult.StaleRevision       =>
                EitherT.leftT[IO, InterviewWorkflow](UseCaseError.Repository(RepositoryError.Conflict))
            }
          } yield saved
      }
    } yield saved

  def inspect(actor: ActorContext, id: InterviewWorkflowId): UseCaseIO[InterviewWorkflow] =
    for {
      user <- authorization.resolve(actor)
      value <- UseCase.repository(
        if (user.role == UserRole.Admin) workflows.findForAdmin(id)
        else workflows.findForActor(id, InterviewWorkflowAccess(user.id, user.role))
      )
      workflow <- EitherT.fromEither[IO](value.toRight(UseCaseError.Domain(DomainError.NotFound("interviewWorkflow"))))
    } yield workflow

  def repair(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ): UseCaseIO[InterviewWorkflow] =
    for {
      user <- authorization.resolve(actor)
      _ <- EitherT.fromEither[IO](
        Either.cond(user.role == UserRole.Admin, (), UseCaseError.Domain(DomainError.Forbidden))
      )
      workflow <- inspect(actor, id)
      now <- EitherT.liftF(currentTime)
      repaired <- UseCase.repository(workflows.repair(workflow, expectedRevision, idempotencyKey, now, user.id))
    } yield repaired

  /** Cancelling is a rejection of the application (`Interview -> Rejected`, system-generated feedback): the candidate,
    * the owning recruiter and Admin may do it. The provider cancel runs afterwards, from the durable intent.
    */
  def cancel(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ): InterviewActionIO[InterviewWorkflow] =
    act(actor, id, expectedRevision, idempotencyKey, InterviewAction.Cancel, "") { (user, now) =>
      Right(InterviewLifecycleEvent.Cancel(initiatorOf(user.role), now))
    }

  /** The candidate flags that a different time is wanted; no slot changes. */
  def requestReschedule(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ): InterviewActionIO[InterviewWorkflow] =
    act(actor, id, expectedRevision, idempotencyKey, InterviewAction.RequestReschedule, "") { (_, now) =>
      Right(InterviewLifecycleEvent.RequestReschedule(now))
    }

  def dismissRescheduleRequest(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ): InterviewActionIO[InterviewWorkflow] =
    act(actor, id, expectedRevision, idempotencyKey, InterviewAction.DismissRequest, "") { (_, _) =>
      Right(InterviewLifecycleEvent.DismissRescheduleRequest)
    }

  /** A recruiter or Admin proposes another time; it only takes effect if the candidate accepts it in time. */
  def proposeReschedule(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      startsAt: Instant,
      endsAt: Instant,
      idempotencyKey: UUID
  ): InterviewActionIO[InterviewWorkflow] = {
    val canonicalStart = startsAt.truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
    val canonicalEnd = endsAt.truncatedTo(java.time.temporal.ChronoUnit.MILLIS)
    act(actor, id, expectedRevision, idempotencyKey, InterviewAction.Propose, s"$canonicalStart|$canonicalEnd") {
      (user, now) => Right(InterviewLifecycleEvent.Propose(startsAt, endsAt, user.id, now, proposalTtl))
    }
  }

  def withdrawReschedule(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ): InterviewActionIO[InterviewWorkflow] =
    act(actor, id, expectedRevision, idempotencyKey, InterviewAction.Withdraw, "") { (_, now) =>
      Right(InterviewLifecycleEvent.WithdrawProposal(now))
    }

  /** Only the candidate consents: acceptance holds the replacement first, then swaps and cancels the old slot. */
  def acceptReschedule(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ): InterviewActionIO[InterviewWorkflow] =
    act(actor, id, expectedRevision, idempotencyKey, InterviewAction.Accept, "") { (_, now) =>
      Right(InterviewLifecycleEvent.AcceptProposal(now))
    }

  def declineReschedule(
      actor: ActorContext,
      id: InterviewWorkflowId,
      expectedRevision: Long,
      idempotencyKey: UUID
  ): InterviewActionIO[InterviewWorkflow] =
    act(actor, id, expectedRevision, idempotencyKey, InterviewAction.Decline, "") { (_, now) =>
      Right(InterviewLifecycleEvent.DeclineProposal(now))
    }

  /** Admin only: informational notifications whose delivery budget is spent, awaiting repair. */
  def notificationRepairs(
      actor: ActorContext,
      id: InterviewWorkflowId
  ): UseCaseIO[List[InterviewWorkflowCommandRecord]] =
    for {
      _ <- requireAdmin(actor)
      records <- UseCase.repository(workflows.findNotificationRepairs(id))
    } yield records

  /** Admin only: requeues every spent informational notification of the workflow with a fresh budget. */
  def repairNotifications(
      actor: ActorContext,
      id: InterviewWorkflowId,
      idempotencyKey: UUID
  ): UseCaseIO[Int] =
    for {
      admin <- requireAdmin(actor)
      now <- EitherT.liftF(currentTime)
      repaired <- UseCase.repository(workflows.repairNotifications(id, idempotencyKey, now, admin.id))
    } yield repaired

  private def requireAdmin(actor: ActorContext): UseCaseIO[User] =
    authorization
      .resolve(actor)
      .flatMap(user => UseCase.ensure(user.role == UserRole.Admin, UseCaseError.Domain(DomainError.Forbidden)).as(user))

  private def initiatorOf(role: UserRole): InterviewCancellationInitiator = role match {
    case UserRole.Candidate => InterviewCancellationInitiator.Candidate
    case UserRole.Recruiter => InterviewCancellationInitiator.Recruiter
    case UserRole.Admin     => InterviewCancellationInitiator.Admin
  }

  private def useCase[A](value: UseCaseIO[A]): InterviewActionIO[A] = value.leftMap(InterviewActionError.UseCase.apply)

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
      event: (User, Instant) => Either[InterviewWorkflowError, InterviewLifecycleEvent]
  ): InterviewActionIO[InterviewWorkflow] =
    for {
      user <- useCase(authorization.resolve(actor))
      visible <- useCase(
        UseCase.repository(
          if (user.role == UserRole.Admin) workflows.findForAdmin(id)
          else workflows.findForActor(id, InterviewWorkflowAccess(user.id, user.role))
        )
      )
      _ <- EitherT.fromEither[IO](
        visible.toRight(InterviewActionError.UseCase(UseCaseError.Domain(DomainError.NotFound("interviewWorkflow"))))
      )
      now <- EitherT.liftF[IO, InterviewActionError, Instant](currentTime)
      decided <- EitherT.fromEither[IO](event(user, now).leftMap(InterviewActionError.Workflow.apply))
      _ <- EitherT.cond[IO](
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
        UseCase.repository(workflows.applyLifecycle(id, expectedRevision, decided, origin, now, None))
      )
      workflow <- EitherT.fromEither[IO](outcomeToWorkflow(action, outcome))
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
