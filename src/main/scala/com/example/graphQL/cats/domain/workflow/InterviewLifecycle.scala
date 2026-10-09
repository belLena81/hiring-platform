package com.example.graphQL.cats.domain.workflow

import com.example.graphQL.cats.domain.model.Identifiers.UserId
import java.time.{Duration, Instant}

/** Who ends an interview. Derived by the service from trusted actor context, never from request input. */
enum InterviewCancellationInitiator {
  case Candidate, Recruiter, Admin
}

/** System-generated rejection feedback: fixed per initiator, never blank, and free of user data or identifiers. */
object InterviewCancellationFeedback {
  def text(initiator: InterviewCancellationInitiator): String = initiator match {
    case InterviewCancellationInitiator.Candidate => "Interview cancelled by the candidate."
    case InterviewCancellationInitiator.Recruiter => "Interview cancelled by the recruiter."
    case InterviewCancellationInitiator.Admin     => "Interview cancelled by an administrator."
  }
}

/** The message a participant receives about an interview change. */
enum InterviewNotificationKind {
  case Scheduled, Cancelled, Rescheduled, RescheduleProposed, RescheduleDeclined, RescheduleWithdrawn,
    RescheduleExpired, RescheduleRequested, RescheduleUnavailable

  /** Stable, kind-qualified fragment of an idempotency key. */
  def keyName: String = toString.take(1).toLowerCase + toString.drop(1)
}

/** How long a reschedule proposal may stay open: validated to 1 hour through 14 days, 72 hours by default. */
final case class InterviewProposalTtl private (duration: Duration)

object InterviewProposalTtl {
  val Min: Duration = Duration.ofHours(1)
  val Max: Duration = Duration.ofDays(14)
  val Default: InterviewProposalTtl = InterviewProposalTtl(Duration.ofHours(72))

  def fromSeconds(seconds: Long): Either[InterviewWorkflowError, InterviewProposalTtl] =
    from(Duration.ofSeconds(seconds))

  def from(duration: Duration): Either[InterviewWorkflowError, InterviewProposalTtl] =
    Either.cond(
      duration.compareTo(Min) >= 0 && duration.compareTo(Max) <= 0,
      InterviewProposalTtl(duration),
      InterviewWorkflowError.ProposalTtlOutOfRange
    )
}

/** A recruiter or Admin proposal that only takes effect if the candidate accepts it before `expiresAt`. */
final case class InterviewRescheduleProposal(interval: InterviewInterval, proposedBy: UserId, expiresAt: Instant)

/** Cancel, reschedule request and proposal inputs, provider outcomes and notification outcomes. */
enum InterviewLifecycleEvent {
  case Cancel(initiator: InterviewCancellationInitiator, now: Instant)
  case RequestReschedule(now: Instant)
  case DismissRescheduleRequest
  case Propose(startsAt: Instant, endsAt: Instant, proposedBy: UserId, now: Instant, ttl: InterviewProposalTtl)
  case AcceptProposal(now: Instant)
  case DeclineProposal(now: Instant)
  case WithdrawProposal(now: Instant)
  case ProposalExpired(now: Instant)
  case CancelConfirmed(at: Instant)
  case CancelOutcomeUnknown
  case CancelLookupConfirmed(at: Instant)
  case CancelLookupAbsent
  case HoldConfirmed
  case HoldRejected
  case HoldOutcomeUnknown
  case HoldLookupFound
  case HoldLookupAbsent
  case SwapCommitted
  case SwapRejected
  case SwapDeadlineElapsed
  case SwapOutcomeUnknown
  case SwapLookupFound
  case SwapLookupAbsent
  case NotificationDelivered(participant: InterviewParticipant)
  case NotificationOutcomeUnknown(participant: InterviewParticipant)
  case NotificationLookupFound(participant: InterviewParticipant)
  case NotificationLookupAbsent(participant: InterviewParticipant)
  case RetryExhausted(step: String)
}

/** Durable intents. None carries an application status: the service owns the cancellation status change. */
enum InterviewLifecycleCommand {
  case CancelCalendarSlot(idempotencyKey: String)
  case LookupCalendarCancellation(idempotencyKey: String)
  case HoldReplacementSlot(idempotencyKey: String, interval: InterviewInterval)
  case LookupReplacementHold(idempotencyKey: String)
  case CommitRescheduledInterval(interval: InterviewInterval, generation: Int)
  case LookupRescheduleCommitReceipt(workflowId: InterviewWorkflowId)
  case ExpireProposal(availableAt: Instant)
  case Notify(kind: InterviewNotificationKind, participant: InterviewParticipant, idempotencyKey: String)
  case LookupNotificationReceipt(
      kind: InterviewNotificationKind,
      participant: InterviewParticipant,
      idempotencyKey: String
  )
  case RequireRepair(reason: String)
}

final case class InterviewLifecycleDecision(workflow: InterviewWorkflow, commands: List[InterviewLifecycleCommand])

/** Pure cancel, reschedule request and proposal policy. The caller persists the state and commands atomically. */
object InterviewLifecyclePolicy {
  import InterviewLifecycleCommand as Command
  import InterviewLifecycleEvent as Event
  import InterviewWorkflowError as Error
  import InterviewWorkflowPhase as Phase

  private val bothParticipants = InterviewParticipant.values.toList

  def decide(
      workflow: InterviewWorkflow,
      expectedRevision: Long,
      event: InterviewLifecycleEvent
  ): Either[InterviewWorkflowError, InterviewLifecycleDecision] =
    if (workflow.revision != expectedRevision || expectedRevision == Long.MaxValue) Left(Error.StaleRevision)
    else transition(workflow, event)

  private def transition(
      w: InterviewWorkflow,
      event: InterviewLifecycleEvent
  ): Either[InterviewWorkflowError, InterviewLifecycleDecision] = {
    val revision = w.revision + 1L

    def move(commands: List[InterviewLifecycleCommand])(
        update: InterviewWorkflow => InterviewWorkflow
    ): Either[InterviewWorkflowError, InterviewLifecycleDecision] =
      Right(InterviewLifecycleDecision(update(w).copy(revision = revision), commands))

    def inform(kind: InterviewNotificationKind, recipients: List[InterviewParticipant]) =
      recipients.map(p => Command.Notify(kind, p, informationalKey(w.id, kind, revision, p)))

    def repair(prefix: String, step: String) =
      move(List(Command.RequireRepair(s"$prefix: $step")))(_.copy(phase = Phase.RepairRequired))

    def startRound(kind: InterviewNotificationKind, phase: InterviewWorkflowPhase)(
        update: InterviewWorkflow => InterviewWorkflow
    ) = {
      val next = update(w).copy(phase = phase, notified = Set.empty)
      move(bothParticipants.map(p => Command.Notify(kind, p, roundKey(next, kind, p))))(_ => next)
    }

    def round(done: InterviewWorkflowPhase)(
        deliveredBy: InterviewParticipant
    ) = {
      val delivered = w.notified + deliveredBy
      val phase = if (delivered.size == bothParticipants.size) done else w.phase
      move(Nil)(_.copy(phase = phase, notified = delivered))
    }

    // Provider-cancel family shared by cancellation, the old-slot cancel and the compensation cancel.
    def cancelOutcome(key: String, confirmed: Instant => Either[Error, InterviewLifecycleDecision]) =
      event match {
        case Event.CancelConfirmed(at)       => confirmed(at)
        case Event.CancelLookupConfirmed(at) => confirmed(at)
        case Event.CancelOutcomeUnknown      => move(List(Command.LookupCalendarCancellation(key)))(identity)
        case Event.CancelLookupAbsent        => move(List(Command.CancelCalendarSlot(key)))(identity)
        case _                               => Left(Error.InvalidTransition)
      }

    (w.phase, event) match {
      case (_, Event.Cancel(_, now)) =>
        w.phase match {
          case Phase.Completed | Phase.ProposalPending =>
            if (started(w, now)) Left(Error.InterviewAlreadyStarted)
            else
              move(List(Command.CancelCalendarSlot(InterviewWorkflow.cancellationKey(w.id, w.generation))))(
                _.copy(phase = Phase.CancelPending, proposal = None, rescheduleRequestedAt = None)
              )
          case other => Left(unavailable(other))
        }
      case (_, Event.RequestReschedule(now)) =>
        requireSettled(w, now).flatMap { _ =>
          if (w.rescheduleRequestedAt.nonEmpty) Right(InterviewLifecycleDecision(w, Nil)) // idempotent flag
          else
            move(inform(InterviewNotificationKind.RescheduleRequested, List(InterviewParticipant.Recruiter)))(
              _.copy(rescheduleRequestedAt = Some(now))
            )
        }
      case (Phase.Completed, Event.DismissRescheduleRequest) if w.rescheduleRequestedAt.nonEmpty =>
        move(Nil)(_.copy(rescheduleRequestedAt = None))
      case (_, Event.Propose(startsAt, endsAt, proposedBy, now, ttl)) =>
        for {
          _ <- requireSettled(w, now)
          interval <- InterviewInterval.validate(startsAt, endsAt, now)
          _ <- Either.cond(interval != w.interval, (), Error.RescheduleIntervalUnchanged)
          expiresAt = proposalExpiry(now, ttl.duration, w.interval.startsAt, interval.startsAt)
          decision <- move(
            List[InterviewLifecycleCommand](Command.ExpireProposal(expiresAt)) ++
              inform(InterviewNotificationKind.RescheduleProposed, List(InterviewParticipant.Candidate))
          )(
            _.copy(
              phase = Phase.ProposalPending,
              proposal = Some(InterviewRescheduleProposal(interval, proposedBy, expiresAt)),
              rescheduleRequestedAt = None
            )
          )
        } yield decision
      case (_, Event.AcceptProposal(now)) =>
        for {
          proposal <- openProposal(w, now)
          _ <- Either.cond(w.generation < Int.MaxValue, (), Error.InvalidTransition)
          decision <- move(
            List(
              Command.HoldReplacementSlot(InterviewWorkflow.reservationKey(w.id, w.generation + 1), proposal.interval)
            )
          )(_.copy(phase = Phase.RescheduleHoldPending, pendingInterval = Some(proposal.interval), proposal = None))
        } yield decision
      case (_, Event.DeclineProposal(now)) =>
        openProposal(w, now).flatMap { _ =>
          move(inform(InterviewNotificationKind.RescheduleDeclined, List(InterviewParticipant.Recruiter)))(
            _.copy(phase = Phase.Completed, proposal = None)
          )
        }
      case (_, Event.WithdrawProposal(now)) =>
        openProposal(w, now).flatMap { _ =>
          move(inform(InterviewNotificationKind.RescheduleWithdrawn, List(InterviewParticipant.Candidate)))(
            _.copy(phase = Phase.Completed, proposal = None)
          )
        }
      case (_, Event.ProposalExpired(now)) =>
        w.proposal.filter(_ => w.phase == Phase.ProposalPending).toRight(Error.NoOpenProposal).flatMap { proposal =>
          if (now.isBefore(proposal.expiresAt)) Left(Error.InvalidTransition)
          else
            move(inform(InterviewNotificationKind.RescheduleExpired, bothParticipants))(
              _.copy(phase = Phase.Completed, proposal = None)
            )
        }

      case (Phase.CancelPending, _) =>
        cancelOutcome(
          InterviewWorkflow.cancellationKey(w.id, w.generation),
          at =>
            startRound(InterviewNotificationKind.Cancelled, Phase.CancelNotificationsPending)(
              _.copy(cancelledAt = Some(at))
            )
        ).orElse(exhausted(event, "retry exhausted", repair))
      case (Phase.CancelNotificationsPending, _) =>
        notificationOutcome(w, event, InterviewNotificationKind.Cancelled, round(Phase.Cancelled), move)
          .orElse(exhausted(event, "retry exhausted", repair))

      case (Phase.RescheduleHoldPending, Event.HoldConfirmed | Event.HoldLookupFound) =>
        w.pendingInterval.toRight(Error.InvalidTransition).flatMap { interval =>
          move(List(Command.CommitRescheduledInterval(interval, w.generation + 1)))(
            _.copy(phase = Phase.RescheduleSwapPending)
          )
        }
      case (Phase.RescheduleHoldPending, Event.HoldRejected) =>
        move(inform(InterviewNotificationKind.RescheduleUnavailable, bothParticipants))(
          _.copy(phase = Phase.Completed, pendingInterval = None)
        )
      case (Phase.RescheduleHoldPending, Event.HoldOutcomeUnknown) =>
        move(List(Command.LookupReplacementHold(InterviewWorkflow.reservationKey(w.id, w.generation + 1))))(identity)
      case (Phase.RescheduleHoldPending, Event.HoldLookupAbsent) =>
        w.pendingInterval.toRight(Error.InvalidTransition).flatMap { interval =>
          move(List(Command.HoldReplacementSlot(InterviewWorkflow.reservationKey(w.id, w.generation + 1), interval)))(
            identity
          )
        }
      case (Phase.RescheduleHoldPending, Event.RetryExhausted(step)) => repair("retry exhausted", step)

      case (Phase.RescheduleSwapPending, Event.SwapCommitted | Event.SwapLookupFound) =>
        w.pendingInterval.toRight(Error.InvalidTransition).flatMap { interval =>
          move(List(Command.CancelCalendarSlot(InterviewWorkflow.cancellationKey(w.id, w.generation))))(
            _.copy(
              phase = Phase.RescheduleCancelOldPending,
              interval = interval,
              pendingInterval = None,
              generation = w.generation + 1
            )
          )
        }
      case (Phase.RescheduleSwapPending, Event.SwapOutcomeUnknown) =>
        move(List(Command.LookupRescheduleCommitReceipt(w.id)))(identity)
      case (Phase.RescheduleSwapPending, Event.SwapLookupAbsent) =>
        w.pendingInterval.toRight(Error.InvalidTransition).flatMap { interval =>
          move(List(Command.CommitRescheduledInterval(interval, w.generation + 1)))(identity)
        }
      case (Phase.RescheduleSwapPending, Event.SwapRejected | Event.SwapDeadlineElapsed) =>
        move(List(Command.CancelCalendarSlot(InterviewWorkflow.cancellationKey(w.id, w.generation + 1))))(
          _.copy(phase = Phase.RescheduleCompensationPending)
        )
      case (Phase.RescheduleSwapPending, Event.RetryExhausted(step)) => repair("retry exhausted", step)

      case (Phase.RescheduleCancelOldPending, _) =>
        cancelOutcome(
          InterviewWorkflow.cancellationKey(w.id, w.generation - 1),
          _ => startRound(InterviewNotificationKind.Rescheduled, Phase.RescheduleNotificationsPending)(identity)
        ).orElse(exhausted(event, "retry exhausted", repair))
      case (Phase.RescheduleCompensationPending, _) =>
        cancelOutcome(
          InterviewWorkflow.cancellationKey(w.id, w.generation + 1),
          _ =>
            move(inform(InterviewNotificationKind.RescheduleUnavailable, bothParticipants))(
              _.copy(phase = Phase.Completed, pendingInterval = None)
            )
        ).orElse(exhausted(event, "compensation failed", repair))
      case (Phase.RescheduleNotificationsPending, _) =>
        notificationOutcome(w, event, InterviewNotificationKind.Rescheduled, round(Phase.Completed), move)
          .orElse(exhausted(event, "retry exhausted", repair))

      case _ => Left(Error.InvalidTransition)
    }
  }

  private def notificationOutcome(
      w: InterviewWorkflow,
      event: InterviewLifecycleEvent,
      kind: InterviewNotificationKind,
      deliver: InterviewParticipant => Either[InterviewWorkflowError, InterviewLifecycleDecision],
      move: List[InterviewLifecycleCommand] => (InterviewWorkflow => InterviewWorkflow) => Either[
        InterviewWorkflowError,
        InterviewLifecycleDecision
      ]
  ): Either[InterviewWorkflowError, InterviewLifecycleDecision] = event match {
    case InterviewLifecycleEvent.NotificationDelivered(p)      => deliver(p)
    case InterviewLifecycleEvent.NotificationLookupFound(p)    => deliver(p)
    case InterviewLifecycleEvent.NotificationOutcomeUnknown(p) =>
      move(List(InterviewLifecycleCommand.LookupNotificationReceipt(kind, p, roundKey(w, kind, p))))(identity)
    case InterviewLifecycleEvent.NotificationLookupAbsent(p) =>
      move(List(InterviewLifecycleCommand.Notify(kind, p, roundKey(w, kind, p))))(identity)
    case _ => Left(InterviewWorkflowError.InvalidTransition)
  }

  private def exhausted(
      event: InterviewLifecycleEvent,
      prefix: String,
      repair: (String, String) => Either[InterviewWorkflowError, InterviewLifecycleDecision]
  ): Either[InterviewWorkflowError, InterviewLifecycleDecision] = event match {
    case InterviewLifecycleEvent.RetryExhausted(step) => repair(prefix, step)
    case _                                            => Left(InterviewWorkflowError.InvalidTransition)
  }

  private def started(w: InterviewWorkflow, now: Instant): Boolean = !w.interval.startsAt.isAfter(now)

  /** Typed reason a cancel, request or proposal cannot start from a phase that is not a stable interview. */
  private def unavailable(phase: InterviewWorkflowPhase): InterviewWorkflowError = phase match {
    case InterviewWorkflowPhase.Cancelled | InterviewWorkflowPhase.CancelPending |
        InterviewWorkflowPhase.CancelNotificationsPending =>
      InterviewWorkflowError.InterviewAlreadyCancelled
    case _ => InterviewWorkflowError.InterviewNotSettled
  }

  /** A request or new proposal needs a settled, not yet started interview with no proposal open. */
  private def requireSettled(w: InterviewWorkflow, now: Instant): Either[InterviewWorkflowError, Unit] =
    w.phase match {
      case InterviewWorkflowPhase.Completed =>
        Either.cond(!started(w, now), (), InterviewWorkflowError.InterviewAlreadyStarted)
      case InterviewWorkflowPhase.ProposalPending => Left(InterviewWorkflowError.ProposalAlreadyOpen)
      case other                                  => Left(unavailable(other))
    }

  private def openProposal(
      w: InterviewWorkflow,
      now: Instant
  ): Either[InterviewWorkflowError, InterviewRescheduleProposal] =
    w.proposal
      .filter(_ => w.phase == InterviewWorkflowPhase.ProposalPending)
      .toRight(InterviewWorkflowError.NoOpenProposal)
      .flatMap(proposal =>
        Either.cond(now.isBefore(proposal.expiresAt), proposal, InterviewWorkflowError.ProposalExpired)
      )

  /** Earliest of `now + ttl`, the current start and the proposed start; arithmetic cannot overflow. */
  private def proposalExpiry(now: Instant, ttl: Duration, currentStart: Instant, proposedStart: Instant): Instant = {
    val cap = if (currentStart.isBefore(proposedStart)) currentStart else proposedStart
    if (ttl.compareTo(Duration.between(now, cap)) >= 0) cap else now.plus(ttl)
  }

  private def informationalKey(
      id: InterviewWorkflowId,
      kind: InterviewNotificationKind,
      revision: Long,
      participant: InterviewParticipant
  ): String = s"${id.value}:${kind.keyName}:r$revision:notify:$participant"

  /** Cancellation and successful reschedule rounds have one stable key per participant. */
  private def roundKey(
      w: InterviewWorkflow,
      kind: InterviewNotificationKind,
      participant: InterviewParticipant
  ): String =
    kind match {
      case InterviewNotificationKind.Rescheduled =>
        s"${w.id.value}:${kind.keyName}:g${w.generation}:notify:$participant"
      case _ => s"${w.id.value}:${kind.keyName}:notify:$participant"
    }
}
