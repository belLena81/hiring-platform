package com.example.graphQL.cats.service.application

import cats.Monad
import cats.data.EitherT
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError

/** Runs one claimed command against the calendar, notification or hiring-status authority and reads the provider's
  * answer as an `InterviewResult`.
  */
private[application] final class InterviewProviderExecutor[F[_]: Monad](
    repository: InterviewWorkflowRepository[F],
    calendar: InterviewCalendarProvider[F],
    notifications: InterviewNotificationProvider[F],
    currentTime: F[java.time.Instant]
) {
  // The failure is classified, not dropped: a refusal is definitive, anything else is an outcome the lookup resolves.
  private def provider[A](operation: InterviewProviderF[F, A]): F[InterviewResult] =
    operation.fold[InterviewResult](
      {
        case InterviewProviderError.Conflict => InterviewResult.Rejected
        case _                               => InterviewResult.OutcomeUnknown
      },
      _ => InterviewResult.Succeeded
    )

  /** The one reading of a lookup: found, definitively absent, or unknown when the answer could not be read. */
  private def lookedUp[E](answer: EitherT[F, E, Boolean]): F[InterviewResult] =
    answer.fold[InterviewResult](
      _ => InterviewResult.OutcomeUnknown,
      found => if (found) InterviewResult.Found else InterviewResult.Absent
    )

  private def reservationLookup(workflowId: InterviewWorkflowId, key: String)(
      found: InterviewCalendarReservation => Boolean
  ): F[InterviewResult] =
    lookedUp(calendar.lookup(workflowId, key).map(_.exists(found)))

  private def receiptLookup(key: String): F[InterviewResult] =
    lookedUp(notifications.lookup(key).map(_.nonEmpty))

  /** A write the repository refuses as a conflict is a definitive rejection; any other failure is unknown. */
  private def repositoryResult(operation: RepositoryF[F, ?]): F[InterviewResult] =
    operation.fold[InterviewResult](
      {
        case RepositoryError.Conflict => InterviewResult.Rejected
        case _                        => InterviewResult.OutcomeUnknown
      },
      _ => InterviewResult.Succeeded
    )

  private def recipient(workflow: InterviewWorkflow, participant: InterviewParticipant) =
    if (participant == InterviewParticipant.Candidate) workflow.candidateId else workflow.recruiterId

  def execute(
      workflow: InterviewWorkflow,
      command: InterviewCommand,
      claim: ClaimedInterviewWorkflowCommand
  ): F[InterviewResult] = command match {
    case lifecycle: InterviewLifecycleCommand => currentTime.flatMap(executeLifecycle(workflow, lifecycle, claim, _))
    case scheduling: InterviewWorkflowCommand => executeScheduling(workflow, scheduling, claim)
  }

  /** Cancel and reschedule intents. The provider is the authority: a cancel is confirmed only by the provider, and any
    * unknown outcome is answered by a lookup by the same key, never by a local assumption.
    */
  private def executeLifecycle(
      workflow: InterviewWorkflow,
      command: InterviewLifecycleCommand,
      claim: ClaimedInterviewWorkflowCommand,
      now: java.time.Instant
  ): F[InterviewResult] = {
    import InterviewLifecycleCommand as C
    command match {
      case C.CancelCalendarSlot(key) =>
        calendar
          .cancel(workflow.id, key, now, Some(claim))
          .fold[InterviewResult](
            _ => InterviewResult.OutcomeUnknown,
            {
              case InterviewCalendarCancellation.Cancelled(_) | InterviewCalendarCancellation.AlreadyCancelled(_) =>
                InterviewResult.Succeeded
              // Definitive: the provider has no such reservation, so repeating the cancel or looking it up cannot help.
              case InterviewCalendarCancellation.UnknownReservation => InterviewResult.Rejected
            }
          )
      case C.LookupCalendarCancellation(key) =>
        reservationLookup(workflow.id, key)(_.releasedAt.nonEmpty)
      case C.HoldReplacementSlot(key, interval) =>
        provider(
          calendar.reserve(workflow.id, key, workflow.candidateId, workflow.recruiterId, interval, now, Some(claim))
        ).flatMap {
          // A refused hold may still be a replay of one that already succeeded.
          case InterviewResult.Rejected =>
            reservationLookup(workflow.id, key)(held => held.releasedAt.isEmpty && held.interval == interval).map {
              case InterviewResult.Found  => InterviewResult.Succeeded
              case InterviewResult.Absent => InterviewResult.Rejected
              case unknown                => unknown
            }
          case result => Monad[F].pure(result)
        }
      case C.LookupReplacementHold(key) =>
        reservationLookup(workflow.id, key)(_.releasedAt.isEmpty)
      case C.CommitRescheduledInterval(_, _) =>
        if (!now.isBefore(InterviewCalendarFence.swapDeadline(workflow))) Monad[F].pure(InterviewResult.Rejected)
        else
          repositoryResult(repository.approveRescheduledInterval(workflow, now, claim))
      case C.LookupRescheduleCommitReceipt(id, generation) =>
        lookedUp(repository.hasRescheduleApproval(id, generation))
      // Due work: the command is only claimed once the proposal's expiry time has been reached.
      case C.ExpireProposal(_)              => Monad[F].pure(InterviewResult.Succeeded)
      case C.Notify(kind, participant, key) =>
        val send = provider(
          notifications.notify(
            workflow.id,
            recipient(workflow, participant),
            participant,
            kind,
            key,
            now,
            Some(claim)
          )
        )
        // Informational messages are looked up first so a retry after a crash does not resend a recorded delivery.
        if (!InterviewCommands.isInformational(command)) send
        else
          notifications
            .lookup(key)
            .foldF[InterviewResult](
              _ => Monad[F].pure(InterviewResult.OutcomeUnknown),
              _.fold(send)(_ => Monad[F].pure(InterviewResult.Succeeded))
            )
      case C.LookupNotificationReceipt(_, _, key) => receiptLookup(key)
      case C.RequireRepair(_)                     => Monad[F].pure(InterviewResult.Rejected)
    }
  }

  private def executeScheduling(
      workflow: InterviewWorkflow,
      command: InterviewWorkflowCommand,
      claim: ClaimedInterviewWorkflowCommand
  ): F[InterviewResult] =
    currentTime.flatMap { now =>
      command match {
        case InterviewWorkflowCommand.ReserveCalendarSlot(key) =>
          def reconcile: F[InterviewResult] =
            calendar
              .lookup(workflow.id, InterviewWorkflow.reservationKey(workflow.id, 0))
              .fold[InterviewResult](
                _ => InterviewResult.OutcomeUnknown,
                reservation =>
                  if (reservation.exists(_.releasedAt.isEmpty)) InterviewResult.Succeeded else InterviewResult.Rejected
              )
          if (!now.isBefore(workflow.preCommitDeadline)) reconcile
          else
            provider(
              calendar.reserve(
                workflow.id,
                key,
                workflow.candidateId,
                workflow.recruiterId,
                workflow.interval,
                now,
                Some(claim)
              )
            ).flatMap {
              case InterviewResult.Rejected => reconcile
              case result                   => Monad[F].pure(result)
            }
        case InterviewWorkflowCommand.LookupCalendarReservation(id) =>
          reservationLookup(id, InterviewWorkflow.reservationKey(id, 0))(_.releasedAt.isEmpty)
        case InterviewWorkflowCommand.CommitAcceptedToInterview(_) =>
          repositoryResult(repository.commitHiring(workflow, now, Some(claim)))
        case InterviewWorkflowCommand.LookupStatusCommitReceipt(id) =>
          lookedUp(repository.isHiringCommitted(id))
        case InterviewWorkflowCommand.ReleaseCalendarSlot(key) => provider(calendar.release(key, now, Some(claim)))
        case InterviewWorkflowCommand.Notify(participant, key) =>
          provider(
            notifications.notify(
              workflow.id,
              recipient(workflow, participant),
              participant,
              InterviewNotificationKind.Scheduled,
              key,
              now,
              Some(claim)
            )
          )
        case InterviewWorkflowCommand.LookupNotificationReceipt(_, key) => receiptLookup(key)
        case InterviewWorkflowCommand.RequireRepair(_)                  =>
          Monad[F].pure(InterviewResult.Rejected)
      }
    }
}
