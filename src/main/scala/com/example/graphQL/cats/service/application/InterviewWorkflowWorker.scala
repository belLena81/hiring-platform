package com.example.graphQL.cats.service.application

import cats.effect.{Clock, IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.*
import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.concurrent.duration.*

final case class InterviewWorkerSettings(
    workerId: String,
    pollInterval: FiniteDuration,
    claimLease: FiniteDuration,
    providerTimeout: FiniteDuration,
    maxAttempts: Int,
    initialBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    replayWindow: FiniteDuration = 7.days
)

/** Resource ownership makes shutdown cancel the publisher before transport and database release. */
final class InterviewWorkflowWorker(
    repository: InterviewWorkflowRepository,
    calendar: InterviewCalendarProvider,
    notifications: InterviewNotificationProvider,
    settings: InterviewWorkerSettings
) {
  private def stableId(value: String): UUID = UUID.nameUUIDFromBytes(value.getBytes(StandardCharsets.UTF_8))

  def publisher(transport: InterviewTransport): Resource[IO, Unit] =
    Resource.make((publishDue(transport).attempt *> IO.sleep(settings.pollInterval)).foreverM.start)(_.cancel).void

  def publishDue(transport: InterviewTransport): IO[Unit] =
    for {
      now <- Clock[IO].realTimeInstant
      claimed <- repository
        .claimDueCommands(settings.workerId, now, now.plusMillis(settings.claimLease.toMillis), 16)
        .value
      _ <- claimed.fold(
        _ => IO.unit,
        _.traverse_ { claim =>
          repository.findForAdmin(claim.record.workflowId).value.flatMap {
            case Right(Some(workflow)) =>
              val commandId = stableId(claim.record.stepId)
              val messageId = if (claim.record.result.nonEmpty) stableId(s"$commandId:result") else commandId
              val message = InterviewMessage(
                messageId,
                workflow.id.value,
                claim.record.stepId,
                step(claim.record.command),
                claim.record.revision,
                if (claim.record.result.nonEmpty) commandId else workflow.id.value,
                workflow.preCommitDeadline,
                claim.record.result.map(value => InterviewResult.valueOf(value.toString)),
                claim.record.occurredAt
              )
              Clock[IO].realTimeInstant
                .flatMap(checkedAt => repository.authorizePublication(claim, checkedAt).value)
                .flatMap {
                  case Left(RepositoryError.Unavailable) =>
                    Clock[IO].realTimeInstant.flatMap(at =>
                      repository
                        .retry(
                          claim,
                          at,
                          at.plusMillis(settings.initialBackoff.toMillis),
                          "publication_coordination_unavailable"
                        )
                        .value
                        .void
                    )
                  case Left(_) | Right(false) => IO.unit
                  case Right(true)            =>
                    transport.publish(message).attempt.flatMap {
                      case Right(_) =>
                        Clock[IO].realTimeInstant.flatMap(at => repository.markPublished(claim, at).value.void)
                      case Left(_) =>
                        Clock[IO].realTimeInstant.flatMap { at =>
                          val delay = (settings.initialBackoff * math.pow(2.0, claim.record.attempts.toDouble - 1))
                            .min(settings.maxBackoff)
                          if (claim.record.attempts >= settings.maxAttempts)
                            repository.requireRepair(claim, at, "publication_exhausted").value.void
                          else
                            repository
                              .retry(claim, at, at.plusMillis(delay.toMillis), "publication_unavailable")
                              .value
                              .void
                        }
                    }
                }
            case _ => IO.unit
          }
        }
      )
    } yield ()

  private def quarantineMessage(message: InterviewMessage, reason: String): IO[Boolean] =
    Clock[IO].realTimeInstant.flatMap(now =>
      repository.quarantine(s"${message.workflowId}:${message.messageId}:$reason", now).value.map(_.isRight)
    )

  private def applicable(workflow: InterviewWorkflow, command: InterviewWorkflowCommandRecord): Boolean = {
    val expectedPhase = step(command.command) match {
      case InterviewStep.Reserve | InterviewStep.LookupReservation => InterviewWorkflowPhase.ReservationPending
      case InterviewStep.CommitHiring | InterviewStep.LookupCommit => InterviewWorkflowPhase.StatusCommitPending
      case InterviewStep.Release                                   => InterviewWorkflowPhase.CompensationPending
      case InterviewStep.NotifyCandidate | InterviewStep.NotifyRecruiter | InterviewStep.LookupNotifyCandidate |
          InterviewStep.LookupNotifyRecruiter =>
        InterviewWorkflowPhase.NotificationsPending
      case InterviewStep.RequireRepair => InterviewWorkflowPhase.RepairRequired
    }
    command.state != InterviewWorkflowCommandState.Superseded && workflow.phase == expectedPhase &&
    (command.revision == workflow.revision || expectedPhase == InterviewWorkflowPhase.NotificationsPending)
  }

  private def withinReplayWindow(message: InterviewMessage)(process: IO[Boolean]): IO[Boolean] =
    Clock[IO].realTimeInstant.flatMap { now =>
      if (message.occurredAt.isAfter(now)) quarantineMessage(message, "future")
      else if (message.occurredAt.isBefore(now.minusMillis(settings.replayWindow.toMillis))) {
        (for {
          workflow <- repository.findForAdmin(InterviewWorkflowId(message.workflowId))
          command <- repository.findCommand(InterviewWorkflowId(message.workflowId), message.stepId)
        } yield (command, workflow)).value.flatMap {
          case Right((Some(command), Some(workflow)))
              if applicable(workflow, command) && command.revision == message.revision &&
                command.occurredAt == message.occurredAt && step(command.command) == message.step &&
                message.deadline == workflow.preCommitDeadline &&
                message.messageId == (if (message.result.isDefined) stableId(s"${stableId(message.stepId)}:result")
                                      else stableId(message.stepId)) &&
                message.causationId == (if (message.result.isDefined) stableId(message.stepId)
                                        else workflow.id.value) =>
            InterviewWorkflow.decide(
              workflow,
              workflow.revision,
              InterviewWorkflowEvent.RetryExhausted("replay_expired")
            ) match {
              case Right((next, commands)) =>
                repository.advance(next, workflow.revision, s"expired:${message.messageId}", commands, now).value.map {
                  case Right(InterviewWorkflowAdvanceResult.StaleRevision) => false
                  case Right(_)                                            => true
                  case Left(_)                                             => false
                }
              case Left(_) => quarantineMessage(message, "expired")
            }
          case Right(_) => quarantineMessage(message, "expired")
          case Left(_)  => IO.pure(false)
        }
      } else process
    }

  def receiveCommand(message: InterviewMessage): IO[Boolean] = withinReplayWindow(message)(processCommand(message))

  private def processCommand(message: InterviewMessage): IO[Boolean] =
    (for {
      command <- repository.findCommand(InterviewWorkflowId(message.workflowId), message.stepId)
      workflow <- repository.findForAdmin(InterviewWorkflowId(message.workflowId))
    } yield (command, workflow)).value.flatMap {
      case Right((None, _)) | Right((_, None))                        => quarantineMessage(message, "unknown")
      case Right((Some(command), Some(_))) if command.result.nonEmpty => IO.pure(true)
      case Right((Some(command), Some(workflow)))
          if applicable(workflow, command) && command.revision == message.revision && step(
            command.command
          ) == message.step && message.result.isEmpty &&
            message.messageId == stableId(message.stepId) && message.causationId == workflow.id.value &&
            message.occurredAt == command.occurredAt && message.deadline == workflow.preCommitDeadline =>
        Clock[IO].realTimeInstant.flatMap { claimAt =>
          repository
            .claimExecution(command, settings.workerId, claimAt, claimAt.plusMillis(settings.claimLease.toMillis))
            .value
            .flatMap {
              case Left(_)            => IO.pure(false)
              case Right(None)        => IO.pure(true)
              case Right(Some(claim)) => {
                execute(workflow, command.command, claim)
                  .timeoutTo(settings.providerTimeout, IO.pure(InterviewResult.OutcomeUnknown))
                  .flatMap { result =>
                    Clock[IO].realTimeInstant.flatMap(at =>
                      repository
                        .recordResult(claim, InterviewCommandResult.valueOf(result.toString), at)
                        .value
                        .map(_.isRight)
                    )
                  }
              }
            }
        }
      case Right(_) => quarantineMessage(message, "inconsistent")
      case Left(_)  => IO.pure(false)
    }

  def receiveResult(message: InterviewMessage): IO[Boolean] = withinReplayWindow(message)(processResult(message))

  private def processResult(message: InterviewMessage): IO[Boolean] =
    (for {
      workflow <- repository.findForAdmin(InterviewWorkflowId(message.workflowId))
      command <- repository.findCommand(InterviewWorkflowId(message.workflowId), message.stepId)
    } yield (workflow, command)).value.flatMap {
      case Right((None, _)) => quarantineMessage(message, "unknown")
      case Right((Some(workflow), Some(command)))
          if applicable(workflow, command) && command.revision == message.revision && step(
            command.command
          ) == message.step && message.result.nonEmpty &&
            message.messageId == stableId(s"${stableId(message.stepId)}:result") &&
            message.occurredAt == command.occurredAt && message.deadline == workflow.preCommitDeadline &&
            (message.revision == workflow.revision ||
              (workflow.phase == InterviewWorkflowPhase.NotificationsPending && Set(
                InterviewStep.NotifyCandidate,
                InterviewStep.NotifyRecruiter,
                InterviewStep.LookupNotifyCandidate,
                InterviewStep.LookupNotifyRecruiter
              ).contains(message.step))) &&
            message.causationId == stableId(message.stepId) && command.result
              .map(_.toString) == message.result.map(_.toString) =>
        repository.attemptCount(workflow.id, command.command).value.flatMap {
          case Left(_)      => IO.pure(false)
          case Right(count) => {
            val selected =
              if (
                count >= settings.maxAttempts && (message.result.contains(
                  InterviewResult.OutcomeUnknown
                ) || message.result.contains(InterviewResult.Absent))
              )
                Some(InterviewWorkflowEvent.RetryExhausted("provider"))
              else event(message)
            selected.flatMap(ev => InterviewWorkflow.decide(workflow, workflow.revision, ev).toOption) match {
              case None                   => IO.pure(true)
              case Some((next, commands)) =>
                Clock[IO].realTimeInstant.flatMap { now =>
                  val retrying = message.result
                    .contains(InterviewResult.OutcomeUnknown) || message.result.contains(InterviewResult.Absent)
                  val delay = (settings.initialBackoff * math.pow(2.0, count.toDouble - 1.0)).min(settings.maxBackoff)
                  val available = if (retrying) Some(now.plusMillis(delay.toMillis)) else None
                  repository
                    .advance(next, workflow.revision, message.messageId.toString, commands, now, available)
                    .value
                    .map {
                      case Right(InterviewWorkflowAdvanceResult.StaleRevision) => false
                      case Right(_)                                            => true
                      case Left(_)                                             => false
                    }
                }
            }
          }
        }
      case Right(_) => quarantineMessage(message, "inconsistent")
      case Left(_)  => IO.pure(false)
    }

  private def execute(
      workflow: InterviewWorkflow,
      command: InterviewWorkflowCommand,
      claim: ClaimedInterviewWorkflowCommand
  ): IO[InterviewResult] = {
    def provider[A](operation: InterviewProviderIO[A]): IO[InterviewResult] = operation.value.map {
      case Right(_)                              => InterviewResult.Succeeded
      case Left(InterviewProviderError.Conflict) => InterviewResult.Rejected
      case Left(_)                               => InterviewResult.OutcomeUnknown
    }
    Clock[IO].realTimeInstant.flatMap { now =>
      command match {
        case InterviewWorkflowCommand.ReserveCalendarSlot(key) =>
          def reconcile: IO[InterviewResult] = calendar.lookup(workflow.id).value.map {
            case Right(Some(reservation)) if reservation.releasedAt.isEmpty => InterviewResult.Succeeded
            case Right(_)                                                   => InterviewResult.Rejected
            case Left(_)                                                    => InterviewResult.OutcomeUnknown
          }
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
              case result                   => IO.pure(result)
            }
        case InterviewWorkflowCommand.LookupCalendarReservation(id) =>
          calendar.lookup(id).value.map {
            case Right(Some(value)) if value.releasedAt.isEmpty => InterviewResult.Found
            case Right(_)                                       => InterviewResult.Absent
            case Left(_)                                        => InterviewResult.OutcomeUnknown
          }
        case InterviewWorkflowCommand.CommitAcceptedToInterview(_) =>
          repository.commitHiring(workflow, now, Some(claim)).value.map {
            case Right(_)                       => InterviewResult.Succeeded
            case Left(RepositoryError.Conflict) => InterviewResult.Rejected
            case Left(_)                        => InterviewResult.OutcomeUnknown
          }
        case InterviewWorkflowCommand.LookupStatusCommitReceipt(id) =>
          repository.hasHiringReceipt(id).value.map {
            case Right(true)  => InterviewResult.Found
            case Right(false) => InterviewResult.Absent
            case Left(_)      => InterviewResult.OutcomeUnknown
          }
        case InterviewWorkflowCommand.ReleaseCalendarSlot(key) => provider(calendar.release(key, now, Some(claim)))
        case InterviewWorkflowCommand.Notify(participant, key) =>
          provider(
            notifications.notify(
              workflow.id,
              if (participant == InterviewParticipant.Candidate) workflow.candidateId else workflow.recruiterId,
              participant,
              key,
              now,
              Some(claim)
            )
          )
        case InterviewWorkflowCommand.LookupNotificationReceipt(key) =>
          notifications.lookup(key).value.map {
            case Right(Some(_)) => InterviewResult.Found
            case Right(None)    => InterviewResult.Absent
            case Left(_)        => InterviewResult.OutcomeUnknown
          }
        case InterviewWorkflowCommand.Retry(_) | InterviewWorkflowCommand.RequireRepair(_) =>
          IO.pure(InterviewResult.Rejected)
      }
    }
  }

  private def step(command: InterviewWorkflowCommand): InterviewStep = command match {
    case InterviewWorkflowCommand.ReserveCalendarSlot(_)                    => InterviewStep.Reserve
    case InterviewWorkflowCommand.LookupCalendarReservation(_)              => InterviewStep.LookupReservation
    case InterviewWorkflowCommand.CommitAcceptedToInterview(_)              => InterviewStep.CommitHiring
    case InterviewWorkflowCommand.LookupStatusCommitReceipt(_)              => InterviewStep.LookupCommit
    case InterviewWorkflowCommand.ReleaseCalendarSlot(_)                    => InterviewStep.Release
    case InterviewWorkflowCommand.Notify(InterviewParticipant.Candidate, _) => InterviewStep.NotifyCandidate
    case InterviewWorkflowCommand.Notify(InterviewParticipant.Recruiter, _) => InterviewStep.NotifyRecruiter
    case InterviewWorkflowCommand.LookupNotificationReceipt(key)            =>
      if (key.endsWith(":Candidate")) InterviewStep.LookupNotifyCandidate else InterviewStep.LookupNotifyRecruiter
    case InterviewWorkflowCommand.Retry(_) | InterviewWorkflowCommand.RequireRepair(_) => InterviewStep.RequireRepair
  }

  private def event(message: InterviewMessage): Option[InterviewWorkflowEvent] = message.result.flatMap { result =>
    import InterviewResult.*
    import InterviewStep.*
    import InterviewWorkflowEvent.*
    (message.step, result) match {
      case (Reserve, Succeeded)                           => Some(ReservationConfirmed)
      case (Reserve, Rejected)                            => Some(ReservationRejected)
      case (Reserve, _)                                   => Some(ReservationOutcomeUnknown)
      case (LookupReservation, Found)                     => Some(ReservationLookupFound)
      case (LookupReservation, Absent)                    => Some(ReservationLookupAbsent)
      case (LookupReservation, _)                         => Some(ReservationOutcomeUnknown)
      case (CommitHiring, Succeeded)                      => Some(StatusCommitted)
      case (CommitHiring, Rejected)                       => Some(StatusCommitRejected)
      case (CommitHiring, _)                              => Some(StatusCommitOutcomeUnknown)
      case (LookupCommit, Found)                          => Some(StatusLookupFound)
      case (LookupCommit, Absent)                         => Some(StatusLookupAbsent)
      case (LookupCommit, _)                              => Some(StatusCommitOutcomeUnknown)
      case (Release, Succeeded)                           => Some(ReservationReleased)
      case (Release, _)                                   => Some(ReservationReleaseOutcomeUnknown)
      case (NotifyCandidate | NotifyRecruiter, Succeeded) => Some(NotificationDelivered(participant(message.step)))
      case (NotifyCandidate | NotifyRecruiter, _)         => Some(NotificationOutcomeUnknown(participant(message.step)))
      case (LookupNotifyCandidate | LookupNotifyRecruiter, Found) =>
        Some(NotificationLookupFound(participant(message.step)))
      case (LookupNotifyCandidate | LookupNotifyRecruiter, Absent) =>
        Some(NotificationLookupAbsent(participant(message.step)))
      case (LookupNotifyCandidate | LookupNotifyRecruiter, _) =>
        Some(NotificationOutcomeUnknown(participant(message.step)))
      case _ => Some(RetryExhausted("provider"))
    }
  }

  private def participant(step: InterviewStep): InterviewParticipant =
    if (step == InterviewStep.NotifyCandidate || step == InterviewStep.LookupNotifyCandidate)
      InterviewParticipant.Candidate
    else InterviewParticipant.Recruiter
}
