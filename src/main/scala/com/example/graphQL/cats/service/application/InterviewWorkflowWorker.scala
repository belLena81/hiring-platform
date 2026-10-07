package com.example.graphQL.cats.service.application

import cats.effect.{Clock, IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField}
import com.example.graphQL.cats.service.application.InterviewMessagePolicy.{stableId, step}
import scala.concurrent.duration.*

enum InterviewPublicationPass {
  case Full, Idle, Deferred
}

object InterviewWorkflowWorker {

  /** A full bounded pass yields and continues; idle or deferred work waits before polling again. */
  private[application] def publicationLoop(
      publish: IO[InterviewPublicationPass],
      pollInterval: FiniteDuration
  ): fs2.Stream[IO, Unit] = fs2.Stream.repeatEval(publish.flatMap {
    case InterviewPublicationPass.Full => IO.cede
    case _                             => IO.sleep(pollInterval)
  })
}

final case class InterviewWorkerSettings(
    workerId: String,
    pollInterval: FiniteDuration,
    claimLease: FiniteDuration,
    providerTimeout: FiniteDuration,
    maxAttempts: Int,
    initialBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    replayWindow: FiniteDuration = 7.days,
    clockSkewTolerance: FiniteDuration = 5.seconds,
    publicationBatchSize: Int = 16
)

/** Resource ownership makes shutdown cancel the publisher before transport and database release. */
final class InterviewWorkflowWorker(
    repository: InterviewWorkflowRepository,
    calendar: InterviewCalendarProvider,
    notifications: InterviewNotificationProvider,
    settings: InterviewWorkerSettings,
    diagnostics: Diagnostics,
    currentTime: IO[java.time.Instant] = Clock[IO].realTimeInstant
) {
  private enum PublicationOutcome {
    case Published, LeaseLost, CoordinationUnavailable
  }
  private enum PublicationStep {
    case Published, Idle, Deferred
  }

  private def report(operation: String): IO[Unit] =
    diagnostics.emit(LogEvent.MongoRepositoryFailed, fields = Map(LogField.SpanName -> operation))

  private def observe[A](operation: RepositoryIO[A], name: String): IO[Option[A]] =
    operation.value.flatMap {
      case Right(value) => IO.pure(Some(value))
      case Left(_)      => report(name).as(None)
    }

  def publisher(transport: InterviewTransport): Resource[IO, Unit] =
    Resource
      .make(publicationStream(transport).compile.drain.start)(_.cancel)
      .void

  def publicationStream(transport: InterviewTransport): fs2.Stream[IO, Unit] =
    InterviewWorkflowWorker.publicationLoop(
      publishDue(transport).handleErrorWith {
        case fenced: InterviewProducerGenerationFenced => IO.raiseError(fenced)
        case _ => report("interviewWorkflow.publisher").as(InterviewPublicationPass.Deferred)
      },
      settings.pollInterval
    )

  def publishDue(transport: InterviewTransport): IO[InterviewPublicationPass] = {
    def drain(remaining: Int): IO[InterviewPublicationPass] =
      if (remaining <= 0) IO.pure(InterviewPublicationPass.Full)
      else
        publishNext(transport).flatMap {
          case PublicationStep.Published => drain(remaining - 1)
          case PublicationStep.Idle      => IO.pure(InterviewPublicationPass.Idle)
          case PublicationStep.Deferred  => IO.pure(InterviewPublicationPass.Deferred)
        }
    drain(settings.publicationBatchSize)
  }

  private def publishNext(transport: InterviewTransport): IO[PublicationStep] =
    for {
      now <- currentTime
      claimed <- observe(
        repository.claimDueCommands(settings.workerId, now, now.plusMillis(settings.claimLease.toMillis), 1),
        "interviewWorkflow.claimPublication"
      )
      results <- claimed.toList.flatten.traverse { claim =>
        observe(repository.findForAdmin(claim.record.workflowId), "interviewWorkflow.loadPublication").flatMap {
          case Some(Some(workflow)) =>
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
              claim.record.result.map(InterviewResult.fromCommandResult),
              claim.record.occurredAt
            )
            def finish: IO[Boolean] = currentTime.flatMap(at =>
              observe(
                repository.requireRepair(claim, at, "publication_exhausted"),
                "interviewWorkflow.finishPublication"
              ).as(false)
            )
            if (claim.record.publicationAttempts >= settings.maxAttempts) finish
            else
              currentTime
                .flatMap(checkedAt =>
                  repository.authorizePublication(claim, transport.generationFor(message), checkedAt).value
                )
                .flatMap {
                  case Left(RepositoryError.Unavailable) =>
                    currentTime.flatMap(at =>
                      observe(
                        repository.retry(
                          claim,
                          at,
                          at.plusMillis(settings.initialBackoff.toMillis),
                          "publication_coordination_unavailable"
                        ),
                        "interviewWorkflow.deferPublication"
                      ).as(false)
                    )
                  case Left(_)      => report("interviewWorkflow.authorizePublication").as(false)
                  case Right(false) => IO.pure(false)
                  case Right(true)  =>
                    publishWithLease(transport, message, claim).attempt.flatMap {
                      case Right(PublicationOutcome.Published) =>
                        currentTime.flatMap(at =>
                          observe(repository.markPublished(claim, at), "interviewWorkflow.markPublished")
                            .map(_.isDefined)
                        )
                      case Right(PublicationOutcome.LeaseLost)               => IO.pure(false)
                      case Right(PublicationOutcome.CoordinationUnavailable) =>
                        currentTime.flatMap(at =>
                          observe(
                            repository.retry(
                              claim,
                              at,
                              at.plusMillis(settings.initialBackoff.toMillis),
                              "publication_coordination_unavailable"
                            ),
                            "interviewWorkflow.deferPublication"
                          ).as(false)
                        )
                      case Left(fenced: InterviewProducerGenerationFenced) => IO.raiseError(fenced)
                      case Left(_) if claim.record.publicationAttempts + 1 >= settings.maxAttempts => finish
                      case Left(_)                                                                 =>
                        currentTime.flatMap { at =>
                          val delay = InterviewWorkflowPolicy.backoffMillis(
                            claim.record.publicationAttempts + 1,
                            settings.initialBackoff.toMillis,
                            settings.maxBackoff.toMillis
                          )
                          observe(
                            repository.retry(claim, at, at.plusMillis(delay), "publication_unavailable"),
                            "interviewWorkflow.retryPublication"
                          ).as(false)
                        }
                    }
                }
          case _ => IO.pure(false)
        }
      }
    } yield claimed match {
      case Some(Nil)                   => PublicationStep.Idle
      case _ if results.contains(true) => PublicationStep.Published
      case _                           => PublicationStep.Deferred
    }

  private def publishWithLease(
      transport: InterviewTransport,
      message: InterviewMessage,
      claim: ClaimedInterviewWorkflowCommand
  ): IO[PublicationOutcome] = {
    def renew: IO[Option[PublicationOutcome]] = currentTime.flatMap { now =>
      repository.renewPublication(claim, now, now.plusMillis(settings.claimLease.toMillis)).value.map {
        case Right(true)  => None
        case Right(false) => Some(PublicationOutcome.LeaseLost)
        case Left(_)      => Some(PublicationOutcome.CoordinationUnavailable)
      }
    }
    val heartbeat = fs2.Stream
      .awakeEvery[IO]((settings.claimLease / 3).max(1.millis))
      .evalMap(_ => renew)
      .unNone
      .take(1)
      .compile
      .last
      .map(_.getOrElse(PublicationOutcome.CoordinationUnavailable))
    // Loading and authorization can consume most of the original claim lease.
    renew.flatMap {
      case Some(outcome) => IO.pure(outcome)
      case None          =>
        IO.defer(IO.race(transport.publish(message), heartbeat)).map {
          case Left(_)        => PublicationOutcome.Published
          case Right(outcome) => outcome
        }
    }
  }

  private def quarantineMessage(message: InterviewMessage, reason: InterviewMessageRejection): IO[Boolean] =
    currentTime.flatMap(now =>
      repository.quarantine(s"${message.workflowId}:${message.messageId}:${reason.code}", now).value.flatMap {
        case Right(_) => IO.pure(true)
        case Left(_)  => report("interviewWorkflow.quarantine").as(false)
      }
    )

  private def withinReplayWindow(message: InterviewMessage)(process: IO[Boolean]): IO[Boolean] =
    currentTime.flatMap { now =>
      InterviewMessagePolicy.replayDisposition(message, now, settings.replayWindow, settings.clockSkewTolerance) match {
        case InterviewReplayDisposition.Deferred => IO.pure(false)
        case InterviewReplayDisposition.Future   => repairFutureMessage(message, now)
        case InterviewReplayDisposition.Timely   => process
        case InterviewReplayDisposition.Expired  =>
          (for {
            workflow <- repository.findForAdmin(InterviewWorkflowId(message.workflowId))
            command <- repository.findCommand(InterviewWorkflowId(message.workflowId), message.stepId)
          } yield (command, workflow)).value.flatMap {
            case Right((command, workflow)) =>
              InterviewMessagePolicy.expiredDecision(workflow, command, message) match {
                case Right(InterviewWorkflowDecision(next, commands)) =>
                  repository
                    .advance(
                      next,
                      next.revision - 1L,
                      InterviewAdvanceCause.ReplayExpired(message.messageId),
                      commands,
                      now
                    )
                    .value
                    .map {
                      case Right(InterviewWorkflowAdvanceResult.StaleRevision) => false
                      case Right(_)                                            => true
                      case Left(_)                                             => false
                    }
                case Left(reason) => quarantineMessage(message, reason)
              }
            case Left(_) => report("interviewWorkflow.receive").as(false)
          }
      }
    }

  private def repairFutureMessage(message: InterviewMessage, now: java.time.Instant): IO[Boolean] =
    (for {
      workflow <- repository.findForAdmin(InterviewWorkflowId(message.workflowId))
      command <- repository.findCommand(InterviewWorkflowId(message.workflowId), message.stepId)
    } yield (workflow, command)).value.flatMap {
      case Left(_)                    => report("interviewWorkflow.futureReceipt").as(false)
      case Right((workflow, command)) =>
        InterviewMessagePolicy.futureDecision(workflow, command, message) match {
          case Left(_) => quarantineMessage(message, InterviewMessageRejection.Future)
          case Right(InterviewWorkflowDecision(next, commands)) =>
            repository
              .advance(next, next.revision - 1L, InterviewAdvanceCause.FutureMessage(message.messageId), commands, now)
              .value
              .flatMap {
                case Right(InterviewWorkflowAdvanceResult.StaleRevision) | Left(_) => IO.pure(false)
                case Right(_) => quarantineMessage(message, InterviewMessageRejection.Future)
              }
        }
    }

  def receiveCommand(message: InterviewMessage): IO[Boolean] = withinReplayWindow(message)(processCommand(message))

  private def processCommand(message: InterviewMessage): IO[Boolean] =
    (for {
      command <- repository.findCommand(InterviewWorkflowId(message.workflowId), message.stepId)
      workflow <- repository.findForAdmin(InterviewWorkflowId(message.workflowId))
    } yield (command, workflow)).value.flatMap {
      case Right((stored, current)) =>
        InterviewMessagePolicy.commandAdmission(current, stored, message) match {
          case InterviewCommandAdmission.Acknowledge                => IO.pure(true)
          case InterviewCommandAdmission.Quarantine(reason)         => quarantineMessage(message, reason)
          case InterviewCommandAdmission.Execute(workflow, command) =>
            currentTime.flatMap { claimAt =>
              repository
                .claimExecution(
                  command,
                  settings.workerId,
                  claimAt,
                  claimAt.plusMillis(settings.claimLease.toMillis),
                  settings.maxAttempts
                )
                .value
                .flatMap {
                  case Left(_) => report("interviewWorkflow.claimExecution").as(false)
                  case Right(InterviewExecutionClaimOutcome.Busy) => IO.pure(false)
                  case Right(
                        InterviewExecutionClaimOutcome.AlreadyHandled |
                        InterviewExecutionClaimOutcome.ReconciliationQueued |
                        InterviewExecutionClaimOutcome.RepairRequired
                      ) =>
                    IO.pure(true)
                  case Right(InterviewExecutionClaimOutcome.Acquired(claim)) => {
                    execute(workflow, command.command, claim)
                      .timeoutTo(settings.providerTimeout, IO.pure(InterviewResult.OutcomeUnknown))
                      .flatMap { result =>
                        currentTime.flatMap(at =>
                          repository
                            .recordResult(claim, InterviewCommandResult.fromTransportResult(result), at)
                            .value
                            .flatMap {
                              case Right(_) => IO.pure(true)
                              case Left(_)  => report("interviewWorkflow.recordResult").as(false)
                            }
                        )
                      }
                  }
                }
            }
        }
      case Left(_) => report("interviewWorkflow.receive").as(false)
    }

  def receiveResult(message: InterviewMessage): IO[Boolean] = withinReplayWindow(message)(processResult(message))

  private def processResult(message: InterviewMessage): IO[Boolean] =
    (for {
      workflow <- repository.findForAdmin(InterviewWorkflowId(message.workflowId))
      command <- repository.findCommand(InterviewWorkflowId(message.workflowId), message.stepId)
    } yield (workflow, command)).value.flatMap {
      case Right((current, stored)) =>
        InterviewMessagePolicy.resultAdmission(current, stored, message) match {
          case Left(reason)               => quarantineMessage(message, reason)
          case Right((workflow, command)) =>
            repository.attemptCount(workflow.id, command.command).value.flatMap {
              case Left(_)      => report("interviewWorkflow.receive").as(false)
              case Right(count) => {
                val selected = InterviewMessagePolicy.resultEvent(message, count, settings.maxAttempts)
                selected.flatMap(ev => InterviewWorkflow.decide(workflow, workflow.revision, ev).toOption) match {
                  case None => quarantineMessage(message, InterviewMessageRejection.InvalidTransition)
                  case Some(InterviewWorkflowDecision(next, commands)) =>
                    currentTime.flatMap { now =>
                      val available = InterviewMessagePolicy.retryAvailableAt(
                        message.result,
                        count,
                        now,
                        settings.initialBackoff.toMillis,
                        settings.maxBackoff.toMillis
                      )
                      repository
                        .advance(
                          next,
                          workflow.revision,
                          InterviewAdvanceCause.ResultReceipt(message.messageId.toString),
                          commands,
                          now,
                          available
                        )
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
        }
      case Left(_) => report("interviewWorkflow.receive").as(false)
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
    currentTime.flatMap { now =>
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
        case InterviewWorkflowCommand.LookupNotificationReceipt(_, key) =>
          notifications.lookup(key).value.map {
            case Right(Some(_)) => InterviewResult.Found
            case Right(None)    => InterviewResult.Absent
            case Left(_)        => InterviewResult.OutcomeUnknown
          }
        case InterviewWorkflowCommand.RequireRepair(_) =>
          IO.pure(InterviewResult.Rejected)
      }
    }
  }

}
