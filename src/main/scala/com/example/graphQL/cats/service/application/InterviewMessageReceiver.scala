package com.example.graphQL.cats.service.application

import cats.effect.IO
import com.example.graphQL.cats.domain.policy.RetryBackoff
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.port.*

/** Admits command and result messages: replay-window checks, quarantine, execution claims and result application. A
  * message is acknowledged (`true`) only once its outcome is durable.
  */
private[application] final class InterviewMessageReceiver(
    repository: InterviewWorkflowRepository,
    settings: InterviewWorkerSettings,
    reporter: InterviewWorkerReporter,
    currentTime: IO[java.time.Instant],
    executor: InterviewProviderExecutor
) {
  import reporter.report

  private type Loaded = (Option[InterviewWorkflow], Option[InterviewWorkflowCommandRecord])

  def receiveCommand(message: InterviewMessage): IO[Boolean] = withinReplayWindow(message)(processCommand(message))

  def receiveResult(message: InterviewMessage): IO[Boolean] = withinReplayWindow(message)(processResult(message))

  private def withLoaded(message: InterviewMessage, failure: String)(admit: Loaded => IO[Boolean]): IO[Boolean] = {
    val id = InterviewWorkflowId(message.workflowId)
    (for {
      workflow <- repository.findForAdmin(id)
      command <- repository.findCommand(id, message.stepId)
    } yield (workflow, command)).value.flatMap {
      case Right(loaded) => admit(loaded)
      case Left(_)       => report(failure).as(false)
    }
  }

  /** A stale revision or a failed write leaves the message unacknowledged for redelivery. */
  private def advanced(result: RepositoryIO[InterviewWorkflowAdvanceResult]): IO[Boolean] =
    result.value.map {
      case Right(InterviewWorkflowAdvanceResult.StaleRevision) | Left(_) => false
      case Right(_)                                                      => true
    }

  private def quarantineMessage(message: InterviewMessage, reason: InterviewMessageRejection): IO[Boolean] =
    currentTime.flatMap(now =>
      reporter.succeeded(
        repository.quarantine(s"${message.workflowId}:${message.messageId}:${reason.code}", now),
        "interviewWorkflow.quarantine"
      )
    )

  private def withinReplayWindow(message: InterviewMessage)(process: IO[Boolean]): IO[Boolean] =
    currentTime.flatMap { now =>
      InterviewMessagePolicy.replayDisposition(message, now, settings.replayWindow, settings.clockSkewTolerance) match {
        case InterviewReplayDisposition.Deferred => IO.pure(false)
        case InterviewReplayDisposition.Future   => repairFutureMessage(message, now)
        case InterviewReplayDisposition.Timely   => process
        case InterviewReplayDisposition.Expired  =>
          withLoaded(message, "interviewWorkflow.receive") { case (workflow, command) =>
            val cause = InterviewAdvanceCause.ReplayExpired(message.messageId)
            if (command.exists(isLifecycle))
              failLifecycleMessage(
                message,
                workflow,
                command,
                InterviewFailureCode.ReplayExpired,
                InterviewMessageRejection.Expired,
                cause,
                now,
                IO.pure(true)
              )
            else
              InterviewMessagePolicy.expiredDecision(workflow, command, message) match {
                case Right(InterviewWorkflowDecision(next, commands)) =>
                  advanced(repository.advance(next, next.revision - 1L, cause, commands, now))
                case Left(reason) => quarantineMessage(message, reason)
              }
          }
      }
    }

  private def repairFutureMessage(message: InterviewMessage, now: java.time.Instant): IO[Boolean] =
    withLoaded(message, "interviewWorkflow.futureReceipt") { case (workflow, command) =>
      val cause = InterviewAdvanceCause.FutureMessage(message.messageId)
      val quarantine = quarantineMessage(message, InterviewMessageRejection.Future)
      if (command.exists(isLifecycle))
        failLifecycleMessage(
          message,
          workflow,
          command,
          InterviewFailureCode.FutureMessage,
          InterviewMessageRejection.Future,
          cause,
          now,
          quarantine
        )
      else
        InterviewMessagePolicy.futureDecision(workflow, command, message) match {
          case Left(_)                                          => quarantine
          case Right(InterviewWorkflowDecision(next, commands)) =>
            advanced(repository.advance(next, next.revision - 1L, cause, commands, now))
              .flatMap(if (_) quarantine else IO.pure(false))
        }
    }

  private def isLifecycle(record: InterviewWorkflowCommandRecord): Boolean = record.command match {
    case _: InterviewLifecycleCommand => true
    case _                            => false
  }

  private def deferExpiry(record: InterviewWorkflowCommandRecord, now: java.time.Instant): IO[Boolean] =
    record.command match {
      case InterviewLifecycleCommand.ExpireProposal(due) =>
        val availableAt =
          InterviewMessagePolicy.expiryDeferral(due, now, settings.initialBackoff, settings.maxBackoff)
        repository.deferExpiry(record, availableAt).value.flatMap {
          case Right(true)  => IO.pure(true)
          case Right(false) =>
            // Not requeued: acknowledge only if the row is settled elsewhere, otherwise redeliver the message.
            repository.findCommand(record.workflowId, record.stepId).value.map {
              case Right(Some(current)) => settledElsewhere(current)
              case Right(None)          => true
              case Left(_)              => false
            }
          case Left(_) => IO.pure(false)
        }
      case _ => IO.pure(false)
    }

  private def settledElsewhere(record: InterviewWorkflowCommandRecord): Boolean =
    record.result.nonEmpty || Set(
      InterviewWorkflowCommandState.Superseded,
      InterviewWorkflowCommandState.RepairRequired,
      InterviewWorkflowCommandState.ResultPending,
      InterviewWorkflowCommandState.ResultPublished
    )(record.state)

  /** A cancel or reschedule message that is too old or too new fails its step through the lifecycle policy; an
    * informational notification fails only its own command; an expiry that is too early is requeued for its due time
    * and its message acknowledged. `acknowledge` runs once the failure is durable and decides how the message itself is
    * settled, so a rejected message is quarantined exactly once.
    */
  private def failLifecycleMessage(
      message: InterviewMessage,
      workflow: Option[InterviewWorkflow],
      command: Option[InterviewWorkflowCommandRecord],
      step: InterviewFailureCode,
      rejection: InterviewMessageRejection,
      cause: InterviewAdvanceCause,
      now: java.time.Instant,
      acknowledge: IO[Boolean]
  ): IO[Boolean] = {
    def settled(durable: IO[Boolean]): IO[Boolean] = durable.flatMap(if (_) acknowledge else IO.pure(false))
    InterviewMessagePolicy.lifecycleReplayFailure(workflow, command, message, step, rejection, now) match {
      case Left(reason)                                               => quarantineMessage(message, reason)
      case Right(InterviewMessagePolicy.LifecycleReplayFailure.Defer) =>
        // The expiry is simply not due on this node's clock: requeue it, with a backoff so a skewed consumer does not
        // receive it in a hot loop, and settle the message (without quarantining it) only when the row is requeued or
        // is already settled by another path.
        command.fold(IO.pure(false))(record => deferExpiry(record, now))
      case Right(InterviewMessagePolicy.LifecycleReplayFailure.Notification) =>
        settled(
          command.fold(IO.pure(false))(record =>
            repository
              .settleNotificationResult(record, InterviewNotificationSettlement.Exhausted(step), now)
              .value
              .map(_.isRight)
          )
        )
      case Right(InterviewMessagePolicy.LifecycleReplayFailure.Workflow(event)) =>
        settled(
          workflow.fold(IO.pure(false))(current =>
            repository
              .applyLifecycle(current.id, current.revision, event, InterviewLifecycleOrigin.Internal(cause), now, None)
              .value
              .map {
                case Right(InterviewLifecycleOutcome.Applied(_) | InterviewLifecycleOutcome.Duplicate(_)) => true
                case _                                                                                    => false
              }
          )
        )
    }
  }

  private def processCommand(message: InterviewMessage): IO[Boolean] =
    withLoaded(message, "interviewWorkflow.receive") { case (current, stored) =>
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
                case Left(_)                                    => report("interviewWorkflow.claimExecution").as(false)
                case Right(InterviewExecutionClaimOutcome.Busy) => IO.pure(false)
                case Right(
                      InterviewExecutionClaimOutcome.AlreadyHandled |
                      InterviewExecutionClaimOutcome.ReconciliationQueued |
                      InterviewExecutionClaimOutcome.RepairRequired
                    ) =>
                  IO.pure(true)
                case Right(InterviewExecutionClaimOutcome.Acquired(claim)) =>
                  executor
                    .execute(workflow, command.command, claim)
                    .timeoutTo(settings.providerTimeout, IO.pure(InterviewResult.OutcomeUnknown))
                    .flatMap(result =>
                      currentTime.flatMap(at =>
                        reporter.succeeded(
                          repository.recordResult(claim, result, at),
                          "interviewWorkflow.recordResult"
                        )
                      )
                    )
              }
          }
      }
    }

  private def processResult(message: InterviewMessage): IO[Boolean] =
    withLoaded(message, "interviewWorkflow.receive") { case (current, stored) =>
      InterviewMessagePolicy.resultAdmission(current, stored, message) match {
        case Left(reason)               => quarantineMessage(message, reason)
        case Right((workflow, command)) =>
          repository.attemptCount(workflow.id, command.command).value.flatMap {
            case Left(_)      => report("interviewWorkflow.receive").as(false)
            case Right(count) =>
              command.command match {
                case lifecycle: InterviewLifecycleCommand =>
                  applyLifecycleResult(workflow, command, lifecycle, message, count)
                case _: InterviewWorkflowCommand =>
                  val selected = InterviewMessagePolicy.resultEvent(message, count, settings.maxAttempts)
                  selected.flatMap(ev => InterviewWorkflow.decide(workflow, workflow.revision, ev).toOption) match {
                    case None => quarantineMessage(message, InterviewMessageRejection.InvalidTransition)
                    case Some(InterviewWorkflowDecision(next, commands)) =>
                      currentTime.flatMap(now =>
                        advanced(
                          repository.advance(
                            next,
                            workflow.revision,
                            InterviewAdvanceCause.ResultReceipt(message.messageId.toString),
                            commands,
                            now,
                            retryAvailableAt(message, count, now)
                          )
                        )
                      )
                  }
              }
          }
      }
    }

  private def retryAvailableAt(message: InterviewMessage, attempts: Long, now: java.time.Instant) =
    InterviewMessagePolicy.retryAvailableAt(
      message.result,
      attempts,
      now,
      settings.initialBackoff.toMillis,
      settings.maxBackoff.toMillis
    )

  /** Applies a recorded cancel or reschedule result. Round steps move the workflow through the lifecycle policy; an
    * informational notification never does: a failure is retried or, once its budget is spent, repaired on its own.
    */
  private def applyLifecycleResult(
      workflow: InterviewWorkflow,
      record: InterviewWorkflowCommandRecord,
      command: InterviewLifecycleCommand,
      message: InterviewMessage,
      attempts: Long
  ): IO[Boolean] =
    message.result.fold(quarantineMessage(message, InterviewMessageRejection.Inconsistent))(result =>
      currentTime.flatMap { now =>
        if (InterviewCommands.isInformational(command))
          if (result == InterviewResult.Succeeded) IO.pure(true)
          else {
            val settlement =
              if (attempts >= settings.maxAttempts)
                InterviewNotificationSettlement.Exhausted(InterviewFailureCode.NotificationExhausted)
              else
                InterviewNotificationSettlement.Retry(
                  now.plusMillis(
                    RetryBackoff
                      .exponentialMillis(attempts, settings.initialBackoff.toMillis, settings.maxBackoff.toMillis)
                  )
                )
            reporter.succeeded(
              repository.settleNotificationResult(record, settlement, now),
              "interviewWorkflow.settleNotification"
            )
          }
        else
          InterviewMessagePolicy.lifecycleResultEvent(command, result, now, attempts, settings.maxAttempts) match {
            case None        => quarantineMessage(message, InterviewMessageRejection.InvalidTransition)
            case Some(event) =>
              repository
                .applyLifecycle(
                  workflow.id,
                  workflow.revision,
                  event,
                  InterviewLifecycleOrigin.Internal(InterviewAdvanceCause.ResultReceipt(message.messageId.toString)),
                  now,
                  retryAvailableAt(message, attempts, now)
                )
                .value
                .flatMap {
                  case Right(InterviewLifecycleOutcome.Applied(_) | InterviewLifecycleOutcome.Duplicate(_)) =>
                    IO.pure(true)
                  case Right(InterviewLifecycleOutcome.Rejected(InterviewWorkflowError.StaleRevision)) =>
                    IO.pure(false)
                  case Right(_) => quarantineMessage(message, InterviewMessageRejection.InvalidTransition)
                  case Left(_)  => report("interviewWorkflow.applyLifecycleResult").as(false)
                }
          }
      }
    )
}
