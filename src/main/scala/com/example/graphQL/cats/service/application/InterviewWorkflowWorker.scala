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
            case Right((command, workflow)) if command.exists(isLifecycle) =>
              failLifecycleMessage(
                message,
                workflow,
                command,
                "replay_expired",
                InterviewMessageRejection.Expired,
                InterviewAdvanceCause.ReplayExpired(message.messageId),
                now,
                IO.pure(true)
              )
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
      case Left(_) => report("interviewWorkflow.futureReceipt").as(false)
      case Right((workflow, command)) if command.exists(isLifecycle) =>
        failLifecycleMessage(
          message,
          workflow,
          command,
          "future_message",
          InterviewMessageRejection.Future,
          InterviewAdvanceCause.FutureMessage(message.messageId),
          now,
          quarantineMessage(message, InterviewMessageRejection.Future)
        )
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
      step: String,
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
              case Right(count) =>
                command.command match {
                  case lifecycle: InterviewLifecycleCommand =>
                    applyLifecycleResult(workflow, command, lifecycle, message, count)
                  case _: InterviewWorkflowCommand => {
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
        }
      case Left(_) => report("interviewWorkflow.receive").as(false)
    }

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
                InterviewNotificationSettlement.Exhausted("notification_exhausted")
              else
                InterviewNotificationSettlement.Retry(
                  now.plusMillis(
                    InterviewWorkflowPolicy
                      .backoffMillis(attempts, settings.initialBackoff.toMillis, settings.maxBackoff.toMillis)
                  )
                )
            repository.settleNotificationResult(record, settlement, now).value.flatMap {
              case Right(_) => IO.pure(true)
              case Left(_)  => report("interviewWorkflow.settleNotification").as(false)
            }
          }
        else
          InterviewMessagePolicy.lifecycleResultEvent(command, result, now, attempts, settings.maxAttempts) match {
            case None        => quarantineMessage(message, InterviewMessageRejection.InvalidTransition)
            case Some(event) =>
              val available = InterviewMessagePolicy.retryAvailableAt(
                message.result,
                attempts,
                now,
                settings.initialBackoff.toMillis,
                settings.maxBackoff.toMillis
              )
              repository
                .applyLifecycle(
                  workflow.id,
                  workflow.revision,
                  event,
                  InterviewLifecycleOrigin.Internal(InterviewAdvanceCause.ResultReceipt(message.messageId.toString)),
                  now,
                  available
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

  private def provider[A](operation: InterviewProviderIO[A]): IO[InterviewResult] = operation.value.map {
    case Right(_)                              => InterviewResult.Succeeded
    case Left(InterviewProviderError.Conflict) => InterviewResult.Rejected
    case Left(_)                               => InterviewResult.OutcomeUnknown
  }

  /** The one reading of a lookup: found, definitively absent, or unknown when the answer could not be read. */
  private def lookedUp[E](answer: Either[E, Boolean]): InterviewResult = answer match {
    case Right(true)  => InterviewResult.Found
    case Right(false) => InterviewResult.Absent
    case Left(_)      => InterviewResult.OutcomeUnknown
  }

  private def reservationLookup(workflowId: InterviewWorkflowId, key: String)(
      found: InterviewCalendarReservation => Boolean
  ): IO[InterviewResult] =
    calendar.lookup(workflowId, key).value.map(answer => lookedUp(answer.map(_.exists(found))))

  private def receiptLookup(key: String): IO[InterviewResult] =
    notifications.lookup(key).value.map(answer => lookedUp(answer.map(_.nonEmpty)))

  private def recipient(workflow: InterviewWorkflow, participant: InterviewParticipant) =
    if (participant == InterviewParticipant.Candidate) workflow.candidateId else workflow.recruiterId

  private def execute(
      workflow: InterviewWorkflow,
      command: InterviewCommand,
      claim: ClaimedInterviewWorkflowCommand
  ): IO[InterviewResult] = command match {
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
  ): IO[InterviewResult] = {
    import InterviewLifecycleCommand as C
    command match {
      case C.CancelCalendarSlot(key) =>
        calendar.cancel(workflow.id, key, now, Some(claim)).value.map {
          case Right(InterviewCalendarCancellation.Cancelled(_) | InterviewCalendarCancellation.AlreadyCancelled(_)) =>
            InterviewResult.Succeeded
          // Definitive: the provider has no such reservation, so repeating the cancel or looking it up cannot help.
          case Right(InterviewCalendarCancellation.UnknownReservation) => InterviewResult.Rejected
          case _                                                       => InterviewResult.OutcomeUnknown
        }
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
          case result => IO.pure(result)
        }
      case C.LookupReplacementHold(key) =>
        reservationLookup(workflow.id, key)(_.releasedAt.isEmpty)
      case C.CommitRescheduledInterval(_, _) =>
        if (!now.isBefore(InterviewCalendarFence.swapDeadline(workflow))) IO.pure(InterviewResult.Rejected)
        else
          repository.approveRescheduledInterval(workflow, now, claim).value.map {
            case Right(_)                       => InterviewResult.Succeeded
            case Left(RepositoryError.Conflict) => InterviewResult.Rejected
            case Left(_)                        => InterviewResult.OutcomeUnknown
          }
      case C.LookupRescheduleCommitReceipt(id, generation) =>
        repository.hasRescheduleApproval(id, generation).value.map(lookedUp)
      // Due work: the command is only claimed once the proposal's expiry time has been reached.
      case C.ExpireProposal(_)              => IO.pure(InterviewResult.Succeeded)
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
          notifications.lookup(key).value.flatMap {
            case Right(Some(_)) => IO.pure(InterviewResult.Succeeded)
            case Right(None)    => send
            case Left(_)        => IO.pure(InterviewResult.OutcomeUnknown)
          }
      case C.LookupNotificationReceipt(_, _, key) => receiptLookup(key)
      case C.RequireRepair(_)                     => IO.pure(InterviewResult.Rejected)
    }
  }

  private def executeScheduling(
      workflow: InterviewWorkflow,
      command: InterviewWorkflowCommand,
      claim: ClaimedInterviewWorkflowCommand
  ): IO[InterviewResult] =
    currentTime.flatMap { now =>
      command match {
        case InterviewWorkflowCommand.ReserveCalendarSlot(key) =>
          def reconcile: IO[InterviewResult] =
            calendar.lookup(workflow.id, InterviewWorkflow.reservationKey(workflow.id, 0)).value.map {
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
          reservationLookup(id, InterviewWorkflow.reservationKey(id, 0))(_.releasedAt.isEmpty)
        case InterviewWorkflowCommand.CommitAcceptedToInterview(_) =>
          repository.commitHiring(workflow, now, Some(claim)).value.map {
            case Right(_)                       => InterviewResult.Succeeded
            case Left(RepositoryError.Conflict) => InterviewResult.Rejected
            case Left(_)                        => InterviewResult.OutcomeUnknown
          }
        case InterviewWorkflowCommand.LookupStatusCommitReceipt(id) =>
          repository.hasHiringReceipt(id).value.map(lookedUp)
        case InterviewWorkflowCommand.ReleaseCalendarSlot(key) => provider(calendar.release(key, now, Some(claim)))
        case InterviewWorkflowCommand.Notify(participant, key) =>
          provider(
            notifications.notify(
              workflow.id,
              if (participant == InterviewParticipant.Candidate) workflow.candidateId else workflow.recruiterId,
              participant,
              InterviewNotificationKind.Scheduled,
              key,
              now,
              Some(claim)
            )
          )
        case InterviewWorkflowCommand.LookupNotificationReceipt(_, key) => receiptLookup(key)
        case InterviewWorkflowCommand.RequireRepair(_)                  =>
          IO.pure(InterviewResult.Rejected)
      }
    }

}
