package com.example.graphQL.cats.service.application

import cats.Applicative
import cats.effect.Temporal
import cats.syntax.all.*
import com.example.graphQL.cats.domain.policy.RetryBackoff
import com.example.graphQL.cats.domain.workflow.*
import com.example.graphQL.cats.service.application.InterviewMessagePolicy.{stableId, step}
import com.example.graphQL.cats.service.port.*
import com.example.graphQL.cats.service.RepositoryError
import scala.concurrent.duration.*

/** Claims due commands one at a time and publishes them under a renewed claim lease. */
private[application] final class InterviewPublication[F[_]: Temporal](
    repository: InterviewWorkflowRepository[F],
    settings: InterviewWorkerSettings,
    reporter: InterviewWorkerReporter[F],
    currentTime: F[java.time.Instant]
) {
  import reporter.{observe, report}

  private enum PublicationOutcome {
    case Published, LeaseLost, CoordinationUnavailable
  }

  /** `Full` means one command was published and the pass may continue. */
  def publishDue(transport: InterviewTransport[F]): F[InterviewPublicationPass] = {
    def drain(remaining: Int): F[InterviewPublicationPass] =
      if (remaining <= 0) Applicative[F].pure(InterviewPublicationPass.Full)
      else
        publishNext(transport).flatMap {
          case InterviewPublicationPass.Full => drain(remaining - 1)
          case pass                          => Applicative[F].pure(pass)
        }
    drain(settings.publicationBatchSize)
  }

  private def publishNext(transport: InterviewTransport[F]): F[InterviewPublicationPass] =
    for {
      now <- currentTime
      claimed <- observe(
        repository.claimDueCommands(settings.workerId, now, now.plusMillis(settings.claimLease.toMillis), 1),
        "interviewWorkflow.claimPublication"
      )
      results <- claimed.toList.flatten.traverse(publishClaim(transport, _))
    } yield claimed match {
      case Some(Nil)                   => InterviewPublicationPass.Idle
      case _ if results.contains(true) => InterviewPublicationPass.Full
      case _                           => InterviewPublicationPass.Deferred
    }

  private def messageFor(workflow: InterviewWorkflow, record: InterviewWorkflowCommandRecord): InterviewMessage = {
    val commandId = stableId(record.stepId)
    val hasResult = record.result.nonEmpty
    InterviewMessage(
      if (hasResult) stableId(s"$commandId:result") else commandId,
      workflow.id.value,
      record.stepId,
      step(record.command),
      record.revision,
      if (hasResult) commandId else workflow.id.value,
      workflow.preCommitDeadline,
      record.result,
      record.occurredAt
    )
  }

  private def publishClaim(transport: InterviewTransport[F], claim: ClaimedInterviewWorkflowCommand): F[Boolean] =
    observe(repository.findForAdmin(claim.record.workflowId), "interviewWorkflow.loadPublication").flatMap {
      case Some(Some(workflow)) => publish(transport, messageFor(workflow, claim.record), claim)
      case _                    => Applicative[F].pure(false)
    }

  private def publish(
      transport: InterviewTransport[F],
      message: InterviewMessage,
      claim: ClaimedInterviewWorkflowCommand
  ): F[Boolean] = {
    def finish: F[Boolean] = currentTime.flatMap(at =>
      observe(
        repository.requireRepair(claim, at, InterviewFailureCode.PublicationExhausted),
        "interviewWorkflow.finishPublication"
      )
        .as(false)
    )
    def deferCoordination: F[Boolean] = currentTime.flatMap(at =>
      observe(
        repository.retry(
          claim,
          at,
          at.plusMillis(settings.initialBackoff.toMillis),
          InterviewFailureCode.PublicationCoordinationUnavailable
        ),
        "interviewWorkflow.deferPublication"
      ).as(false)
    )
    if (claim.record.publicationAttempts >= settings.maxAttempts) finish
    else
      currentTime
        .flatMap(checkedAt => repository.authorizePublication(claim, transport.generationFor(message), checkedAt).value)
        .flatMap {
          case Left(RepositoryError.Unavailable) => deferCoordination
          case Left(_)                           => report("interviewWorkflow.authorizePublication").as(false)
          case Right(false)                      => Applicative[F].pure(false)
          case Right(true)                       =>
            publishWithLease(transport, message, claim).attempt.flatMap {
              case Right(PublicationOutcome.Published) =>
                currentTime.flatMap(at =>
                  reporter.succeeded(repository.markPublished(claim, at), "interviewWorkflow.markPublished")
                )
              case Right(PublicationOutcome.LeaseLost)               => Applicative[F].pure(false)
              case Right(PublicationOutcome.CoordinationUnavailable) => deferCoordination
              case Left(fenced: InterviewProducerGenerationFenced)   => Temporal[F].raiseError(fenced)
              case Left(_) if claim.record.publicationAttempts + 1 >= settings.maxAttempts => finish
              case Left(_)                                                                 =>
                currentTime.flatMap { at =>
                  val delay = RetryBackoff.exponentialMillis(
                    claim.record.publicationAttempts + 1,
                    settings.initialBackoff.toMillis,
                    settings.maxBackoff.toMillis
                  )
                  observe(
                    repository.retry(claim, at, at.plusMillis(delay), InterviewFailureCode.PublicationUnavailable),
                    "interviewWorkflow.retryPublication"
                  ).as(false)
                }
            }
        }
  }

  private def publishWithLease(
      transport: InterviewTransport[F],
      message: InterviewMessage,
      claim: ClaimedInterviewWorkflowCommand
  ): F[PublicationOutcome] = {
    def renew: F[Option[PublicationOutcome]] = currentTime.flatMap { now =>
      repository.renewPublication(claim, now, now.plusMillis(settings.claimLease.toMillis)).value.map {
        case Right(true)  => None
        case Right(false) => Some(PublicationOutcome.LeaseLost)
        case Left(_)      => Some(PublicationOutcome.CoordinationUnavailable)
      }
    }
    val heartbeat = fs2.Stream
      .awakeEvery[F]((settings.claimLease / 3).max(1.millis))
      .evalMap(_ => renew)
      .unNone
      .take(1)
      .compile
      .last
      .map(_.getOrElse(PublicationOutcome.CoordinationUnavailable))
    // Loading and authorization can consume most of the original claim lease.
    renew.flatMap {
      case Some(outcome) => Applicative[F].pure(outcome)
      case None          =>
        Applicative[F].unit.flatMap(_ => Temporal[F].race(transport.publish(message), heartbeat)).map {
          case Left(_)        => PublicationOutcome.Published
          case Right(outcome) => outcome
        }
    }
  }
}
