package com.example.graphQL.cats.service.auth

import cats.effect.{Resource, Temporal}
import cats.syntax.all.*
import com.example.graphQL.cats.service.{BackgroundWorker, Diagnostics, LogEvent, LogField, LogFields}
import com.example.graphQL.cats.service.Diagnostics.*
import com.example.graphQL.cats.service.port.{RecruiterJobClosureProgress, RecruiterJobClosureRepository}
import scala.concurrent.duration.*

/** Settings for the follow-up closure loop: how long to wait when nothing is pending and the bounds of the retry
  * backoff after a failed page. All values are positive and the cap is not below the initial delay.
  */
final case class RecruiterJobClosureSchedule private (
    idle: FiniteDuration,
    initialBackoff: FiniteDuration,
    maxBackoff: FiniteDuration
)

object RecruiterJobClosureSchedule {
  val Default: RecruiterJobClosureSchedule = RecruiterJobClosureSchedule(1.second, 1.second, 30.seconds)

  def from(
      idle: FiniteDuration,
      initialBackoff: FiniteDuration,
      maxBackoff: FiniteDuration
  ): Option[RecruiterJobClosureSchedule] =
    Option.when(idle > Duration.Zero && initialBackoff > Duration.Zero && maxBackoff >= initialBackoff)(
      RecruiterJobClosureSchedule(idle, initialBackoff, maxBackoff)
    )
}

/** Closes the open jobs of deleted recruiters that did not fit the first page closed by the deletion transaction.
  *
  * Each step is one idempotent transaction in the repository, so a crash between steps resumes from the stored state
  * with no duplicate `JOB_CLOSED` event. A failed step is reported and retried with capped exponential backoff; the
  * loop itself never ends on an error, only on release of the resource.
  */
final class RecruiterJobClosureWorker[F[_]](using F: Temporal[F])(
    repository: RecruiterJobClosureRepository[F],
    diagnostics: Diagnostics[F],
    schedule: RecruiterJobClosureSchedule = RecruiterJobClosureSchedule.Default,
    alive: F[Unit] = F.unit
) {
  private val name = "recruiterJobClosure"

  /** One closure step; a typed or thrown failure is reported and returned as `None`. */
  def runOnce: F[Option[RecruiterJobClosureProgress]] =
    repository.closeNextPage.value.attempt.flatMap {
      case Right(Right(progress)) => F.pure(Some(progress))
      case Right(Left(error))     =>
        diagnostics
          .emit(
            LogEvent.RuntimeFailed,
            fields = Map(LogField.Worker -> name, LogField.SpanName -> s"$name.${error.toString}")
          )
          .as(None)
      case Left(error) =>
        diagnostics
          .emit(LogEvent.RuntimeFailed, fields = LogFields.failure(error) + (LogField.Worker -> name))
          .as(None)
    }

  private val loop: F[Unit] =
    F.tailRecM(schedule.initialBackoff) { backoff =>
      runOnce.flatMap {
        case Some(RecruiterJobClosureProgress.Idle) =>
          alive *> F.sleep(schedule.idle).as(Left(schedule.initialBackoff))
        case Some(_) => alive.as(Left(schedule.initialBackoff))
        case None    => F.sleep(backoff).as(Left((backoff * 2).min(schedule.maxBackoff)))
      }
    }

  def resource: Resource[F, Unit] = BackgroundWorker.resource(name, diagnostics)(loop)
}
