package com.example.graphQL.cats.service.events

import cats.Monad
import cats.effect.{Clock, Resource, Temporal}
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.{
  PendingSearchSessionWork,
  SearchSessionWorkFailure,
  SearchSessionWorkRepository
}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.{BackgroundWorker, Diagnostics, LogEvent, LogField, LogFields, LogOutcome}
import com.example.graphQL.cats.service.events.{OperationalEventEnvelope, SearchSession}

import scala.concurrent.duration.*

final case class SearchSessionHandoffConfig(
    workerId: String = "search-session-worker",
    parallelism: Int = 4,
    lease: FiniteDuration = 30.seconds,
    retryDelay: FiniteDuration = 250.millis,
    maxAttempts: Int = 3,
    pollInterval: FiniteDuration = 250.millis
)

trait SearchSessionHandoff[F[_]] {
  def enqueue(session: SearchSession, event: OperationalEventEnvelope): F[Unit]
}

object SearchSessionHandoff {
  private val WorkerName = "search-session-handoff"

  def resource[F[_]](using
      F: Temporal[F]
  )(
      repository: SearchSessionWorkRepository[F],
      config: SearchSessionHandoffConfig,
      diagnostics: Diagnostics[F],
      clock: Clock[F] = F,
      alive: F[Unit] = F.unit
  ): Resource[F, SearchSessionHandoff[F]] = {
    val worker = workerLoop(repository, config, diagnostics, clock, alive)
    List
      .tabulate(config.parallelism)(index =>
        BackgroundWorker.resource(s"$WorkerName-${index + 1}", diagnostics)(worker)
      )
      .sequence_
      .as(new SearchSessionHandoff[F] {
        def enqueue(session: SearchSession, event: OperationalEventEnvelope): F[Unit] =
          repository.enqueue(PendingSearchSessionWork(session, event), session.occurredAt).value.flatMap {
            case Right(())   => F.unit
            case Left(error) =>
              diagnostics.emit(
                LogEvent.RuntimeFailed,
                fields = Map(
                  LogField.Outcome -> LogOutcome.Rejected.wire,
                  LogField.ErrorType -> errorType(error),
                  LogField.ErrorLocation -> "unavailable"
                )
              )
          }
      })
  }

  /** An unexpected failure is reported and the loop resumes after `pollInterval`, so one fault never ends the worker.
    */
  private def workerLoop[F[_]](
      repository: SearchSessionWorkRepository[F],
      config: SearchSessionHandoffConfig,
      diagnostics: Diagnostics[F],
      clock: Clock[F],
      alive: F[Unit]
  )(using F: Temporal[F]): F[Unit] =
    iteration(repository, config, diagnostics, clock, alive)
      .handleErrorWith(error =>
        diagnostics.emit(
          LogEvent.RuntimeFailed,
          fields = LogFields.failure(error) + (LogField.Worker -> WorkerName)
        ) *> F.sleep(config.pollInterval)
      )
      .foreverM

  private def iteration[F[_]](
      repository: SearchSessionWorkRepository[F],
      config: SearchSessionHandoffConfig,
      diagnostics: Diagnostics[F],
      clock: Clock[F],
      alive: F[Unit]
  )(using F: Temporal[F]): F[Unit] =
    // Only a pass that reached the store and handled its claim (or found none) is progress for readiness.
    clock.realTimeInstant.flatMap { now =>
      repository.claim(config.workerId, now, now.plusMillis(config.lease.toMillis)).value.flatMap {
        case Right(Some(claim)) =>
          processClaim(repository, config, claim, now).flatMap {
            case WorkOutcome.Completed | WorkOutcome.RetryScheduled | WorkOutcome.Failed => alive
            case WorkOutcome.OwnershipLost                                               =>
              diagnostics.emit(LogEvent.RuntimeFailed, fields = Map(LogField.Outcome -> LogOutcome.OwnershipLost.wire))
            case WorkOutcome.RecoveryDeferred(error) =>
              diagnostics.emit(
                LogEvent.RuntimeFailed,
                fields =
                  Map(LogField.Outcome -> LogOutcome.RecoveryDeferred.wire, LogField.ErrorType -> errorType(error))
              ) *> F.sleep(config.pollInterval)
          }
        case Right(None) => alive *> F.sleep(config.pollInterval)
        case Left(error) =>
          diagnostics.emit(
            LogEvent.RuntimeFailed,
            fields = Map(
              LogField.Outcome -> LogOutcome.Rejected.wire,
              LogField.ErrorType -> errorType(error),
              LogField.ErrorLocation -> "unavailable"
            )
          ) *> F.sleep(config.pollInterval)
      }
    }

  private[events] enum WorkOutcome {
    case Completed, RetryScheduled, Failed, OwnershipLost
    case RecoveryDeferred(error: RepositoryError)
  }

  private[events] def processClaim[F[_]: Monad](
      repository: SearchSessionWorkRepository[F],
      config: SearchSessionHandoffConfig,
      claim: com.example.graphQL.cats.service.port.ClaimedSearchSessionWork,
      now: java.time.Instant
  ): F[WorkOutcome] =
    repository.complete(claim, now).value.flatMap {
      case Right(()) => Monad[F].pure(WorkOutcome.Completed)
      case Left(_)   =>
        // Persisted attempts count prior failed executions, starting at zero.
        val exhausted = claim.attempts.toLong + 1L >= config.maxAttempts.toLong
        val transition =
          if (exhausted) repository.fail(claim, SearchSessionWorkFailure.RetryExhausted, now)
          else repository.retry(claim, now.plusMillis(config.retryDelay.toMillis))
        transition.value.map {
          case Right(())                      => if (exhausted) WorkOutcome.Failed else WorkOutcome.RetryScheduled
          case Left(RepositoryError.Conflict) => WorkOutcome.OwnershipLost
          // Leave the guarded claim for lease recovery; never pretend the write succeeded.
          case Left(error) => WorkOutcome.RecoveryDeferred(error)
        }
    }

  private def errorType(error: RepositoryError): String =
    error match {
      case RepositoryError.Unavailable | RepositoryError.Overloaded | RepositoryError.VectorSearchUnavailable |
          RepositoryError.InvalidStoredData | RepositoryError.MissingStoredResult =>
        "java.lang.RuntimeException"
      case RepositoryError.DuplicateApplication | RepositoryError.AuthorityRevoked | RepositoryError.Conflict |
          RepositoryError.InvalidEvent =>
        "java.lang.IllegalStateException"
    }
}
