package com.example.graphQL.cats.service.events

import cats.effect.{Clock, IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.{
  PendingSearchSessionWork,
  RepositoryError,
  SearchSessionWorkFailure,
  SearchSessionWorkRepository
}
import com.example.graphQL.cats.service.{BackgroundWorker, Diagnostics, LogEvent, LogField}
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

trait SearchSessionHandoff {
  def enqueue(session: SearchSession, event: OperationalEventEnvelope): IO[Unit]
}

object SearchSessionHandoff {
  private val WorkerName = "search-session-handoff"

  def resource(
      repository: SearchSessionWorkRepository,
      config: SearchSessionHandoffConfig,
      diagnostics: Diagnostics,
      clock: Clock[IO] = Clock[IO]
  ): Resource[IO, SearchSessionHandoff] = {
    val worker = workerLoop(repository, config, diagnostics, clock)
    List
      .tabulate(config.parallelism)(index =>
        BackgroundWorker.resource(s"$WorkerName-${index + 1}", diagnostics)(worker)
      )
      .sequence_
      .as(new SearchSessionHandoff {
        def enqueue(session: SearchSession, event: OperationalEventEnvelope): IO[Unit] =
          repository.enqueue(PendingSearchSessionWork(session, event), session.occurredAt).value.flatMap {
            case Right(())   => IO.unit
            case Left(error) =>
              diagnostics.emit(
                LogEvent.RuntimeFailed,
                fields = Map(
                  LogField.Outcome -> "REJECTED",
                  LogField.ErrorType -> errorType(error),
                  LogField.ErrorLocation -> "unavailable"
                )
              )
          }
      })
  }

  private def workerLoop(
      repository: SearchSessionWorkRepository,
      config: SearchSessionHandoffConfig,
      diagnostics: Diagnostics,
      clock: Clock[IO]
  ): IO[Unit] =
    (clock.realTimeInstant.flatMap { now =>
      repository.claim(config.workerId, now, now.plusMillis(config.lease.toMillis)).value.flatMap {
        case Right(Some(claim)) =>
          processClaim(repository, config, claim, now).flatMap {
            case WorkOutcome.Completed | WorkOutcome.RetryScheduled | WorkOutcome.Failed => IO.unit
            case WorkOutcome.OwnershipLost                                               =>
              diagnostics.emit(LogEvent.RuntimeFailed, fields = Map(LogField.Outcome -> "OWNERSHIP_LOST"))
            case WorkOutcome.RecoveryDeferred(error) =>
              diagnostics.emit(
                LogEvent.RuntimeFailed,
                fields = Map(LogField.Outcome -> "RECOVERY_DEFERRED", LogField.ErrorType -> errorType(error))
              ) *> IO.sleep(config.pollInterval)
          }
        case Right(None) => IO.sleep(config.pollInterval)
        case Left(error) =>
          diagnostics.emit(
            LogEvent.RuntimeFailed,
            fields = Map(
              LogField.Outcome -> "REJECTED",
              LogField.ErrorType -> errorType(error),
              LogField.ErrorLocation -> "unavailable"
            )
          ) *> IO.sleep(config.pollInterval)
      }
    }).foreverM

  private[events] enum WorkOutcome {
    case Completed, RetryScheduled, Failed, OwnershipLost
    case RecoveryDeferred(error: RepositoryError)
  }

  private[events] def processClaim(
      repository: SearchSessionWorkRepository,
      config: SearchSessionHandoffConfig,
      claim: com.example.graphQL.cats.service.port.ClaimedSearchSessionWork,
      now: java.time.Instant
  ): IO[WorkOutcome] =
    repository.complete(claim, now).value.flatMap {
      case Right(()) => IO.pure(WorkOutcome.Completed)
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
      case RepositoryError.Unavailable | RepositoryError.InvalidStoredData | RepositoryError.MissingWriteResult |
          RepositoryError.MissingStoredResult =>
        "java.lang.RuntimeException"
      case _ => "java.lang.IllegalStateException"
    }
}
