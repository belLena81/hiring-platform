package com.example.graphQL.cats.service.events

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.repository.protocol.{PendingSearchSessionWork, RepositoryError, SearchSessionWorkFailure, SearchSessionWorkRepository}
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField}
import com.example.graphQL.cats.shared.events.{OperationalEventEnvelope, SearchSession}

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
  val noop: SearchSessionHandoff = new SearchSessionHandoff {
    def enqueue(session: SearchSession, event: OperationalEventEnvelope): IO[Unit] = IO.unit
  }

  def resource(
      repository: SearchSessionWorkRepository,
      config: SearchSessionHandoffConfig,
      diagnostics: Diagnostics
  ): Resource[IO, SearchSessionHandoff] = {
    val worker = workerLoop(repository, config, diagnostics)
    Resource.make(List.fill(config.parallelism)(worker.start).sequence)(_.traverse_(_.cancel)).as(new SearchSessionHandoff {
      def enqueue(session: SearchSession, event: OperationalEventEnvelope): IO[Unit] =
        repository.enqueue(PendingSearchSessionWork(session, event), session.occurredAt).flatMap {
          case Right(()) => IO.unit
          case Left(error) => diagnostics.emit(LogEvent.RuntimeFailed, fields = Map(
            LogField.Outcome -> "REJECTED",
            LogField.ErrorType -> errorType(error),
            LogField.ErrorLocation -> "unavailable"
          ))
        }
    })
  }

  private def workerLoop(repository: SearchSessionWorkRepository, config: SearchSessionHandoffConfig, diagnostics: Diagnostics): IO[Unit] =
    (IO.realTimeInstant.flatMap { now =>
      repository.claim(config.workerId, now, now.plusMillis(config.lease.toMillis)).flatMap {
        case Right(Some(claim)) =>
          repository.complete(claim, now).flatMap {
            case Right(()) => IO.unit
            case Left(_) if claim.attempts >= config.maxAttempts =>
              repository.fail(claim, SearchSessionWorkFailure.RetryExhausted, now).void
            case Left(_) => repository.retry(claim, now.plusMillis(config.retryDelay.toMillis)).void
          }
        case Right(None) => IO.sleep(config.pollInterval)
        case Left(error) => diagnostics.emit(LogEvent.RuntimeFailed, fields = Map(
          LogField.Outcome -> "REJECTED",
          LogField.ErrorType -> errorType(error),
          LogField.ErrorLocation -> "unavailable"
        )) *> IO.sleep(config.pollInterval)
      }
    }).foreverM

  private def errorType(error: RepositoryError): String =
    error match {
      case RepositoryError.Unavailable => "java.lang.RuntimeException"
      case _ => "java.lang.IllegalStateException"
    }
}
