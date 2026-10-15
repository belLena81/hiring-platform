package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Outcome, Ref, Resource}
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy.Completion
import com.example.graphQL.cats.service.port.{RepositoryF}
import com.example.graphQL.cats.service.RepositoryError
import com.example.graphQL.cats.service.Diagnostics
import com.example.graphQL.cats.service.resilience.CircuitBreaker
import cats.syntax.all.*
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.models.client.ClientSessionOptions
import retry.*
import retry.RetryPolicies.*

import scala.concurrent.duration.*

private[mongo] trait MongoTransactionRunner {
  def run[A](operation: Option[ClientSession[IO]] => RepositoryF[IO, A]): RepositoryF[IO, A]
}

private[mongo] object MongoTransactionRunner {
  final case class RetryPolicy(
      maxTransactionAttempts: Int = 5,
      maxCommitAttempts: Int = 3,
      initialDelay: FiniteDuration = 25.millis,
      maxDelay: FiniteDuration = 250.millis
  )

  def sessions(
      client: MongoClient[IO],
      duplicateKeyError: RepositoryError,
      retryPolicy: RetryPolicy = RetryPolicy(),
      diagnostics: Diagnostics[IO],
      transientExhaustionError: RepositoryError = RepositoryError.Conflict,
      breaker: CircuitBreaker[IO] = CircuitBreaker.disabled[IO]
  ): MongoTransactionRunner =
    new MongoTransactionRunner {
      override def run[A](
          operation: Option[ClientSession[IO]] => RepositoryF[IO, A]
      ): RepositoryF[IO, A] = RepositoryF.fromFEither {
        // The error channel carries the original driver exception until the single final mapping below.
        def retrying[B](maxAttempts: Int, retriable: Throwable => Boolean)(action: IO[B]): IO[B] =
          retryingOnErrors(action)(
            policy = backoff[Throwable](retryPolicy, maxAttempts),
            errorHandler =
              (error, _) => IO.pure(if (retriable(error)) HandlerDecision.Continue else HandlerDecision.Stop)
          )

        def abort(session: ClientSession[IO]): IO[Unit] =
          abortActiveTransaction(diagnostics, IO.delay(session.hasActiveTransaction), session.abortTransaction)

        def commit(session: ClientSession[IO]): IO[Unit] =
          // A transient label means the whole transaction is retried, so the commit alone is not.
          retrying(retryPolicy.maxCommitAttempts, MongoErrors.isCommitRetriable)(
            session.commitTransaction.onError { case error =>
              MongoRepositorySupport.reportFailure(diagnostics, "transaction.commit", error)
            }
          )

        // Resource release owns the only abort, for typed rejection, failure and cancellation alike.
        val attemptOnce: IO[Either[RepositoryError, A]] =
          client.startSession(ClientSessionOptions()).use { session =>
            Resource.make(session.startTransaction.as(session))(abort).use { active =>
              operation(Some(active)).value
                .onError {
                  case error if !MongoErrors.isTransient(error) =>
                    MongoRepositorySupport.reportFailure(diagnostics, "transaction.operation", error)
                }
                .flatTap(_.traverse_(_ => commit(active)))
            }
          }

        // The breaker sees the outcome after the runner's own retries; an open breaker starts no session at all.
        Ref.of[IO, Boolean](false).flatMap { sawUnreachable =>
          // Retries can outlast the request timeout, so a call cancelled mid-retry still reports what it observed.
          val observed = attemptOnce.onError {
            case error if MongoErrors.isDependencyUnavailable(error) =>
              sawUnreachable.set(true)
          }
          breaker
            .protect(retrying(retryPolicy.maxTransactionAttempts, MongoErrors.isTransient)(observed))(
              classify(sawUnreachable.get)
            )
            .map(_.leftMap(_ => RepositoryError.Unavailable).flatten)
            .handleErrorWith(error =>
              // Per-attempt conflicts are not logged; the exhausted outcome is.
              MongoRepositorySupport
                .reportFailure(diagnostics, "transaction.exhausted", error)
                .whenA(MongoErrors.isTransient(error))
                .as(MongoErrors.toRepositoryError(error, duplicateKeyError, transientExhaustionError))
            )
        }
      }
    }

  /** Only a committed result counts as success. A typed business rejection says nothing about the dependency's health,
    * so it cannot close the breaker; a cancelled call counts only if an earlier attempt already saw the dependency
    * fail.
    */
  private[mongo] def classify[A](
      sawUnreachable: IO[Boolean]
  )(outcome: Outcome[IO, Throwable, Either[RepositoryError, A]]): IO[Completion] =
    outcome match {
      case Outcome.Succeeded(result) =>
        result.map(_.fold(_ => Completion.IgnoredFailure, _ => Completion.Success))
      case Outcome.Errored(error) =>
        IO.pure(
          if (MongoErrors.isDependencyUnavailable(error)) Completion.CountedFailure else Completion.IgnoredFailure
        )
      case Outcome.Canceled() =>
        sawUnreachable.map(if (_) Completion.CountedFailure else Completion.IgnoredFailure)
    }

  private[mongo] def abortActiveTransaction(
      diagnostics: Diagnostics[IO],
      isActive: IO[Boolean],
      abort: IO[Unit]
  ): IO[Unit] =
    // Resource constructs release effects at acquisition; inspect native state only when cleanup runs.
    MongoRepositorySupport.guardCleanup(diagnostics, "transaction.abort")(
      isActive.flatMap(active => if (active) abort else IO.unit)
    )

  private[mongo] def backoff[A](policy: RetryPolicy, maxAttempts: Int): retry.RetryPolicy[IO, A] =
    limitRetries[IO](math.max(maxAttempts - 1, 0)) join
      capDelay(policy.maxDelay, fullJitter[IO](policy.initialDelay))
}
