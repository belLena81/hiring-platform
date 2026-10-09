package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.service.port.{RepositoryError, RepositoryIO}
import com.example.graphQL.cats.service.Diagnostics
import cats.syntax.all.*
import mongo4cats.client.{ClientSession, MongoClient}
import mongo4cats.models.client.ClientSessionOptions
import retry.*
import retry.RetryPolicies.*

import scala.concurrent.duration.*

private[mongo] trait MongoTransactionRunner {
  def run[A](operation: Option[ClientSession[IO]] => RepositoryIO[A]): RepositoryIO[A]
}

private[mongo] object MongoTransactionRunner {
  final case class RetryPolicy(
      maxTransactionAttempts: Int = 3,
      maxCommitAttempts: Int = 3,
      initialDelay: FiniteDuration = 25.millis,
      maxDelay: FiniteDuration = 250.millis
  )

  def sessions(
      client: MongoClient[IO],
      duplicateKeyError: RepositoryError,
      retryPolicy: RetryPolicy = RetryPolicy(),
      diagnostics: Diagnostics,
      transientExhaustionError: RepositoryError = RepositoryError.Conflict
  ): MongoTransactionRunner =
    new MongoTransactionRunner {
      override def run[A](
          operation: Option[ClientSession[IO]] => RepositoryIO[A]
      ): RepositoryIO[A] = RepositoryIO.fromIOEither {
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
          retrying(retryPolicy.maxCommitAttempts, MongoErrors.isUnknownCommitResult)(
            session.commitTransaction.onError { case error =>
              MongoRepositorySupport.reportFailure(diagnostics, "transaction.commit", error)
            }
          )

        // Resource release owns the only abort, for typed rejection, failure and cancellation alike.
        val attemptOnce: IO[Either[RepositoryError, A]] =
          client.startSession(ClientSessionOptions()).use { session =>
            Resource.make(session.startTransaction.as(session))(abort).use { active =>
              operation(Some(active)).value
                .onError { case error =>
                  MongoRepositorySupport.reportFailure(diagnostics, "transaction.operation", error)
                }
                .flatTap(_.traverse_(_ => commit(active)))
            }
          }

        retrying(retryPolicy.maxTransactionAttempts, MongoErrors.isTransient)(attemptOnce)
          .handleError(MongoErrors.toRepositoryError(_, duplicateKeyError, transientExhaustionError))
      }
    }

  private[mongo] def abortActiveTransaction(
      diagnostics: Diagnostics,
      isActive: IO[Boolean],
      abort: IO[Unit]
  ): IO[Unit] =
    // Resource constructs release effects at acquisition; inspect native state only when cleanup runs.
    MongoRepositorySupport.guard(diagnostics, "transaction.abort")(
      isActive.flatMap(active => if (active) abort else IO.unit)
    )(_ => ())

  private[mongo] def backoff[A](policy: RetryPolicy, maxAttempts: Int): retry.RetryPolicy[IO, A] =
    limitRetries[IO](math.max(maxAttempts - 1, 0)) join
      capDelay(policy.maxDelay, exponentialBackoff[IO](policy.initialDelay))
}
