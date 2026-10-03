package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.service.port.{RepositoryError, RepositoryIO}
import com.example.graphQL.cats.service.Diagnostics
import com.mongodb.{MongoCommandException, MongoException, MongoWriteException}
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

  enum RetryStage {
    case Operation, Commit
  }

  enum RetryDecision {
    case RetryTransaction, RetryCommit, Fail
  }

  object RetryDecision {
    def decide(stage: RetryStage, labels: Set[String]): RetryDecision =
      stage match {
        case RetryStage.Operation if labels.contains("TransientTransactionError") =>
          RetryTransaction
        case RetryStage.Commit if labels.contains("UnknownTransactionCommitResult") =>
          RetryCommit
        case RetryStage.Commit if labels.contains("TransientTransactionError") =>
          RetryTransaction
        case _ => Fail
      }
  }

  private enum CommitOutcome[+A] {
    case Completed(result: Either[RepositoryError, A])
    case RetryTransaction(error: Throwable)
    case RetryCommit(error: Throwable)
  }

  def sessions(
      client: MongoClient[IO],
      duplicateKeyError: RepositoryError,
      retryPolicy: RetryPolicy = RetryPolicy(),
      diagnostics: Diagnostics
  ): MongoTransactionRunner =
    new MongoTransactionRunner {
      override def run[A](
          operation: Option[ClientSession[IO]] => RepositoryIO[A]
      ): RepositoryIO[A] = RepositoryIO.fromIOEither {
        def withSession[A](use: ClientSession[IO] => IO[A]): IO[A] =
          client.startSession(ClientSessionOptions()).use(use)

        def abort(session: ClientSession[IO]): IO[Unit] =
          abortActiveTransaction(diagnostics, IO.delay(session.hasActiveTransaction), session.abortTransaction)

        val transactionBackoff = backoff[CommitOutcome[A]](retryPolicy, retryPolicy.maxTransactionAttempts)
        val commitBackoff = backoff[CommitOutcome[A]](retryPolicy, retryPolicy.maxCommitAttempts)

        def labels(error: MongoException): Set[String] =
          Set("TransientTransactionError", "UnknownTransactionCommitResult").filter(error.hasErrorLabel)

        def commit(session: ClientSession[IO], result: A): IO[CommitOutcome[A]] = {
          val commitAttempt =
            session.commitTransaction
              .as(CommitOutcome.Completed(Right(result)))
              .handleErrorWith {
                case error: MongoException =>
                  MongoRepositorySupport.reportFailure(diagnostics, "transaction.commit", error).as {
                    RetryDecision.decide(RetryStage.Commit, labels(error)) match {
                      case RetryDecision.RetryCommit      => CommitOutcome.RetryCommit(error)
                      case RetryDecision.RetryTransaction => CommitOutcome.RetryTransaction(error)
                      case RetryDecision.Fail             => CommitOutcome.Completed(mapWrite(error, duplicateKeyError))
                    }
                  }
                case error =>
                  MongoRepositorySupport
                    .reportFailure(diagnostics, "transaction.commit", error)
                    .as(CommitOutcome.Completed(mapWrite(error, duplicateKeyError)))
              }

          retryingOnFailures(commitAttempt)(
            policy = commitBackoff,
            valueHandler = (result, _) =>
              IO.pure(result match {
                case CommitOutcome.RetryCommit(_) => HandlerDecision.Continue
                case _                            => HandlerDecision.Stop
              })
          ).map(_.fold(identity, identity))
        }

        def attemptOnce: IO[CommitOutcome[A]] = withSession { session =>
          Resource
            .make(session.startTransaction.as(session))(abort)
            .use { active =>
              // IO owns session finalization and distinguishes typed rejection from driver failures.
              operation(Some(active)).value.attempt.flatMap {
                case Left(error) =>
                  error match {
                    case mongo: MongoException
                        if RetryDecision.decide(
                          RetryStage.Operation,
                          labels(mongo)
                        ) == RetryDecision.RetryTransaction =>
                      (abort(active) *> MongoRepositorySupport.reportFailure(
                        diagnostics,
                        "transaction.operation",
                        error
                      ))
                        .as(CommitOutcome.RetryTransaction(mongo))
                    case _ =>
                      (abort(active) *> MongoRepositorySupport.reportFailure(
                        diagnostics,
                        "transaction.operation",
                        error
                      ))
                        .as(CommitOutcome.Completed(mapWrite(error, duplicateKeyError)))
                  }
                case Right(Left(error)) =>
                  abort(active).as(CommitOutcome.Completed(Left(error)))
                case Right(Right(result)) =>
                  commit(active, result).flatMap {
                    case retry @ CommitOutcome.RetryTransaction(_) => abort(active).as(retry)
                    case CommitOutcome.RetryCommit(error)          =>
                      abort(active).as(CommitOutcome.Completed(mapWrite(error, duplicateKeyError)))
                    case completed @ CommitOutcome.Completed(_) => IO.pure(completed)
                  }
              }
            }
        }

        retryingOnFailures(attemptOnce)(
          policy = transactionBackoff,
          valueHandler = (result, _) =>
            IO.pure(result match {
              case CommitOutcome.RetryTransaction(_) => HandlerDecision.Continue
              case _                                 => HandlerDecision.Stop
            })
        ).map(_.fold(toResult, toResult))
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

  private def toResult[A](outcome: CommitOutcome[A]): Either[RepositoryError, A] = outcome match {
    case CommitOutcome.Completed(result)   => result
    case CommitOutcome.RetryTransaction(_) => Left(RepositoryError.Conflict)
    case CommitOutcome.RetryCommit(_)      => Left(RepositoryError.Unavailable)
  }

  private[mongo] def backoff[A](policy: RetryPolicy, maxAttempts: Int): retry.RetryPolicy[IO, A] =
    limitRetries[IO](math.max(maxAttempts - 1, 0)) join
      capDelay(policy.maxDelay, exponentialBackoff[IO](policy.initialDelay))

  private def mapWrite[A](error: Throwable, duplicateKeyError: RepositoryError): Either[RepositoryError, A] =
    error match {
      case write: MongoWriteException if write.getError.getCode == 11000 => Left(duplicateKeyError)
      case mongo: MongoException if isTransientTransactionError(mongo)   => Left(RepositoryError.Conflict)
      case _                                                             => Left(RepositoryError.Unavailable)
    }

  private[mongo] def isWriteConflict(error: MongoCommandException): Boolean =
    error.getErrorCode == 112 || error.hasErrorLabel("TransientTransactionError")

  private def isTransientTransactionError(error: Throwable): Boolean = error match {
    case mongo: MongoException => mongo.hasErrorLabel("TransientTransactionError")
    case _                     => false
  }

}
