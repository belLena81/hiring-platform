package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Resource}
import com.example.graphQL.cats.repository.protocol.RepositoryError
import com.mongodb.{MongoCommandException, MongoException, MongoWriteException}
import com.mongodb.reactivestreams.client.{ClientSession, MongoClient}
import retry.*
import retry.RetryPolicies.*

import scala.concurrent.duration.*

private[mongo] trait MongoTransactionRunner {
  def run[A](operation: Option[ClientSession] => IO[Either[RepositoryError, A]]): IO[Either[RepositoryError, A]]
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

  val noTransaction: MongoTransactionRunner =
    new MongoTransactionRunner {
      override def run[A](
          operation: Option[ClientSession] => IO[Either[RepositoryError, A]]
      ): IO[Either[RepositoryError, A]] =
        operation(None)
    }

  def sessions(
      client: MongoClient,
      duplicateKeyError: RepositoryError,
      retryPolicy: RetryPolicy = RetryPolicy()
  ): MongoTransactionRunner =
    new MongoTransactionRunner {
      override def run[A](
          operation: Option[ClientSession] => IO[Either[RepositoryError, A]]
      ): IO[Either[RepositoryError, A]] = {
        def withSession[A](use: ClientSession => IO[A]): IO[A] =
          Resource
            .make(PublisherBridge.first(client.startSession()).flatMap {
              case Some(session) => IO.pure(session)
              case None          => IO.raiseError(new IllegalStateException("Mongo startSession returned no session"))
            })(session => IO.blocking(session.close()))
            .use(use)

        def abort(session: ClientSession): IO[Unit] =
          PublisherBridge.first(session.abortTransaction()).attempt.void

        val transactionBackoff = backoff[CommitOutcome[A]](retryPolicy, retryPolicy.maxTransactionAttempts)
        val commitBackoff = backoff[CommitOutcome[A]](retryPolicy, retryPolicy.maxCommitAttempts)

        def labels(error: MongoException): Set[String] =
          Set("TransientTransactionError", "UnknownTransactionCommitResult").filter(error.hasErrorLabel)

        def commit(session: ClientSession, result: A): IO[CommitOutcome[A]] = {
          val commitAttempt =
            PublisherBridge.first(session.commitTransaction()).as(CommitOutcome.Completed(Right(result))).handleError {
              case error: MongoException =>
                RetryDecision.decide(RetryStage.Commit, labels(error)) match {
                  case RetryDecision.RetryCommit      => CommitOutcome.RetryCommit(error)
                  case RetryDecision.RetryTransaction => CommitOutcome.RetryTransaction(error)
                  case RetryDecision.Fail             => CommitOutcome.Completed(mapWrite(error, duplicateKeyError))
                }
              case error => CommitOutcome.Completed(mapWrite(error, duplicateKeyError))
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
          IO.delay(session.startTransaction()) *> operation(Some(session)).attempt.flatMap {
            case Left(error) =>
              error match {
                case mongo: MongoException
                    if RetryDecision.decide(RetryStage.Operation, labels(mongo)) == RetryDecision.RetryTransaction =>
                  abort(session).as(CommitOutcome.RetryTransaction(mongo))
                case _ => abort(session).as(CommitOutcome.Completed(mapWrite(error, duplicateKeyError)))
              }
            case Right(Left(error)) =>
              abort(session).as(CommitOutcome.Completed(Left(error)))
            case Right(Right(result)) =>
              commit(session, result).flatMap {
                case retry @ CommitOutcome.RetryTransaction(_) => abort(session).as(retry)
                case CommitOutcome.RetryCommit(error)          =>
                  abort(session).as(CommitOutcome.Completed(mapWrite(error, duplicateKeyError)))
                case completed @ CommitOutcome.Completed(_) => IO.pure(completed)
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
