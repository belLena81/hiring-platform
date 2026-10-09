package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.example.graphQL.cats.service.port.{MutationWriteContext, RepositoryError, RepositoryIO}
import com.mongodb.client.result.UpdateResult
import mongo4cats.client.ClientSession
import munit.CatsEffectSuite
import retry.*

final class MongoTransactionRetrySpec extends CatsEffectSuite {
  test("operation transient labels request a transaction retry") {
    assertEquals(
      MongoTransactionRunner.RetryDecision.decide(
        MongoTransactionRunner.RetryStage.Operation,
        Set("TransientTransactionError")
      ),
      MongoTransactionRunner.RetryDecision.RetryTransaction
    )
  }

  test("unknown commit results request a commit retry") {
    assertEquals(
      MongoTransactionRunner.RetryDecision.decide(
        MongoTransactionRunner.RetryStage.Commit,
        Set("UnknownTransactionCommitResult")
      ),
      MongoTransactionRunner.RetryDecision.RetryCommit
    )
  }

  test("transient commit results request a transaction retry") {
    assertEquals(
      MongoTransactionRunner.RetryDecision.decide(
        MongoTransactionRunner.RetryStage.Commit,
        Set("TransientTransactionError")
      ),
      MongoTransactionRunner.RetryDecision.RetryTransaction
    )
  }

  test("ordinary failures stop without retry") {
    assertEquals(
      MongoTransactionRunner.RetryDecision.decide(
        MongoTransactionRunner.RetryStage.Operation,
        Set.empty
      ),
      MongoTransactionRunner.RetryDecision.Fail
    )
  }

  test("retry decisions cover every stage and relevant label combination") {
    import MongoTransactionRunner.{RetryDecision, RetryStage}

    val labels = List(
      Set.empty[String],
      Set("TransientTransactionError"),
      Set("UnknownTransactionCommitResult"),
      Set("TransientTransactionError", "UnknownTransactionCommitResult")
    )
    val expectedOperation = List(
      RetryDecision.Fail,
      RetryDecision.RetryTransaction,
      RetryDecision.Fail,
      RetryDecision.RetryTransaction
    )
    val expectedCommit = List(
      RetryDecision.Fail,
      RetryDecision.RetryTransaction,
      RetryDecision.RetryCommit,
      RetryDecision.RetryCommit
    )

    assertEquals(labels.map(RetryDecision.decide(RetryStage.Operation, _)), expectedOperation)
    assertEquals(labels.map(RetryDecision.decide(RetryStage.Commit, _)), expectedCommit)
  }

  test("cats-retry bounds transaction attempts") {
    val policy = MongoTransactionRunner.RetryPolicy(
      maxTransactionAttempts = 3,
      initialDelay = scala.concurrent.duration.Duration.Zero,
      maxDelay = scala.concurrent.duration.Duration.Zero
    )
    for {
      attempts <- Ref.of[IO, Int](0)
      result <- retryingOnFailures(attempts.update(_ + 1).as(false))(
        policy = MongoTransactionRunner.backoff[Boolean](policy, policy.maxTransactionAttempts),
        valueHandler = (_, _) => IO.pure(HandlerDecision.Continue)
      )
      observed <- attempts.get
    } yield {
      assertEquals(observed, 3)
      assertEquals(result, Left(false))
    }
  }

  test("noop context opens a repository transaction only for an atomic multi-write") {
    for {
      runs <- Ref.of[IO, Int](0)
      operations <- Ref.of[IO, Int](0)
      runner = recordingRunner(runs)
      atomic <- MongoMutationWriteContext
        .run(MutationWriteContext.directWrite, runner, transactionRequired = true) { _ =>
          RepositoryIO.lift(operations.update(_ + 1)).as("atomic")
        }
        .value
      direct <- MongoMutationWriteContext
        .run(MutationWriteContext.directWrite, runner, transactionRequired = false) { _ =>
          RepositoryIO.lift(operations.update(_ + 1)).as("direct")
        }
        .value
      runCount <- runs.get
      operationCount <- operations.get
    } yield {
      assertEquals(atomic, Right("atomic"))
      assertEquals(direct, Right("direct"))
      assertEquals(runCount, 1)
      assertEquals(operationCount, 2)
    }
  }

  test("Mongo receipt context joins its supplied session without nesting a transaction") {
    for {
      runs <- Ref.of[IO, Int](0)
      runner = recordingRunner(runs)
      result <- MongoMutationWriteContext
        .run(MongoMutationWriteContext(None), runner, transactionRequired = true) { session =>
          RepositoryIO.fromEither(Right(session))
        }
        .value
      runCount <- runs.get
    } yield {
      assertEquals(result, Right(None))
      assertEquals(runCount, 0)
    }
  }

  test("foreign adapter contexts fail as effects for session extraction and execution") {
    val foreign = new MutationWriteContext {}
    for {
      runs <- Ref.of[IO, Int](0)
      sessionFailure <- MongoMutationWriteContext.session(foreign).attempt
      runFailure <- MongoMutationWriteContext
        .run(foreign, recordingRunner(runs), transactionRequired = false)(_ =>
          RepositoryIO.fromEither(Right("unused"): Either[RepositoryError, String])
        )
        .value
        .attempt
      runCount <- runs.get
    } yield {
      assert(sessionFailure.swap.exists(_.isInstanceOf[MongoSetupError]))
      assert(runFailure.swap.exists(_.isInstanceOf[MongoSetupError]))
      assertEquals(runCount, 0)
    }
  }

  test("account deletion classifies a job replacement zero-match as conflict") {
    val zeroMatch = UpdateResult.acknowledged(0L, 0L, null)
    assertEquals(MongoUserRepository.classifyJobClose(Some(zeroMatch)), Left(RepositoryError.Conflict))
  }

  test("account deletion classifies a missing job replacement result precisely") {
    assertEquals(MongoUserRepository.classifyJobClose(None), Left(RepositoryError.MissingWriteResult))
  }

  private def recordingRunner(runs: Ref[IO, Int]): MongoTransactionRunner =
    new MongoTransactionRunner {
      override def run[A](
          operation: Option[ClientSession[IO]] => RepositoryIO[A]
      ): RepositoryIO[A] = RepositoryIO.lift(runs.update(_ + 1)) *> operation(None)
    }
}
