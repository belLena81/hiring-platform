package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
import com.example.graphQL.cats.repository.protocol.{MutationWriteContext, RepositoryError}
import com.mongodb.client.result.UpdateResult
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
      atomic <- MongoMutationWriteContext.run(MutationWriteContext.noop, runner, transactionRequired = true) { _ =>
        operations.update(_ + 1).as(Right("atomic"))
      }
      direct <- MongoMutationWriteContext.run(MutationWriteContext.noop, runner, transactionRequired = false) { _ =>
        operations.update(_ + 1).as(Right("direct"))
      }
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
      result <- MongoMutationWriteContext.run(MongoMutationWriteContext(None), runner, transactionRequired = true) { session =>
        IO.pure(Right(session))
      }
      runCount <- runs.get
    } yield {
      assertEquals(result, Right(None))
      assertEquals(runCount, 0)
    }
  }

  test("account deletion classifies a job replacement zero-match as conflict") {
    val zeroMatch = UpdateResult.acknowledged(0L, 0L, null)
    assertEquals(MongoUserRepository.classifyJobClose(Some(zeroMatch)), Left(RepositoryError.Conflict))
  }

  test("account deletion classifies a missing job replacement result as unavailable") {
    assertEquals(MongoUserRepository.classifyJobClose(None), Left(RepositoryError.Unavailable))
  }

  private def recordingRunner(runs: Ref[IO, Int]): MongoTransactionRunner =
    new MongoTransactionRunner {
      override def run[A](operation: Option[com.mongodb.reactivestreams.client.ClientSession] => IO[Either[RepositoryError, A]]): IO[Either[RepositoryError, A]] =
        runs.update(_ + 1) *> operation(None)
    }
}
