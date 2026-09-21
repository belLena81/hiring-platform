package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, Ref}
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
}
