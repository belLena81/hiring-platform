package com.example.graphQL.cats.repository.mongo

import munit.FunSuite
import scala.concurrent.duration.*

final class MongoTransactionRetrySpec extends FunSuite {
  private val policy = MongoTransactionRunner.RetryPolicy(
    maxTransactionAttempts = 3,
    maxCommitAttempts = 3,
    initialDelay = Duration.Zero,
    maxDelay = Duration.Zero
  )

  test("operation retries only transient transaction errors within its bound") {
    assertEquals(
      MongoTransactionRunner.RetryDecision.decide(
        MongoTransactionRunner.RetryStage.Operation,
        1,
        Set("TransientTransactionError"),
        policy
      ),
      MongoTransactionRunner.RetryDecision.RetryTransaction
    )
    assertEquals(
      MongoTransactionRunner.RetryDecision.decide(
        MongoTransactionRunner.RetryStage.Operation,
        3,
        Set("TransientTransactionError"),
        policy
      ),
      MongoTransactionRunner.RetryDecision.Fail
    )
  }

  test("commit retries its acknowledgement without re-running the transaction") {
    assertEquals(
      MongoTransactionRunner.RetryDecision.decide(
        MongoTransactionRunner.RetryStage.Commit,
        1,
        Set("UnknownTransactionCommitResult"),
        policy
      ),
      MongoTransactionRunner.RetryDecision.RetryCommit
    )
    assertEquals(
      MongoTransactionRunner.RetryDecision.decide(
        MongoTransactionRunner.RetryStage.Commit,
        3,
        Set("UnknownTransactionCommitResult"),
        policy
      ),
      MongoTransactionRunner.RetryDecision.Fail
    )
  }

  test("ordinary write failures never receive a transaction retry") {
    assertEquals(
      MongoTransactionRunner.RetryDecision.decide(
        MongoTransactionRunner.RetryStage.Operation,
        1,
        Set.empty,
        policy
      ),
      MongoTransactionRunner.RetryDecision.Fail
    )
  }
}
