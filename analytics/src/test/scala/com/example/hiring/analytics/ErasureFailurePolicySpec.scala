package com.example.hiring.analytics

import com.example.hiring.analytics.erasure.{ErasureFailureCategory, ErasureFailurePolicy}

import munit.FunSuite

import scala.concurrent.duration.*

final class ErasureFailurePolicySpec extends FunSuite {
  test("transient failures retry with capped exponential backoff and then require repair") {
    val first = ErasureFailurePolicy.decide(AnalyticsError.MarkerStorageFailure(new RuntimeException), 1)
    val seventh = ErasureFailurePolicy.decide(AnalyticsError.SourceReadFailure(new RuntimeException), 7)
    val exhausted = ErasureFailurePolicy.decide(AnalyticsError.MongoConnectionFailure(new RuntimeException), 8)

    assertEquals(first.category, ErasureFailureCategory.TransientStorage)
    assertEquals(first.retryAfter, Some(5.seconds))
    assertEquals(seventh.category, ErasureFailureCategory.TransientSource)
    assertEquals(seventh.retryAfter, Some(300.seconds))
    assertEquals(exhausted.retryAfter, None)
  }

  test("invalid state goes to repair immediately and unknown failures have fewer retries") {
    val invalid = ErasureFailurePolicy.decide(AnalyticsError.InvalidGoldSchema, 1)
    val unknownRetry = ErasureFailurePolicy.decide(new IllegalStateException("arbitrary detail"), 2)
    val unknownExhausted = ErasureFailurePolicy.decide(new IllegalStateException("arbitrary detail"), 3)

    assertEquals(invalid.category, ErasureFailureCategory.InvalidState)
    assertEquals(invalid.retryAfter, None)
    assertEquals(unknownRetry.category, ErasureFailureCategory.Unknown)
    assertEquals(unknownRetry.retryAfter, Some(10.seconds))
    assertEquals(unknownExhausted.retryAfter, None)
  }
}
