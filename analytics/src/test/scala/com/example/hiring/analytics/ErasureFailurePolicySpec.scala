package com.example.hiring.analytics
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

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
    val invalidSilver = ErasureFailurePolicy.decide(AnalyticsError.InvalidSilverSchema, 1)
    val unknownRetry = ErasureFailurePolicy.decide(new IllegalStateException("arbitrary detail"), 2)
    val unknownExhausted = ErasureFailurePolicy.decide(new IllegalStateException("arbitrary detail"), 3)

    assertEquals(invalid.category, ErasureFailureCategory.InvalidState)
    assertEquals(invalid.retryAfter, None)
    assertEquals(invalidSilver.category, ErasureFailureCategory.InvalidState)
    assertEquals(invalidSilver.retryAfter, None)
    assertEquals(unknownRetry.category, ErasureFailureCategory.Unknown)
    assertEquals(unknownRetry.retryAfter, Some(10.seconds))
    assertEquals(unknownExhausted.retryAfter, None)
  }
}
