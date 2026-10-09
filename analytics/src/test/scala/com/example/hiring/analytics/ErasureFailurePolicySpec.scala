package com.example.hiring.analytics
import com.example.hiring.analytics.errors.*
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

  test("durable retry delays keep the historical capped doubling schedule") {
    def historical(nextAttempt: Int): FiniteDuration =
      math.min(300L, 5L * (1L << math.min(math.max(nextAttempt - 1, 0), 6))).seconds

    (-1 to 20).foreach(attempt => assertEquals(ErasureFailurePolicy.retryDelay(attempt), historical(attempt)))
    assertEquals(
      (1 to 9).map(ErasureFailurePolicy.retryDelay).toVector,
      Vector(5, 10, 20, 40, 80, 160, 300, 300, 300).map(_.seconds)
    )
  }

  test("every lakehouse failure introduced from generic wrappers keeps its transient source category") {
    val sourceFailures = Vector[AnalyticsError](
      AnalyticsError.LakehouseLockOwnershipLost,
      AnalyticsError.LakehouseLockOwnershipUncertain,
      AnalyticsError.DeltaSchemaMismatch("file:///tmp/table"),
      AnalyticsError.MarkedSubjectRetained("file:///tmp/table"),
      AnalyticsError.InvalidBronzeSchema,
      AnalyticsError.StreamingAdmissionConflictsChanged,
      AnalyticsError.ReportNotSingular,
      AnalyticsError.ReportRowLimitExceeded(10),
      AnalyticsError.KeyRetirementAuditUnverified(new RuntimeException)
    )
    sourceFailures.foreach { error =>
      val decision = ErasureFailurePolicy.decide(error, 1)
      assertEquals(decision.category, ErasureFailureCategory.TransientSource, error.getMessage)
      assertEquals(decision.retryAfter, Some(5.seconds))
    }
  }
}
