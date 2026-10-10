package com.example.hiring.analytics
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.service.erasure.*

import cats.effect.IO
import cats.effect.unsafe.implicits.global
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

  test("error codes are stable and never derive from exception messages") {
    assertEquals(AnalyticsErrorText.code(AnalyticsError.MalformedMarker), "MALFORMED_MARKER")
    assertEquals(AnalyticsErrorText.code(AnalyticsError.DataIntegrityViolation("secret")), "DATA_INTEGRITY_VIOLATION")
    assertEquals(AnalyticsErrorText.code(AnalyticsError.ReportNotSingular), "ANALYTICS_ERROR")
    assertEquals(AnalyticsErrorText.code(new RuntimeException("secret")), "UNEXPECTED_ERROR")
  }

  test("CLI labels are fixed text that never echoes dynamic error payloads") {
    val labels = Vector(
      AnalyticsError.InvalidInput.one("secret") -> "analytics input is invalid",
      AnalyticsError.InvalidConfiguration("secret") -> "analytics configuration is invalid",
      AnalyticsError.InvalidSourceSchema(Vector("x")) -> "analytics source schema is invalid",
      AnalyticsError.EmptyRequestedRange("t", 0, 1L) -> "requested analytics range is empty",
      AnalyticsError.ExpiredOffsetRange("t", 0, 1L, 2L) -> "requested analytics range expired",
      AnalyticsError.MissingOffsetRange("t", 0, 1L, 0L) -> "requested analytics range is incomplete",
      AnalyticsError.UnexpectedOffsetPartition("t", 0) -> "analytics source returned an unrequested partition",
      AnalyticsError.RunIdRangeConflict("r") -> "analytics run ID conflicts with existing ranges",
      AnalyticsError.MarkerLimitExceeded(1) -> "analytics erasure marker limit exceeded",
      AnalyticsError.ReportNotSingular -> "time-to-hire report is not singular"
    )
    labels.foreach((error, label) => assertEquals(AnalyticsErrorText.label(error), label))
    assert(AnalyticsErrorText.label(new IllegalStateException("secret")).endsWith("IllegalStateException"))
    assertEquals(AnalyticsErrorText.code(AnalyticsError.InvalidConfiguration("x")), "INVALID_CONFIGURATION")
    assertEquals(AnalyticsErrorText.code(AnalyticsError.InvalidSilverSchema), "INVALID_SILVER_SCHEMA")
    assertEquals(AnalyticsErrorText.code(AnalyticsError.InvalidGoldSchema), "INVALID_GOLD_SCHEMA")
    assertEquals(
      AnalyticsErrorText.code(AnalyticsError.PhysicalReclamationUnverified),
      "PHYSICAL_RECLAMATION_UNVERIFIED"
    )
  }

  test("unexpected failures are translated once and typed errors pass through") {
    import AnalyticsErrorTranslation.*
    val cause = new IllegalStateException("x")
    val wrap = AnalyticsError.SourceReadFailure(_)
    def run(work: IO[Int]) = work.attempt.unsafeRunSync()
    assertEquals(run(IO.raiseError(cause).translating(wrap)), Left(wrap(cause)))
    assertEquals(
      run(IO.raiseError(AnalyticsError.InvalidGoldSchema).translating(wrap)),
      Left(AnalyticsError.InvalidGoldSchema)
    )
    assertEquals(
      run(
        IO.raiseError(cause).translatingWith { case _: IllegalStateException => AnalyticsError.MalformedMarker }(wrap)
      ),
      Left(AnalyticsError.MalformedMarker)
    )
    assertEquals(run(IO.pure(1).translating(wrap)), Right(1))
  }

  test("invalid state goes to repair immediately and unknown failures have fewer retries") {
    val invalid = ErasureFailurePolicy.decide(AnalyticsError.InvalidGoldSchema, 1)
    val invalidSilver = ErasureFailurePolicy.decide(AnalyticsError.InvalidSilverSchema, 1)
    val corrupt = ErasureFailurePolicy.decide(AnalyticsError.DataIntegrityViolation("corrupt journal row"), 1)
    val unknownRetry = ErasureFailurePolicy.decide(new IllegalStateException("arbitrary detail"), 2)
    val unknownExhausted = ErasureFailurePolicy.decide(new IllegalStateException("arbitrary detail"), 3)

    assertEquals(invalid.category, ErasureFailureCategory.InvalidState)
    assertEquals(invalid.retryAfter, None)
    assertEquals(invalidSilver.category, ErasureFailureCategory.InvalidState)
    assertEquals(invalidSilver.retryAfter, None)
    assertEquals(corrupt.category, ErasureFailureCategory.InvalidState)
    assertEquals(corrupt.retryAfter, None)
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
