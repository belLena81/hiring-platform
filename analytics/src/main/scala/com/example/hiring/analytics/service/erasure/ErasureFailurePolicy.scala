package com.example.hiring.analytics.service.erasure
import com.example.hiring.analytics.errors.AnalyticsError

import scala.concurrent.duration.*

final case class ErasureFailureDecision(category: ErasureFailureCategory, retryAfter: Option[FiniteDuration])

/** Pure classification and bounded retry schedule for durable erasure failures. */
private[analytics] object ErasureFailurePolicy {
  private val MaximumTransientAttempts = 8
  private val MaximumUnknownAttempts = 3

  def decide(error: Throwable, nextAttempt: Int): ErasureFailureDecision = {
    val category = error match {
      case _: AnalyticsError.MarkerStorageFailure | _: AnalyticsError.MongoConnectionFailure =>
        ErasureFailureCategory.TransientStorage
      case _: AnalyticsError.SourceReadFailure | _: AnalyticsError.LakehouseFailure |
          _: AnalyticsError.SparkStartupFailure =>
        ErasureFailureCategory.TransientSource
      case AnalyticsError.MalformedMarker | AnalyticsError.InvalidGoldSchema | _: AnalyticsError.InvalidConfiguration |
          AnalyticsError.PhysicalReclamationUnverified =>
        ErasureFailureCategory.InvalidState
      case _ => ErasureFailureCategory.Unknown
    }
    val retryable = nextAttempt > 0 && (category match {
      case ErasureFailureCategory.InvalidState => false
      case ErasureFailureCategory.Unknown      => nextAttempt < MaximumUnknownAttempts
      case _                                   => nextAttempt < MaximumTransientAttempts
    })
    val seconds = math.min(300L, 5L * (1L << math.min(math.max(nextAttempt - 1, 0), 6)))
    ErasureFailureDecision(category, Option.when(retryable)(seconds.seconds))
  }
}
