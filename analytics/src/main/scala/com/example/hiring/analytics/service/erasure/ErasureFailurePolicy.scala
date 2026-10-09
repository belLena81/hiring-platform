package com.example.hiring.analytics.service.erasure
import com.example.hiring.analytics.errors.AnalyticsError

import scala.concurrent.duration.*

final case class ErasureFailureDecision(category: ErasureFailureCategory, retryAfter: Option[FiniteDuration])

/** Pure classification and bounded retry schedule for durable erasure failures. */
private[analytics] object ErasureFailurePolicy {
  private val MaximumTransientAttempts = 8
  private val MaximumUnknownAttempts = 3

  /** Durable retry delay before attempt `nextAttempt` (1-based): 5s doubled per prior attempt, capped at 300s. The
    * exponent is clamped at 6 (320s) before the cap, so the schedule is 5, 10, 20, 40, 80, 160, 300, 300, ... seconds.
    */
  private[analytics] def retryDelay(nextAttempt: Int): FiniteDuration = {
    val exponent = math.min(math.max(nextAttempt - 1, 0), 6)
    math.min(300L, 5L * (1L << exponent)).seconds
  }

  /** Exhaustive over every analytics error: a new error case is a compile error until it is categorized here. */
  private def categorize(error: AnalyticsError): ErasureFailureCategory = error match {
    case _: AnalyticsError.MarkerStorageFailure | _: AnalyticsError.MongoConnectionFailure =>
      ErasureFailureCategory.TransientStorage
    case _: AnalyticsError.SourceReadFailure | _: AnalyticsError.LakehouseFailure |
        _: AnalyticsError.SparkStartupFailure | AnalyticsError.LakehouseLockOwnershipLost |
        AnalyticsError.LakehouseLockOwnershipUncertain | _: AnalyticsError.DeltaSchemaMismatch |
        _: AnalyticsError.MarkedSubjectRetained | AnalyticsError.InvalidBronzeSchema |
        AnalyticsError.StreamingAdmissionConflictsChanged | AnalyticsError.ReportNotSingular |
        _: AnalyticsError.ReportRowLimitExceeded | _: AnalyticsError.KeyRetirementAuditUnverified =>
      ErasureFailureCategory.TransientSource
    case AnalyticsError.MalformedMarker | AnalyticsError.InvalidSilverSchema | AnalyticsError.InvalidGoldSchema |
        _: AnalyticsError.InvalidConfiguration | AnalyticsError.PhysicalReclamationUnverified =>
      ErasureFailureCategory.InvalidState
    case _: AnalyticsError.InvalidInput | _: AnalyticsError.InvalidSourceSchema |
        _: AnalyticsError.InvalidLateFactSchema | _: AnalyticsError.EmptyRequestedRange |
        _: AnalyticsError.ExpiredOffsetRange | _: AnalyticsError.MissingOffsetRange |
        _: AnalyticsError.UnexpectedOffsetPartition | _: AnalyticsError.RunIdRangeConflict |
        AnalyticsError.MissingMarkerCollection | AnalyticsError.GuardedErasurePublicationRejected |
        AnalyticsError.LateFactReplayRejected | AnalyticsError.LateFactReplayRequestConflict |
        _: AnalyticsError.MarkerLimitExceeded | AnalyticsError.LakehouseLockTimeout =>
      ErasureFailureCategory.Unknown
  }

  def decide(error: Throwable, nextAttempt: Int): ErasureFailureDecision = {
    val category = error match {
      case analytics: AnalyticsError => categorize(analytics)
      case _                         => ErasureFailureCategory.Unknown
    }
    val retryable = nextAttempt > 0 && (category match {
      case ErasureFailureCategory.InvalidState => false
      case ErasureFailureCategory.Unknown      => nextAttempt < MaximumUnknownAttempts
      case _                                   => nextAttempt < MaximumTransientAttempts
    })
    ErasureFailureDecision(category, Option.when(retryable)(retryDelay(nextAttempt)))
  }
}
