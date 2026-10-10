package com.example.hiring.analytics.errors

/** The single mapping from errors to operator-facing text. Messages and stack traces can carry credentials or source
  * payloads, so only these fixed codes and labels are emitted.
  */
object AnalyticsErrorText {

  /** A stable, non-sensitive error code for logs. */
  def code(error: Throwable): String = error match {
    case _: AnalyticsError.InvalidConfiguration       => "INVALID_CONFIGURATION"
    case _: AnalyticsError.DataIntegrityViolation     => "DATA_INTEGRITY_VIOLATION"
    case AnalyticsError.MalformedMarker               => "MALFORMED_MARKER"
    case AnalyticsError.InvalidSilverSchema           => "INVALID_SILVER_SCHEMA"
    case AnalyticsError.InvalidGoldSchema             => "INVALID_GOLD_SCHEMA"
    case AnalyticsError.PhysicalReclamationUnverified => "PHYSICAL_RECLAMATION_UNVERIFIED"
    case _: AnalyticsError                            => "ANALYTICS_ERROR"
    case _                                            => "UNEXPECTED_ERROR"
  }

  /** A fixed human-readable label for CLI output; dynamic values in error payloads are never rendered. */
  def label(error: Throwable): String = error match {
    case _: AnalyticsError.InvalidInput              => "analytics input is invalid"
    case _: AnalyticsError.InvalidConfiguration      => "analytics configuration is invalid"
    case _: AnalyticsError.InvalidSourceSchema       => "analytics source schema is invalid"
    case _: AnalyticsError.EmptyRequestedRange       => "requested analytics range is empty"
    case _: AnalyticsError.ExpiredOffsetRange        => "requested analytics range expired"
    case _: AnalyticsError.MissingOffsetRange        => "requested analytics range is incomplete"
    case _: AnalyticsError.UnexpectedOffsetPartition => "analytics source returned an unrequested partition"
    case _: AnalyticsError.RunIdRangeConflict        => "analytics run ID conflicts with existing ranges"
    case _: AnalyticsError.MarkerLimitExceeded       => "analytics erasure marker limit exceeded"
    case analyticsError: AnalyticsError              => analyticsError.getMessage
    case _ => s"analytics program failed unexpectedly: ${error.getClass.getSimpleName}"
  }
}
