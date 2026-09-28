package com.example.hiring.analytics.errors

import cats.data.NonEmptyChain

sealed abstract class AnalyticsError(message: String, cause: Option[Throwable] = None)
    extends RuntimeException(message, cause.orNull)

object AnalyticsError {
  final case class InvalidInput(problems: NonEmptyChain[String])
      extends AnalyticsError(problems.toNonEmptyList.toList.mkString("; "))
  final case class InvalidConfiguration(detail: String) extends AnalyticsError(detail)
  final case class InvalidSourceSchema(missing: Vector[String])
      extends AnalyticsError(s"Kafka batch records are missing required columns: ${missing.mkString(", ")}")
  final case class EmptyRequestedRange(topic: String, partition: Int, offset: Long)
      extends AnalyticsError(s"requested analytics range is empty: $topic partition $partition at offset $offset")
  final case class ExpiredOffsetRange(topic: String, partition: Int, requestedStart: Long, earliestAvailable: Long)
      extends AnalyticsError(
        s"requested analytics offset expired: $topic partition $partition starts at $requestedStart; earliest available is $earliestAvailable"
      )
  final case class MissingOffsetRange(topic: String, partition: Int, requested: Long, observed: Long)
      extends AnalyticsError(
        s"requested analytics range is incomplete: $topic partition $partition has $observed of $requested offsets"
      )
  final case class UnexpectedOffsetPartition(topic: String, partition: Int)
      extends AnalyticsError(s"analytics source returned an unrequested partition: $topic partition $partition")
  final case class RunIdRangeConflict(runId: String)
      extends AnalyticsError(s"analytics run ID '$runId' was already used with different offset ranges")
  case object MissingMarkerCollection extends AnalyticsError("analytics erasure request collection is unavailable")
  case object MalformedMarker extends AnalyticsError("pending analytics erasure request has an invalid subject id")
  case object ErasureNotReady extends AnalyticsError("analytics erasure is not ready for guarded publication")
  case object ErasureDeferred extends AnalyticsError("analytics erasure was durably deferred for a later retry")
  case object InvalidGoldSchema extends AnalyticsError("Gold dataset does not match its declared report schema")
  case object PhysicalReclamationUnverified
      extends AnalyticsError("analytics erasure could not verify retention-safe physical Delta reclamation")
  final case class MarkerLimitExceeded(limit: Int)
      extends AnalyticsError(s"pending analytics erasure marker limit exceeded ($limit)")
  final case class MarkerStorageFailure(underlying: Throwable)
      extends AnalyticsError("analytics erasure marker storage is unavailable", Some(underlying))
  final case class SourceReadFailure(underlying: Throwable)
      extends AnalyticsError("analytics source read failed", Some(underlying))
  final case class LakehouseFailure(underlying: Throwable)
      extends AnalyticsError("analytics lakehouse operation failed", Some(underlying))
  final case class SparkStartupFailure(underlying: Throwable)
      extends AnalyticsError("analytics Spark session could not start", Some(underlying))
  final case class MongoConnectionFailure(underlying: Throwable)
      extends AnalyticsError("analytics Mongo client could not start", Some(underlying))
  case object LakehouseLockTimeout extends AnalyticsError("timed out waiting for the analytics lakehouse lock")
}
