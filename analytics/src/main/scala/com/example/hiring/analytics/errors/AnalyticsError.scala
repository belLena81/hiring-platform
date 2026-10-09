package com.example.hiring.analytics.errors

import cats.data.NonEmptyChain

sealed abstract class AnalyticsError(message: String, cause: Option[Throwable] = None)
    extends RuntimeException(message, cause.orNull)

object AnalyticsError {
  private def render(problems: NonEmptyChain[String]): String = problems.toNonEmptyList.toList.mkString("; ")

  final case class InvalidInput(problems: NonEmptyChain[String]) extends AnalyticsError(render(problems))
  object InvalidInput {
    def one(problem: String): InvalidInput = InvalidInput(NonEmptyChain.one(problem))
  }

  /** Configuration and startup failures; accumulated problems render joined by `; `. */
  final case class InvalidConfiguration(problems: NonEmptyChain[String], underlying: Option[Throwable])
      extends AnalyticsError(render(problems), underlying)
  object InvalidConfiguration {
    def apply(detail: String, underlying: Option[Throwable] = None): InvalidConfiguration =
      InvalidConfiguration(NonEmptyChain.one(detail), underlying)
  }

  def fromProblems(problems: NonEmptyChain[String]): InvalidConfiguration = InvalidConfiguration(problems, None)

  final case class InvalidSourceSchema(missing: Vector[String])
      extends AnalyticsError(s"Kafka batch records are missing required columns: ${missing.mkString(", ")}")
  final case class InvalidLateFactSchema(detail: String)
      extends AnalyticsError(s"late hiring fact has an invalid schema: $detail")
  case object InvalidSilverSchema extends AnalyticsError("Silver dataset does not match its declared event schema")
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
  case object GuardedErasurePublicationRejected
      extends AnalyticsError("analytics erasure is not ready for guarded publication")
  case object LateFactReplayRejected extends AnalyticsError("late-fact replay selection is no longer eligible")
  case object LateFactReplayRequestConflict
      extends AnalyticsError("late-fact replay request ID conflicts with its durable selection")
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
  case object LakehouseLockOwnershipLost extends AnalyticsError("lakehouse mutex owner changed before release")
  case object LakehouseLockOwnershipUncertain
      extends AnalyticsError("lakehouse mutex acquisition is uncertain; manual ownership recovery required")
  final case class DeltaSchemaMismatch(path: String)
      extends AnalyticsError(s"Delta dataset schema differs from the expected analytics schema at $path")
  final case class MarkedSubjectRetained(path: String)
      extends AnalyticsError(s"marked subject remains in Delta dataset $path")
  case object InvalidBronzeSchema extends AnalyticsError("persisted Bronze dataset has an incompatible schema")
  case object StreamingAdmissionConflictsChanged
      extends AnalyticsError("streaming admission conflict set changed during ingestion")
  case object ReportNotSingular extends AnalyticsError("time-to-hire report is not singular")
  final case class ReportRowLimitExceeded(limit: Int) extends AnalyticsError(s"report output exceeds $limit rows")
  final case class KeyRetirementAuditUnverified(underlying: Throwable)
      extends AnalyticsError("key retirement audit could not verify every required surface", Some(underlying))
  case object LakehouseLockTimeout extends AnalyticsError("timed out waiting for the analytics lakehouse lock")
}
