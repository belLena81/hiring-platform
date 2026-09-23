package com.example.hiring.analytics

import cats.data.{NonEmptyChain, Validated, ValidatedNec}
import cats.syntax.all.*

/** The analytics retention policy is intentionally separate from Kafka retention. */
object AnalyticsRetention {
  val BronzeDays: Int = 7
  val QuarantineDays: Int = 7
  val SilverDays: Int = 30
  val GoldDays: Int = 30
  val PublishedSnapshotDays: Int = 30
  val DeletionMarkerDays: Int = 31
  val MinimumContributors: Long = 10L
}

sealed abstract class AnalyticsError(message: String, cause: Throwable = null) extends RuntimeException(message, cause)

object AnalyticsError {
  final case class InvalidInput(problems: NonEmptyChain[String])
      extends AnalyticsError(problems.toNonEmptyList.toList.mkString("; "))
  final case class InvalidConfiguration(detail: String) extends AnalyticsError(detail)
  final case class InvalidSourceSchema(missing: Vector[String])
      extends AnalyticsError(s"Kafka batch records are missing required columns: ${missing.mkString(", ")}")
  case object MissingMarkerCollection extends AnalyticsError("analytics erasure request collection is unavailable")
  case object MalformedMarker extends AnalyticsError("pending analytics erasure request has an invalid subject id")
  final case class MarkerLimitExceeded(limit: Int)
      extends AnalyticsError(s"pending analytics erasure marker limit exceeded ($limit)")
  final case class MarkerStorageFailure(underlying: Throwable)
      extends AnalyticsError("analytics erasure marker storage is unavailable", underlying)
  final case class SourceReadFailure(underlying: Throwable)
      extends AnalyticsError("analytics source read failed", underlying)
  final case class LakehouseFailure(underlying: Throwable)
      extends AnalyticsError("analytics lakehouse operation failed", underlying)
  final case class SparkStartupFailure(underlying: Throwable)
      extends AnalyticsError("analytics Spark session could not start", underlying)
  final case class MongoConnectionFailure(underlying: Throwable)
      extends AnalyticsError("analytics Mongo client could not start", underlying)
}

opaque type RunId = String

object RunId {
  def from(value: String): ValidatedNec[String, RunId] =
    if (value != null && value.trim.nonEmpty) value.validNec
    else "run id must be non-empty".invalidNec

  extension (value: RunId) def value: String = value
}

opaque type SubjectToken = String

object SubjectToken {
  def fromHmac(value: String): SubjectToken = value

  extension (token: SubjectToken) def value: String = token
}

enum AnalyticsEventType(val wire: String) {
  case JobCreated extends AnalyticsEventType("JOB_CREATED")
  case JobUpdated extends AnalyticsEventType("JOB_UPDATED")
  case JobClosed extends AnalyticsEventType("JOB_CLOSED")
  case JobViewed extends AnalyticsEventType("JOB_VIEWED")
  case SearchPerformed extends AnalyticsEventType("SEARCH_PERFORMED")
  case SearchResultClicked extends AnalyticsEventType("SEARCH_RESULT_CLICKED")
  case ApplicationCreated extends AnalyticsEventType("APPLICATION_CREATED")
  case ApplicationStatusChanged extends AnalyticsEventType("APPLICATION_STATUS_CHANGED")
  case CandidateHired extends AnalyticsEventType("CANDIDATE_HIRED")
}

enum AnalyticsAggregateType(val wire: String) {
  case Job extends AnalyticsAggregateType("Job")
  case Application extends AnalyticsAggregateType("Application")
  case Search extends AnalyticsAggregateType("Search")
}

enum AnalyticsApplicationStatus(val wire: String) {
  case Accepted extends AnalyticsApplicationStatus("Accepted")
  case Declined extends AnalyticsApplicationStatus("Declined")
  case Interview extends AnalyticsApplicationStatus("Interview")
  case Hired extends AnalyticsApplicationStatus("Hired")
  case Rejected extends AnalyticsApplicationStatus("Rejected")
}

final case class PartitionOffsetRange(topic: String, partition: Int, startOffset: Long, endOffsetExclusive: Long)

object PartitionOffsetRange {
  def validate(range: PartitionOffsetRange): ValidatedNec[String, PartitionOffsetRange] = {
    val errors = Vector(
      Option.when(range.topic == null || range.topic.trim.isEmpty)("topic must be non-empty"),
      Option.when(range.partition < 0)("partition must be non-negative"),
      Option.when(range.startOffset < 0)("start offset must be non-negative"),
      Option.when(range.endOffsetExclusive < range.startOffset)("end offset must not precede start offset")
    ).flatten
    NonEmptyChain.fromSeq(errors) match {
      case Some(problems) => Validated.Invalid(problems)
      case None           => Validated.Valid(range)
    }
  }
}

final case class AnalyticsRunManifest private (runId: RunId, offsetRanges: Vector[PartitionOffsetRange])

object AnalyticsRunManifest {
  def validated(rawRunId: String, ranges: Vector[PartitionOffsetRange]): ValidatedNec[String, AnalyticsRunManifest] = {
    val rangeErrors = ranges.traverse(PartitionOffsetRange.validate)
    val nonEmpty =
      if (ranges.nonEmpty) ().validNec[String]
      else "at least one offset range is required".invalidNec[Unit]
    val unique =
      if (ranges.map(range => (range.topic, range.partition)).distinct.size == ranges.size) ().validNec[String]
      else "each topic partition may occur only once".invalidNec[Unit]
    val oneTopic =
      if (ranges.map(_.topic).distinct.size <= 1) ().validNec[String]
      else "a Kafka batch manifest must contain exactly one topic".invalidNec[Unit]
    (RunId.from(rawRunId), rangeErrors, nonEmpty, unique, oneTopic).mapN { (runId, validRanges, _, _, _) =>
      AnalyticsRunManifest(runId, validRanges)
    }
  }

  def validate(manifest: AnalyticsRunManifest): ValidatedNec[String, AnalyticsRunManifest] =
    validated(manifest.runId.value, manifest.offsetRanges)
}
