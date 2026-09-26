package com.example.hiring.analytics

import cats.data.{NonEmptyChain, Validated, ValidatedNec}
import cats.syntax.all.*
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.Not
import io.github.iltotore.iron.constraint.numeric.Interval
import io.github.iltotore.iron.constraint.string.Blank
import io.github.iltotore.iron.constraint.string.Match

/** The analytics retention policy is intentionally separate from Kafka retention. */
object AnalyticsRetention {
  val BronzeDays: Int = 7
  val QuarantineDays: Int = 7
  val SilverDays: Int = 30
  val GoldDays: Int = 30
  val PublishedSnapshotDays: Int = 30
  val DeletionMarkerDays: Int = 31
  val DeltaVacuumSafetyDays: Int = 7
  val DeltaLogRetentionDays: Int = 30
  val MinimumContributors: Long = 10L
}

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

opaque type RunId = String

object RunId {
  def from(value: String): ValidatedNec[String, RunId] =
    Option(value)
      .filter(_.trim.nonEmpty)
      .toRight("run id must be non-empty")
      .flatMap(_.refineEither[Not[Blank]].leftMap(_ => "run id must be non-empty"))
      .toValidatedNec

  extension (value: RunId) def value: String = value
}

opaque type SubjectToken = String :| Match["[A-Za-z0-9-]{1,40}_[A-Za-z0-9_-]{43}"]

object SubjectToken {
  def fromHmac(value: String): Either[String, SubjectToken] =
    Option(value)
      .toRight("subject token has an invalid format")
      .flatMap(
        _.refineEither[Match["[A-Za-z0-9-]{1,40}_[A-Za-z0-9_-]{43}"]]
          .leftMap(_ => "subject token has an invalid format")
      )

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
    val topic = Option(range.topic)
      .filter(_.trim.nonEmpty)
      .toRight("topic must be non-empty")
      .flatMap(_.refineEither[Not[Blank]].leftMap(_ => "topic must be non-empty"))
      .toValidatedNec
    val partition = range.partition
      .refineEither[Interval.Closed[0, 2147483647]]
      .leftMap(_ => "partition must be non-negative")
      .toValidatedNec
    val start = range.startOffset
      .refineEither[Interval.Closed[0L, 9223372036854775807L]]
      .leftMap(_ => "start offset must be non-negative")
      .toValidatedNec
    val end = range.endOffsetExclusive
      .refineEither[Interval.Closed[0L, 9223372036854775807L]]
      .leftMap(_ => "end offset must be non-negative")
      .toValidatedNec
    (topic, partition, start, end)
      .mapN { (_, _, validStart, validEnd) =>
        if (validEnd < validStart) "end offset must not precede start offset".invalidNec
        else range.validNec
      }
      .andThen(identity)
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
