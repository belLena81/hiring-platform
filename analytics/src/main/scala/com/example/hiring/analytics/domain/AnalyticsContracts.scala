package com.example.hiring.analytics.domain
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

opaque type RunId = String :| Not[Blank]

type AnalyticsPartition = Int :| Interval.Closed[0, 2147483647]
type AnalyticsOffset = Long :| Interval.Closed[0L, 9223372036854775807L]

object RunId {
  def from(value: String): ValidatedNec[String, RunId] =
    Option(value)
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

final case class PartitionOffsetRange private (
    topic: String,
    partition: AnalyticsPartition,
    startOffset: AnalyticsOffset,
    endOffsetExclusive: AnalyticsOffset
)

object PartitionOffsetRange {
  private[analytics] def partitionNumber(value: AnalyticsPartition): Int = value

  def from(
      topic: String,
      partition: Int,
      startOffset: Long,
      endOffsetExclusive: Long
  ): ValidatedNec[String, PartitionOffsetRange] = {
    val validTopic = Option(topic)
      .filter(_.trim.nonEmpty)
      .toRight("topic must be non-empty")
      .flatMap(_.refineEither[Not[Blank]].leftMap(_ => "topic must be non-empty"))
      .toValidatedNec
    val validPartition = partition
      .refineEither[Interval.Closed[0, 2147483647]]
      .leftMap(_ => "partition must be non-negative")
      .toValidatedNec
    val validStart = startOffset
      .refineEither[Interval.Closed[0L, 9223372036854775807L]]
      .leftMap(_ => "start offset must be non-negative")
      .toValidatedNec
    val validEnd = endOffsetExclusive
      .refineEither[Interval.Closed[0L, 9223372036854775807L]]
      .leftMap(_ => "end offset must be non-negative")
      .toValidatedNec
    (validTopic, validPartition, validStart, validEnd)
      .mapN { (topic, partition, start, end) =>
        if (end < start) "end offset must not precede start offset".invalidNec
        else new PartitionOffsetRange(topic, partition, start, end).validNec
      }
      .andThen(identity)
  }

  private[analytics] def unsafe(
      topic: String,
      partition: Int,
      startOffset: Long,
      endOffsetExclusive: Long
  ): PartitionOffsetRange =
    from(topic, partition, startOffset, endOffsetExclusive).toEither.fold(
      errors => throw new IllegalArgumentException(errors.toNonEmptyList.toList.mkString("; ")),
      identity
    )

  private[analytics] def refined(
      topic: String,
      partition: AnalyticsPartition,
      startOffset: AnalyticsOffset,
      endOffsetExclusive: AnalyticsOffset
  ): PartitionOffsetRange =
    new PartitionOffsetRange(topic, partition, startOffset, endOffsetExclusive)

  def validate(range: PartitionOffsetRange): ValidatedNec[String, PartitionOffsetRange] = {
    val topic = Option(range.topic)
      .filter(_.trim.nonEmpty)
      .toRight("topic must be non-empty")
      .flatMap(_.refineEither[Not[Blank]].leftMap(_ => "topic must be non-empty"))
      .toValidatedNec
    topic.andThen { _ =>
      if (range.endOffsetExclusive < range.startOffset) "end offset must not precede start offset".invalidNec
      else range.validNec
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
