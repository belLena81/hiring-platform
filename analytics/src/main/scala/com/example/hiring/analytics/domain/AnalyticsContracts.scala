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
  val MinimumContributors: Long = 10L
}

opaque type RunId = String :| Not[Blank]
opaque type AccountSubjectId = java.util.UUID
opaque type RangeFingerprint = String :| Match["[0-9a-f]{64}"]

type AnalyticsPartition = Int :| Interval.Closed[0, 2147483647]
type AnalyticsOffset = Long :| Interval.Closed[0L, 9223372036854775807L]

object RunId {
  def from(value: String): ValidatedNec[String, RunId] =
    value
      .refineEither[Not[Blank]]
      .leftMap(_ => "run id must be non-empty")
      .toValidatedNec

  extension (value: RunId) def value: String = value
}

object AccountSubjectId {
  def from(value: String): Either[String, AccountSubjectId] =
    Either
      .catchNonFatal(java.util.UUID.fromString(value))
      .leftMap(_ => "account subject id must be a UUID")
      .flatMap(uuid => Either.cond(uuid.toString == value, uuid, "account subject id must use canonical UUID form"))

  extension (value: AccountSubjectId) def value: String = value.toString
}

object RangeFingerprint {
  def from(value: String): Either[String, RangeFingerprint] =
    value.refineEither[Match["[0-9a-f]{64}"]].leftMap(_ => "range fingerprint must be a SHA-256 hex digest")

  extension (value: RangeFingerprint) def value: String = value
}

final class SubjectToken private (private val tokenValue: String) {
  override def equals(other: Any): Boolean = other match {
    case token: SubjectToken => tokenValue == token.tokenValue
    case _                   => false
  }
  override def hashCode(): Int = tokenValue.hashCode
}

object SubjectToken {
  def fromHmac(value: String): Either[String, SubjectToken] =
    value
      .refineEither[Match["[A-Za-z0-9-]{1,40}_[A-Za-z0-9_-]{43}"]]
      .leftMap(_ => "subject token has an invalid format")
      .map(refined => new SubjectToken(refined.toString))

  /** Internal construction for a token produced by HmacSHA256 with a validated key ID. */
  private[domain] def fromDigest(keyId: String, digest: Array[Byte]): SubjectToken = {
    val encoded = java.util.Base64.getUrlEncoder.withoutPadding().encodeToString(digest)
    new SubjectToken(s"${keyId}_$encoded")
  }

  extension (token: SubjectToken) def value: String = token.tokenValue
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

final class PartitionOffsetRange private (
    val topic: String,
    val partition: AnalyticsPartition,
    val startOffset: AnalyticsOffset,
    val endOffsetExclusive: AnalyticsOffset
) {
  override def equals(other: Any): Boolean = other match {
    case that: PartitionOffsetRange =>
      topic == that.topic && partition == that.partition && startOffset == that.startOffset &&
      endOffsetExclusive == that.endOffsetExclusive
    case _ => false
  }

  override def hashCode(): Int = (topic, partition, startOffset, endOffsetExclusive).hashCode()
}

object PartitionOffsetRange {
  private[analytics] def partitionNumber(value: AnalyticsPartition): Int = value

  def from(
      topic: String,
      partition: Int,
      startOffset: Long,
      endOffsetExclusive: Long
  ): ValidatedNec[String, PartitionOffsetRange] =
    fromValidated(topic.validNec, partition.validNec, startOffset.validNec, endOffsetExclusive.validNec)

  def fromValidated(
      topic: ValidatedNec[String, String],
      partition: ValidatedNec[String, Int],
      startOffset: ValidatedNec[String, Long],
      endOffsetExclusive: ValidatedNec[String, Long]
  ): ValidatedNec[String, PartitionOffsetRange] = {
    val validTopic = topic.andThen(_.refineEither[Not[Blank]].leftMap(_ => "topic must be non-empty").toValidatedNec)
    val validPartition = partition.andThen(
      _.refineEither[Interval.Closed[0, 2147483647]].leftMap(_ => "partition must be non-negative").toValidatedNec
    )
    val validStart = startOffset.andThen(
      _.refineEither[Interval.Closed[0L, 9223372036854775807L]]
        .leftMap(_ => "start offset must be non-negative")
        .toValidatedNec
    )
    val validEnd = endOffsetExclusive.andThen(
      _.refineEither[Interval.Closed[0L, 9223372036854775807L]]
        .leftMap(_ => "end offset must be non-negative")
        .toValidatedNec
    )
    val validOrdering = (validStart.toEither, validEnd.toEither) match {
      case (Right(start), Right(end)) => Validated.condNec(end >= start, (), "end offset must not precede start offset")
      case _                          => ().validNec[String]
    }
    (validTopic, validPartition, validStart, validEnd, validOrdering)
      .mapN((topic, partition, start, end, _) => new PartitionOffsetRange(topic, partition, start, end))
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

}

final class AnalyticsRunManifest private (val runId: RunId, val offsetRanges: Vector[PartitionOffsetRange]) {
  override def equals(other: Any): Boolean = other match {
    case that: AnalyticsRunManifest => runId == that.runId && offsetRanges == that.offsetRanges
    case _                          => false
  }

  override def hashCode(): Int = (runId, offsetRanges).hashCode()
}

object AnalyticsRunManifest {
  private def validateRanges(
      ranges: Vector[PartitionOffsetRange]
  ): ValidatedNec[String, Vector[PartitionOffsetRange]] = {
    val nonEmpty =
      if (ranges.nonEmpty) ().validNec[String]
      else "at least one offset range is required".invalidNec[Unit]
    val unique =
      if (ranges.map(range => (range.topic, range.partition)).distinct.size == ranges.size) ().validNec[String]
      else "each topic partition may occur only once".invalidNec[Unit]
    val oneTopic =
      if (ranges.map(_.topic).distinct.size <= 1) ().validNec[String]
      else "a Kafka batch manifest must contain exactly one topic".invalidNec[Unit]
    (ranges.validNec[String], nonEmpty, unique, oneTopic).mapN((validRanges, _, _, _) => validRanges)
  }

  def from(runId: RunId, ranges: Vector[PartitionOffsetRange]): ValidatedNec[String, AnalyticsRunManifest] =
    fromValidated(runId.validNec, ranges.validNec)

  def fromValidated(
      runId: ValidatedNec[String, RunId],
      ranges: ValidatedNec[String, Vector[PartitionOffsetRange]]
  ): ValidatedNec[String, AnalyticsRunManifest] =
    (runId, ranges.andThen(validateRanges)).mapN((id, validRanges) => new AnalyticsRunManifest(id, validRanges))

  def validated(rawRunId: String, ranges: Vector[PartitionOffsetRange]): ValidatedNec[String, AnalyticsRunManifest] =
    fromValidated(RunId.from(rawRunId), ranges.validNec)
}
