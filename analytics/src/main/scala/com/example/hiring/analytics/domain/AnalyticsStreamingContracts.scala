package com.example.hiring.analytics.domain

import cats.data.ValidatedNec
import cats.syntax.all.*

import java.time.{Instant, LocalDate, ZoneOffset}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.*

final case class StreamingBatchId private (value: Long)
object StreamingBatchId {
  def from(value: Long): Either[String, StreamingBatchId] =
    Either.cond(value >= 0L, new StreamingBatchId(value), "streaming batch id must be non-negative")
}

final case class StreamingLineage private (value: String)
object StreamingLineage {
  def from(value: String): Either[String, StreamingLineage] =
    Either.cond(value.trim.nonEmpty, new StreamingLineage(value), "streaming lineage must be non-empty")
}

final case class StreamingBatchIdentity(lineage: StreamingLineage, batchId: StreamingBatchId)

/** Identity covered by the one-time Phase 6 activation authorization. */
final case class StreamingActivationIdentity(
    streamId: String,
    sourceIdentity: String,
    lakehouseId: String,
    contractFingerprint: String,
    settingsFingerprint: String
) {
  def canonical: String =
    Vector(streamId, sourceIdentity, lakehouseId, contractFingerprint, settingsFingerprint).mkString("\n")
}

/** Immutable evidence record. Runtime code only reads this record and never provisions or refreshes it. */
final case class StreamingActivationAuthorization(
    identity: StreamingActivationIdentity,
    evidenceReferences: Vector[String],
    independentReviewerReferences: Vector[String],
    evidenceDigest: String
)

object StreamingActivationAuthorization {
  def fromEvidence(
      identity: StreamingActivationIdentity,
      evidenceReferences: Vector[String],
      independentReviewerReferences: Vector[String]
  ): Either[String, StreamingActivationAuthorization] = {
    val canonicalEvidence = (identity.canonical +: (evidenceReferences ++ independentReviewerReferences).sorted)
      .mkString("\n")
    validate(
      StreamingActivationAuthorization(
        identity,
        evidenceReferences,
        independentReviewerReferences,
        AnalyticsDigest.sha256Hex(canonicalEvidence.getBytes(java.nio.charset.StandardCharsets.UTF_8))
      ),
      identity
    )
  }

  def validate(
      authorization: StreamingActivationAuthorization,
      expected: StreamingActivationIdentity
  ): Either[String, StreamingActivationAuthorization] = {
    val references = authorization.evidenceReferences ++ authorization.independentReviewerReferences
    val canonicalEvidence = (authorization.identity.canonical +: references.sorted).mkString("\n")
    val digest = AnalyticsDigest.sha256Hex(canonicalEvidence.getBytes(java.nio.charset.StandardCharsets.UTF_8))
    for {
      _ <- Either.cond(authorization.identity == expected, (), "activation authorization identity does not match")
      _ <- Either.cond(authorization.evidenceReferences.nonEmpty, (), "activation evidence references are required")
      _ <- Either.cond(
        authorization.independentReviewerReferences.distinct.size >= 2,
        (),
        "independent activation reviewer references are required"
      )
      _ <- Either.cond(references.forall(_.trim.nonEmpty), (), "activation evidence references must be non-empty")
      _ <- Either.cond(authorization.evidenceDigest == digest, (), "activation evidence digest does not match")
    } yield authorization
  }
}

final case class StreamingPartitionSummary private (
    topic: AnalyticsTopic,
    partition: AnalyticsPartition,
    minimumDeliveredOffset: AnalyticsOffset,
    maximumDeliveredOffset: AnalyticsOffset,
    deliveredRecordCount: Long
)
object StreamingPartitionSummary {
  def from(
      topic: String,
      partition: Int,
      minimumDeliveredOffset: Long,
      maximumDeliveredOffset: Long,
      deliveredRecordCount: Long
  ): ValidatedNec[String, StreamingPartitionSummary] = {
    val validTopic = AnalyticsTopic.from(topic).toValidatedNec
    val validPartition = AnalyticsPartition.from(partition).toValidatedNec
    val minimum = AnalyticsOffset.from(minimumDeliveredOffset).toValidatedNec
    val maximum = AnalyticsOffset.from(maximumDeliveredOffset).toValidatedNec
    val count = Either
      .cond(deliveredRecordCount > 0L, deliveredRecordCount, "delivered record count must be positive")
      .toValidatedNec
    val ordering = ((minimum.toEither, maximum.toEither) match {
      case (Right(start), Right(end)) =>
        Either.cond(end >= start, (), "maximum delivered offset must not precede minimum")
      case _ => Right(())
    }).toValidatedNec
    (validTopic, validPartition, minimum, maximum, count, ordering)
      .mapN((validTopic, validPartition, minimumOffset, maximumOffset, validCount, _) =>
        new StreamingPartitionSummary(validTopic, validPartition, minimumOffset, maximumOffset, validCount)
      )
  }
}

enum EventTimeAdmission {
  case Admitted(effectiveEventTime: Instant)
  case LateClosedDay(day: LocalDate)
  case TooFarInFuture
}

object AnalyticsEventTimePolicy {
  val AllowedFutureSkew: FiniteDuration = 5.minutes
  val WatermarkLag: FiniteDuration = 24.hours
  val LateFactRetentionDays: Int = 30

  def admit(
      eventTime: Instant,
      observedAt: Instant,
      previousWatermark: Option[Instant]
  ): EventTimeAdmission = {
    if (eventTime.isAfter(observedAt.plusNanos(AllowedFutureSkew.toNanos))) EventTimeAdmission.TooFarInFuture
    else {
      val day = eventTime.atZone(ZoneOffset.UTC).toLocalDate
      val dayEnd = day.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant
      if (previousWatermark.exists(watermark => !dayEnd.isAfter(watermark))) EventTimeAdmission.LateClosedDay(day)
      else EventTimeAdmission.Admitted(if (eventTime.isAfter(observedAt)) observedAt else eventTime)
    }
  }

  /** Returns no progress for an empty eligible batch; callers persist the result only after publication succeeds. */
  def candidateWatermark(
      previousWatermark: Option[Instant],
      admittedUnambiguousEventTimes: Iterable[Instant],
      observedAt: Instant
  ): Option[Instant] =
    admittedUnambiguousEventTimes.iterator
      .map(time => if (time.isAfter(observedAt)) observedAt else time)
      .reduceOption((left, right) => if (left.isAfter(right)) left else right)
      .map(_.minusNanos(WatermarkLag.toNanos))
      .map(candidate => previousWatermark.fold(candidate)(prior => if (candidate.isAfter(prior)) candidate else prior))
}
