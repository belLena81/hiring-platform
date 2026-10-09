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

final case class StreamingPartitionEndOffset private (
    topic: AnalyticsTopic,
    partition: AnalyticsPartition,
    offset: AnalyticsOffset
)
object StreamingPartitionEndOffset {
  def from(topic: String, partition: Int, offset: Long): ValidatedNec[String, StreamingPartitionEndOffset] =
    (
      AnalyticsTopic.from(topic).toValidatedNec,
      AnalyticsPartition.from(partition).toValidatedNec,
      AnalyticsOffset.from(offset).toValidatedNec
    ).mapN(StreamingPartitionEndOffset.apply)
}

/** Runtime identity covered by immutable analytics activation evidence. */
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

/** Immutable, time-bounded grant record. Runtime code only reads it and never provisions or renews it. */
final case class StreamingActivationAuthorization(
    identity: StreamingActivationIdentity,
    grantId: String,
    validFrom: Instant,
    expiresAt: Instant,
    evidenceReferences: Vector[String],
    independentReviewerReferences: Vector[String],
    evidenceDigest: String
)

object StreamingActivationAuthorization {
  def fromEvidence(
      identity: StreamingActivationIdentity,
      grantId: String,
      validFrom: Instant,
      expiresAt: Instant,
      evidenceReferences: Vector[String],
      independentReviewerReferences: Vector[String]
  ): Either[String, StreamingActivationAuthorization] = {
    validate(
      StreamingActivationAuthorization(
        identity,
        grantId,
        validFrom,
        expiresAt,
        evidenceReferences,
        independentReviewerReferences,
        evidenceDigest(identity, grantId, validFrom, expiresAt, evidenceReferences ++ independentReviewerReferences)
      ),
      identity,
      grantId,
      validFrom
    )
  }

  private def evidenceDigest(
      identity: StreamingActivationIdentity,
      grantId: String,
      validFrom: Instant,
      expiresAt: Instant,
      references: Vector[String]
  ): String =
    AnalyticsDigest.sha256Hex(
      (Vector(identity.canonical, grantId, validFrom.toString, expiresAt.toString) ++ references.sorted).mkString("\n")
    )

  def validate(
      authorization: StreamingActivationAuthorization,
      expected: StreamingActivationIdentity,
      expectedGrantId: String,
      now: Instant
  ): Either[String, StreamingActivationAuthorization] = {
    val references = authorization.evidenceReferences ++ authorization.independentReviewerReferences
    val digest = evidenceDigest(
      authorization.identity,
      authorization.grantId,
      authorization.validFrom,
      authorization.expiresAt,
      references
    )
    for {
      _ <- Either.cond(authorization.identity == expected, (), "activation authorization identity does not match")
      _ <- Either.cond(
        authorization.grantId == expectedGrantId && expectedGrantId.trim.nonEmpty,
        (),
        "activation grant does not match"
      )
      _ <- Either.cond(
        authorization.validFrom.isBefore(authorization.expiresAt),
        (),
        "activation grant interval is invalid"
      )
      _ <- Either.cond(
        !now.isBefore(authorization.validFrom) && now.isBefore(authorization.expiresAt),
        (),
        "activation grant is outside its validity interval"
      )
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
      .maxOption
      .map(_.minusNanos(WatermarkLag.toNanos))
      .map(candidate => previousWatermark.fold(candidate)(Ordering[Instant].max(candidate, _)))
}
