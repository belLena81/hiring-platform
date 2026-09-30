package com.example.hiring.analytics

import com.example.hiring.analytics.domain.*

import munit.FunSuite

import java.time.Instant
import scala.concurrent.duration.*

class AnalyticsEventTimeSpec extends FunSuite {
  private val observedAt = Instant.parse("2026-09-30T12:00:00Z")

  test("UTC daily window remains open until its exclusive end is at or before the prior watermark") {
    val eventTime = Instant.parse("2026-09-29T08:30:00Z")
    val dayEnd = Instant.parse("2026-09-30T00:00:00Z")

    assertEquals(
      AnalyticsEventTimePolicy.admit(eventTime, observedAt, Some(dayEnd.minusNanos(1))),
      EventTimeAdmission.Admitted(eventTime)
    )
    assertEquals(
      AnalyticsEventTimePolicy.admit(eventTime, observedAt, Some(dayEnd)),
      EventTimeAdmission.LateClosedDay(java.time.LocalDate.parse("2026-09-29"))
    )
  }

  test("events beyond allowed future skew are rejected and permitted future times are capped") {
    val atSkewBoundary = observedAt.plusSeconds(300)
    val beyondSkew = atSkewBoundary.plusNanos(1)

    assertEquals(
      AnalyticsEventTimePolicy.admit(atSkewBoundary, observedAt, None),
      EventTimeAdmission.Admitted(observedAt)
    )
    assertEquals(
      AnalyticsEventTimePolicy.admit(beyondSkew, observedAt, None),
      EventTimeAdmission.TooFarInFuture
    )
  }

  test("candidate watermark does not move for empty input and advances monotonically from capped event time") {
    val prior = Instant.parse("2026-09-29T00:00:00Z")
    val candidate = AnalyticsEventTimePolicy.candidateWatermark(
      Some(prior),
      List(observedAt.minusSeconds(600), observedAt.plusSeconds(60)),
      observedAt
    )

    assertEquals(AnalyticsEventTimePolicy.candidateWatermark(Some(prior), Nil, observedAt), None)
    assertEquals(candidate, Some(observedAt.minusSeconds(24 * 60 * 60)))
    assertEquals(
      candidate.flatMap(value =>
        AnalyticsEventTimePolicy.candidateWatermark(Some(value), List(observedAt.minusSeconds(1)), observedAt)
      ),
      candidate
    )
  }

  test("activation authorization binds Phase 6 evidence and independent reviewer references to runtime identity") {
    val identity = StreamingActivationIdentity("stream-a", "source-digest", "lakehouse-digest", "contract", "settings")
    val authorization = StreamingActivationAuthorization
      .fromEvidence(identity, Vector("hal-audit", "retention-proof"), Vector("security-review", "qa-review"))
      .toOption
      .get

    assertEquals(StreamingActivationAuthorization.validate(authorization, identity), Right(authorization))
    assert(
      StreamingActivationAuthorization.validate(authorization, identity.copy(settingsFingerprint = "changed")).isLeft
    )
    assert(
      StreamingActivationAuthorization
        .fromEvidence(identity, Vector("retention-proof"), Vector("one-review"))
        .isLeft
    )
    assert(
      StreamingActivationAuthorization
        .fromEvidence(identity, Vector("retention-proof"), Vector("same-review", "same-review"))
        .isLeft
    )
  }

  test("only a completed published batch may commit its prepared candidate watermark") {
    val lineage = StreamingLineage.from("lineage-a").toOption.get
    val batchId = StreamingBatchId.from(3L).toOption.get
    val fingerprint = RangeFingerprint.from("a" * 64).toOption.get
    val candidate = observedAt.minusSeconds(24 * 60 * 60)
    val preparation = StreamingBatchPreparation(
      StreamingBatchIdentity(lineage, batchId),
      observedAt,
      None,
      Some(candidate),
      fingerprint,
      Vector.empty
    )
    val published = StreamingBatchProgress(
      preparation,
      StreamingBatchOutcome.Published,
      Some(candidate),
      Some(observedAt)
    )
    val qualityBlocked = published.copy(outcome = StreamingBatchOutcome.QualityBlocked, candidateWatermark = None)

    assert(StreamingBatchProgress.validate(published).isRight)
    assert(StreamingBatchProgress.validate(qualityBlocked).isRight)
    assert(StreamingBatchProgress.validate(qualityBlocked.copy(candidateWatermark = Some(candidate))).isLeft)
    assert(StreamingBatchProgress.validate(published.copy(completedAt = None)).isLeft)
  }
}
