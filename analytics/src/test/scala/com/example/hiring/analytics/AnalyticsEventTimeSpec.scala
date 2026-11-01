package com.example.hiring.analytics

import com.example.hiring.analytics.domain.*

import munit.FunSuite

import java.time.{Instant, LocalDate}
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
      observedAt,
      AnalyticsEventTimePolicy.DefaultAllowedLateness
    )

    assertEquals(
      AnalyticsEventTimePolicy.candidateWatermark(
        Some(prior),
        Nil,
        observedAt,
        AnalyticsEventTimePolicy.DefaultAllowedLateness
      ),
      None
    )
    assertEquals(candidate, Some(observedAt.minusSeconds(24 * 60 * 60)))
    assertEquals(
      candidate.flatMap(value =>
        AnalyticsEventTimePolicy.candidateWatermark(
          Some(value),
          List(observedAt.minusSeconds(1)),
          observedAt,
          AnalyticsEventTimePolicy.DefaultAllowedLateness
        )
      ),
      candidate
    )
  }

  test("DDR-04.1 the documented default allowed lateness is 24 hours within its 1 hour to 7 day bounds") {
    assertEquals(AnalyticsEventTimePolicy.DefaultAllowedLateness, 24.hours)
    assertEquals(AnalyticsEventTimePolicy.MinimumAllowedLateness, 1.hour)
    assertEquals(AnalyticsEventTimePolicy.MaximumAllowedLateness, 7.days)
  }

  test("DDR-04.1 the maximum allowed lateness stays below the late-fact retention") {
    assert(
      AnalyticsEventTimePolicy.MaximumAllowedLateness < AnalyticsEventTimePolicy.LateFactRetentionDays.days,
      "a closed-day fact must still be retained when it is first classified late"
    )
  }

  test("DDR-04.1 the candidate watermark trails the greatest admitted event time by the configured lateness") {
    val newest = observedAt.minusSeconds(600)
    Vector(1.hour, 24.hours, 72.hours, 7.days).foreach { lateness =>
      assertEquals(
        AnalyticsEventTimePolicy.candidateWatermark(None, List(newest), observedAt, lateness),
        Some(newest.minusNanos(lateness.toNanos)),
        lateness.toString
      )
    }
  }

  /** The bound for a UTC day is its end: a fact is late once `dayEnd <= newestAdmitted - allowedLateness`. */
  private def admissionAt(factTime: Instant, newest: Instant, lateness: FiniteDuration): EventTimeAdmission = {
    val watermark = AnalyticsEventTimePolicy.candidateWatermark(None, List(newest), newest, lateness)
    AnalyticsEventTimePolicy.admit(factTime, newest, watermark)
  }

  test("DDR-04.2 a fact inside the bound is admitted, at the bound it is a closed-day late fact, past it likewise") {
    val lateness = 48.hours
    // Watermark lands exactly on a UTC midnight: 2026-09-30T00:00:00Z.
    val newest = Instant.parse("2026-10-02T00:00:00Z")
    val watermark = newest.minusNanos(lateness.toNanos)
    assertEquals(watermark, Instant.parse("2026-09-30T00:00:00Z"))
    val inside = watermark // first instant of the open day [watermark, watermark + 1 day)
    val justInside = watermark.plusSeconds(86399L) // last second of the same open day
    val atBound = watermark.minusNanos(1) // last instant of the day that ends exactly at the watermark
    val pastBound = watermark.minusSeconds(5L * 86400L)

    assertEquals(admissionAt(inside, newest, lateness), EventTimeAdmission.Admitted(inside))
    assertEquals(admissionAt(justInside, newest, lateness), EventTimeAdmission.Admitted(justInside))
    assertEquals(
      admissionAt(atBound, newest, lateness),
      EventTimeAdmission.LateClosedDay(LocalDate.parse("2026-09-29"))
    )
    assertEquals(
      admissionAt(pastBound, newest, lateness),
      EventTimeAdmission.LateClosedDay(LocalDate.parse("2026-09-25"))
    )
    // The same fact is still on time under a longer configured lateness.
    assertEquals(admissionAt(atBound, newest, 72.hours), EventTimeAdmission.Admitted(atBound))
  }

  test("DDR-04.2 the effective tolerance is between one and two allowed-lateness periods depending on the UTC day") {
    val lateness = 24.hours
    val fact = Instant.parse("2026-09-29T23:59:59Z") // last second of its UTC day
    // Closes as soon as the newest admitted event passes day end plus the lateness (2026-10-01T00:00:00Z).
    assertEquals(
      admissionAt(fact, Instant.parse("2026-09-30T23:59:59Z"), lateness),
      EventTimeAdmission.Admitted(fact)
    )
    assertEquals(
      admissionAt(fact, Instant.parse("2026-10-01T00:00:00Z"), lateness),
      EventTimeAdmission.LateClosedDay(LocalDate.parse("2026-09-29"))
    )
  }

  test("activation authorization binds Phase 6 evidence and independent reviewer references to runtime identity") {
    val identity = StreamingActivationIdentity("stream-a", "source-digest", "lakehouse-digest", "contract", "settings")
    val validFrom = Instant.parse("2026-09-30T11:00:00Z")
    val expiresAt = Instant.parse("2026-09-30T13:00:00Z")
    val grantId = "grant-2026-09"
    val authorization = StreamingActivationAuthorization
      .fromEvidence(
        identity,
        grantId,
        validFrom,
        expiresAt,
        Vector("hal-audit", "retention-evidence"),
        Vector("security-review", "qa-review")
      )
      .toOption
      .get

    assertEquals(
      StreamingActivationAuthorization.validate(authorization, identity, grantId, validFrom),
      Right(authorization)
    )
    assert(
      StreamingActivationAuthorization
        .validate(authorization, identity.copy(settingsFingerprint = "changed"), grantId, validFrom)
        .isLeft
    )
    assert(
      StreamingActivationAuthorization
        .validate(authorization, identity, "another-grant", validFrom)
        .isLeft
    )
    assert(
      StreamingActivationAuthorization
        .validate(authorization, identity, grantId, validFrom.minusNanos(1))
        .isLeft
    )
    assert(
      StreamingActivationAuthorization
        .validate(authorization, identity, grantId, expiresAt)
        .isLeft
    )
    assert(
      StreamingActivationAuthorization
        .fromEvidence(identity, grantId, expiresAt, expiresAt, Vector("retention-evidence"), Vector("one-review"))
        .isLeft
    )
    assert(
      StreamingActivationAuthorization
        .fromEvidence(identity, grantId, validFrom, expiresAt, Vector("retention-evidence"), Vector("one-review"))
        .isLeft
    )
    assert(
      StreamingActivationAuthorization
        .fromEvidence(
          identity,
          grantId,
          validFrom,
          expiresAt,
          Vector("retention-evidence"),
          Vector("same-review", "same-review")
        )
        .isLeft
    )

    val changedExpiry = authorization.copy(expiresAt = expiresAt.plusSeconds(1))
    assert(
      StreamingActivationAuthorization.validate(changedExpiry, identity, grantId, validFrom).isLeft,
      "grant timestamps must be covered by the evidence digest"
    )
  }

}
