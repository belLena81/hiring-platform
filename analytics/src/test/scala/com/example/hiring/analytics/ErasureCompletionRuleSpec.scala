package com.example.hiring.analytics

import com.example.hiring.analytics.domain.{DeltaErasureHorizons, ErasureCompletionRule}

import java.time.Instant
import scala.concurrent.duration.*

final class ErasureCompletionRuleSpec extends munit.FunSuite {
  test("AEC-01.1 the Delta deadline is the later of the data and rounded log deadlines") {
    assertEquals(
      ErasureCompletionRule.deltaDeadline(Instant.parse("2026-03-02T10:00:00Z"), 7.days, 14.days),
      Instant.parse("2026-03-17T00:00:00Z")
    )
  }

  test("AEC-01.2 a purge late on a UTC day waits for the next day plus the log retention") {
    assertEquals(
      ErasureCompletionRule.deltaDeadline(Instant.parse("2026-03-02T23:59:59Z"), 1.second, 1.second),
      Instant.parse("2026-03-03T00:00:01Z")
    )
  }

  test("AEC-01.3 a purge exactly at UTC midnight still waits a full day") {
    assertEquals(
      ErasureCompletionRule.deltaDeadline(Instant.parse("2026-03-03T00:00:00Z"), 1.second, 1.second),
      Instant.parse("2026-03-04T00:00:01Z")
    )
  }

  test("AEC-01.1 the data deadline dominates when the vacuum safety is longer than the log window") {
    assertEquals(
      ErasureCompletionRule.deltaDeadline(Instant.parse("2026-03-02T10:00:00Z"), 30.days, 1.day),
      Instant.parse("2026-04-01T10:00:00Z")
    )
  }

  test("AC-04.4 the data horizon runs from an OPTIMIZE that follows the purge, never from an earlier one") {
    val horizons = DeltaErasureHorizons(vacuumSafety = 30.days, logRetention = 1.day)
    val purgedAt = Instant.parse("2026-03-02T10:00:00Z")
    assertEquals(
      ErasureCompletionRule.deltaDeadline(purgedAt, None, horizons),
      Instant.parse("2026-04-01T10:00:00Z")
    )
    assertEquals(
      ErasureCompletionRule.deltaDeadline(purgedAt, Some(Instant.parse("2026-03-05T06:00:00Z")), horizons),
      Instant.parse("2026-04-04T06:00:00Z")
    )
    // An OPTIMIZE before the purge cannot shorten the horizon either.
    assertEquals(
      ErasureCompletionRule.deltaDeadline(purgedAt, Some(Instant.parse("2026-02-20T00:00:00Z")), horizons),
      Instant.parse("2026-04-01T10:00:00Z")
    )
  }

  test("AEC-02.1 the worst case is the slowest of Kafka, rolled log and vacuum horizons") {
    val horizons = DeltaErasureHorizons(vacuumSafety = 7.days, logRetention = 14.days)
    assertEquals(ErasureCompletionRule.worstCaseCompletion(horizons, 14.days), 15.days)
    assertEquals(ErasureCompletionRule.worstCaseCompletion(horizons, 40.days), 40.days)
    assertEquals(
      ErasureCompletionRule.worstCaseCompletion(DeltaErasureHorizons(30.days, 1.day), 1.day),
      30.days
    )
  }
}
