package com.example.graphQL.cats.service.search

import java.time.Instant
import munit.FunSuite

/** GDF-01: the pure decision of which filter to relax when a filtered job search finds nothing. */
final class ZeroResultRecoveryPlanSpec extends FunSuite {
  private val bound = Instant.parse("2026-09-16T10:15:30Z")

  test("GDF-01.1 a search with a createdAfter bound and two skills plans the drop first, then any-of skills") {
    val filter = JobSearchFilter(None, Set("Scala", "Kafka"), Some(bound))

    assertEquals(
      ZeroResultRecovery.plan(filter),
      List(
        RelaxationAttempt(filter.copy(createdAfter = None), RelaxedFilter.CreatedAfterDropped(bound)),
        RelaxationAttempt(
          filter.copy(skillMode = SkillMatchMode.AnyOf),
          RelaxedFilter.SkillsAllOfToAnyOf(Set("Scala", "Kafka"))
        )
      )
    )
  }

  test("GDF-01.1 each attempt relaxes exactly one filter and keeps every other filter as the caller sent it") {
    val filter = JobSearchFilter(Some("Kyiv"), Set("Scala", "Kafka"), Some(bound))

    val attempts = ZeroResultRecovery.plan(filter).map(_.filter)

    assertEquals(attempts.map(_.city), List(Some("Kyiv"), Some("Kyiv")))
    assertEquals(attempts.map(_.createdAfter), List(None, Some(bound)))
    assertEquals(attempts.map(_.skillMode), List(SkillMatchMode.AllOf, SkillMatchMode.AnyOf))
  }

  test("GDF-01.3 the plan never holds more than two attempts") {
    assert(ZeroResultRecovery.plan(JobSearchFilter(Some("Kyiv"), Set("Scala", "Kafka"), Some(bound))).size <= 2)
  }

  test("GDF-01.2 an unfiltered search has nothing to relax") {
    assertEquals(ZeroResultRecovery.plan(JobSearchFilter(None, Set.empty, None)), Nil)
  }

  test("GDF-01.2 the city is never relaxed: a city-only search has nothing to relax") {
    assertEquals(ZeroResultRecovery.plan(JobSearchFilter(Some("Kyiv"), Set.empty, None)), Nil)
  }

  test("GDF-01.2 one skill reads the same all-of and any-of, so only a createdAfter bound is relaxed") {
    val filter = JobSearchFilter(None, Set("Scala"), Some(bound))

    assertEquals(
      ZeroResultRecovery.plan(filter).map(_.relaxed),
      List(RelaxedFilter.CreatedAfterDropped(bound))
    )
    assertEquals(ZeroResultRecovery.plan(JobSearchFilter(None, Set("Scala"), None)), Nil)
  }

  test("GDF-01.2 a skills-only search with several skills plans the any-of widening alone") {
    assertEquals(
      ZeroResultRecovery.plan(JobSearchFilter(None, Set("Scala", "Kafka"), None)).map(_.relaxed),
      List(RelaxedFilter.SkillsAllOfToAnyOf(Set("Scala", "Kafka")))
    )
  }

  test("GDF-01.2 a filter that is already any-of is not widened again") {
    assertEquals(
      ZeroResultRecovery.plan(JobSearchFilter(None, Set("Scala", "Kafka"), None, SkillMatchMode.AnyOf)),
      Nil
    )
  }

  test("GDF-01.3 skills that differ only by case or whitespace are one skill: no duplicate-cost widening attempt") {
    assertEquals(ZeroResultRecovery.plan(JobSearchFilter(None, Set("Scala", "scala", " SCALA "), None)), Nil)
    assertEquals(
      ZeroResultRecovery.plan(JobSearchFilter(None, Set("Scala", "scala"), Some(bound))).map(_.relaxed),
      List(RelaxedFilter.CreatedAfterDropped(bound))
    )
  }
}
