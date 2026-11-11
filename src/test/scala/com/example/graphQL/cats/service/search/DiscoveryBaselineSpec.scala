package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.service.search.DiscoveryBaseline.*

import java.time.Instant
import java.util.Locale

final class DiscoveryBaselineSpec extends munit.FunSuite {
  private def search(count: Int, rank: Option[Int]) = SearchObservation.validated(count, rank).toOption.get
  private def followUp(applied: Instant, changed: Option[Instant]) =
    FollowUpObservation.validated(applied, changed).toOption.get
  private def validConfig(config: DiscoveryBaselineCorpus.Config) =
    DiscoveryBaselineCorpus.Config.validated(config).toOption.get
  private val t0 = Instant.parse("2026-01-01T00:00:00Z")

  test("GDF-07 measures zero-result rate, first-click rank and clicked-search share on a small known set") {
    val report = measure(
      List(search(0, None), search(5, Some(1)), search(5, Some(3)), search(4, None)),
      Nil
    )
    assertEquals(report.zeroResultRate, Some(0.25))
    assertEquals(report.clickedSearchShare, Some(0.5))
    assertEquals(report.firstClickRank, Some(RankSummary(2, 2.0, 2.0)))
  }

  test("GDF-07 measures time from application to first status change and counts unreviewed applications") {
    val report = measure(
      Nil,
      List(
        followUp(t0, Some(t0.plusSeconds(60))),
        followUp(t0, Some(t0.plusSeconds(180))),
        followUp(t0, None)
      )
    )
    assertEquals(report.applications, 3)
    assertEquals(report.unreviewedApplications, 1)
    assertEquals(report.applicationToFirstStatusChange, Some(FollowUpSummary(2, 120.0, 120.0)))
  }

  test("GDF-07 reports absence rather than zero when a population is empty") {
    val report = measure(Nil, Nil)
    assertEquals(report.zeroResultRate, None)
    assertEquals(report.clickedSearchShare, None)
    assertEquals(report.firstClickRank, None)
    assertEquals(report.applicationToFirstStatusChange, None)
  }

  test("GDF-07 rejects a click on a zero-result search and a rank beyond the results") {
    assert(SearchObservation.validated(0, Some(1)).isInvalid)
    assert(SearchObservation.validated(3, Some(4)).isInvalid)
    assert(SearchObservation.validated(3, Some(0)).isInvalid)
    assert(SearchObservation.validated(-1, None).isInvalid)
  }

  test("GDF-07 accepts a click on the last result") {
    assert(SearchObservation.validated(3, Some(3)).isValid)
    assert(SearchObservation.validated(1, Some(1)).isValid)
  }

  test("GDF-07 rejects a status change dated before the application") {
    val result = FollowUpObservation.validated(t0, Some(t0.minusSeconds(1)))
    assertEquals(result.fold(_.toList, _ => Nil), List(FollowUpObservation.Invalid.StatusChangeBeforeApplication))
    assert(FollowUpObservation.validated(t0, Some(t0)).isValid)
    assert(FollowUpObservation.validated(t0, None).isValid)
  }

  test("GDF-07 rejects a result capacity or review window that could overflow generation") {
    val local = DiscoveryBaselineCorpus.Config.Local
    val tooBig = local.copy(maxResults = Int.MaxValue, maxReviewHours = Int.MaxValue)
    assertEquals(
      DiscoveryBaselineCorpus.Config.validated(tooBig).fold(_.toList.toSet, _ => Set.empty),
      Set(
        DiscoveryBaselineCorpus.Config.Invalid.ResultCapacityTooLarge,
        DiscoveryBaselineCorpus.Config.Invalid.ReviewWindowTooLong
      )
    )
    assert(DiscoveryBaselineCorpus.Config.validated(local.copy(maxResults = 0)).isInvalid)
    assert(
      DiscoveryBaselineCorpus.Config
        .validated(local.copy(maxReviewHours = DiscoveryBaselineCorpus.Config.MaxReviewHours))
        .isValid
    )
  }

  test("GDF-07 median and mean of skewed unsorted first-click ranks, odd count") {
    val report = measure(List(search(10, Some(7)), search(10, Some(1)), search(10, Some(2))), Nil)
    val rank = report.firstClickRank.get
    assertEquals(rank.clickedSearches, 3)
    assertEquals(rank.medianRank, 2.0)
    assertEqualsDouble(rank.meanRank, 10.0 / 3.0, 1e-9)
  }

  test("GDF-07 median and mean of skewed unsorted seconds to first status change, even count") {
    val changes = List(600L, 60L, 120L, 180L).map(sec => followUp(t0, Some(t0.plusSeconds(sec))))
    val summary = measure(Nil, changes).applicationToFirstStatusChange.get
    assertEquals(summary.reviewedApplications, 4)
    assertEquals(summary.medianSeconds, 150.0)
    assertEquals(summary.meanSeconds, 240.0)
  }

  test("GDF-07 rejects an invalid corpus configuration with every error") {
    val bad = DiscoveryBaselineCorpus.Config.Local.copy(searches = 0, clickRate = 1.5, maxResults = 0)
    assertEquals(DiscoveryBaselineCorpus.Config.validated(bad).fold(_.toList.size, _ => 0), 3)
    assert(DiscoveryBaselineCorpus.Config.validated(DiscoveryBaselineCorpus.Config.Local).isValid)
  }

  test("GDF-07 the same seed yields the same corpus and report, a different seed differs") {
    val local = DiscoveryBaselineCorpus.Config.Local
    val first = DiscoveryBaselineCorpus.generate(validConfig(local))
    assertEquals(DiscoveryBaselineCorpus.generate(validConfig(local)), first)
    assertNotEquals(DiscoveryBaselineCorpus.generate(validConfig(local.copy(seed = 12L))), first)
  }

  test("GDF-07 seed-11 local corpus reproduces the pinned aggregates and renders without per-record data") {
    val local = DiscoveryBaselineCorpus.Config.Local
    val (searches, followUps) = DiscoveryBaselineCorpus.generate(validConfig(local))
    val report = measure(searches, followUps)
    assertEquals(report.searches, 400)
    assertEquals(report.applications, 200)
    assertEquals(report.unreviewedApplications, 43)
    assertEquals(report.firstClickRank.map(_.clickedSearches), Some(155))
    assertEquals(report.firstClickRank.map(_.medianRank), Some(5.0))
    assertEquals(report.applicationToFirstStatusChange.map(_.reviewedApplications), Some(157))
    val expected = List(
      "LOCAL SYNTHETIC BASELINE - no target set, no production claim",
      "searches=400 zeroResultRate=15.8% clickedSearchShare=38.8%",
      "firstClickRank clicked=155 mean=5.84 median=5.0",
      "applications=200 unreviewed=43",
      "applicationToFirstStatusChange reviewed=157 meanSeconds=139636.8 medianSeconds=152640.0"
    ).mkString("\n")
    assertEquals(report.render, expected)
    val previous = Locale.getDefault
    try {
      Locale.setDefault(Locale.GERMANY)
      assertEquals(report.render, expected)
    } finally Locale.setDefault(previous)
    println(report.render)
  }
}
