package com.example.graphQL.cats.service.search

import java.time.Instant

class SearchEvaluationConcurrencySpec extends munit.CatsEffectSuite {
  test("actual authored branch/RRF evaluation at bounded concurrency 1 and 8 preserves rankings and scores") {
    val timestamp = Instant.parse("2026-10-06T12:00:00Z")
    for {
      serial <- SearchEvaluationFixtureReplay.captureAuthoredRun(
        SearchEvaluationStrategy.ApplicationRrf,
        "controlled-test-snapshot",
        "controlled-test-fingerprint",
        timestamp,
        1
      )
      concurrent <- SearchEvaluationFixtureReplay.captureAuthoredRun(
        SearchEvaluationStrategy.ApplicationRrf,
        "controlled-test-snapshot",
        "controlled-test-fingerprint",
        timestamp,
        8
      )
    } yield {
      assertEquals(serial.queries, concurrent.queries)
      val first = SearchEvaluationHarness
        .report(SearchEvaluationFixtures.corpus, serial, 7)
        .fold(errors => fail(errors.toString), identity)
      val second = SearchEvaluationHarness
        .report(SearchEvaluationFixtures.corpus, concurrent, 7)
        .fold(errors => fail(errors.toString), identity)
      assertEquals(first.queries, second.queries)
      assertEquals(first.summary, second.summary)
      assertEquals(first.groups, second.groups)
      assert(SearchEvaluationHarness.compare(first, second).isLeft)
    }
  }
}
