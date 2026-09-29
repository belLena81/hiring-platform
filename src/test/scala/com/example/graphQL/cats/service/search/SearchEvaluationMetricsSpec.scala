package com.example.graphQL.cats.service.search

class SearchEvaluationMetricsSpec extends munit.FunSuite {
  test("Recall@K and NDCG@K compare a ranking with judged relevant results") {
    val relevant = Set("a", "c")
    assertEquals(SearchEvaluationMetrics.recallAtK(List("a", "b"), relevant, 2), 0.5)
    assertEquals(SearchEvaluationMetrics.ndcgAtK(List("a", "c"), relevant, 2), 1.0)
    assertEquals(SearchEvaluationMetrics.ndcgAtK(List("c", "a"), relevant, 2), 1.0)
  }

  test("empty judgments and nonpositive cutoffs produce zero") {
    assertEquals(SearchEvaluationMetrics.recallAtK(List("a"), Set.empty, 5), 0.0)
    assertEquals(SearchEvaluationMetrics.ndcgAtK(List("a"), Set("a"), 0), 0.0)
  }
}
