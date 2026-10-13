package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.service.search.RetrievalLimits.*

final class RetrievalLimitsSpec extends munit.FunSuite {
  private def obs(recall: Option[Double], hits: Boolean, candidate: Double, baseline: Double = 100.0) =
    BucketObservation(recall, hits, candidate, baseline)

  test("MVR-08 retrieval limits rule: a bucket with high recall and lower p95 passes") {
    assertEquals(check(Agreed, obs(Some(0.99), true, 90.0)), Verdict.Pass)
  }
  test("MVR-08 retrieval limits rule: recall exactly 0.95 passes and just below fails") {
    assertEquals(check(Agreed, obs(Some(0.95), true, 100.0)), Verdict.Pass)
    assertEquals(
      check(Agreed, obs(Some(0.9499), true, 100.0)),
      Verdict.Fail(List(Violation.RecallBelowMinimum(0.9499)))
    )
  }
  test("MVR-08 retrieval limits rule: p95 exactly 10 percent above baseline passes and above fails") {
    assertEquals(check(Agreed, obs(Some(1.0), true, 110.0)), Verdict.Pass)
    assert(check(Agreed, obs(Some(1.0), true, 110.5)).isInstanceOf[Verdict.Fail])
  }
  test("MVR-08 retrieval limits rule: recall and latency failures are both reported") {
    check(Agreed, obs(Some(0.5), true, 200.0)) match {
      case Verdict.Fail(violations) => assertEquals(violations.size, 2)
      case Verdict.Pass             => fail("expected failure")
    }
  }
  test("MVR-08 retrieval limits rule: empty bucket is vacuously satisfied only when ANN also returns nothing") {
    assertEquals(check(Agreed, obs(None, false, 100.0)), Verdict.Pass)
    assertEquals(
      check(Agreed, obs(None, true, 100.0)),
      Verdict.Fail(List(Violation.RecallUndefinedButAnnReturnedHits))
    )
  }
  test("MVR-08 retrieval limits rule: empty bucket still enforces the latency limit") {
    assert(check(Agreed, obs(None, false, 150.0)).isInstanceOf[Verdict.Fail])
  }
  test("MVR-08 retrieval limits rule: non-finite, out-of-range or zero-baseline measurements are invalid") {
    val invalid = Verdict.Fail(List(Violation.InvalidMeasurement))
    assertEquals(check(Agreed, obs(Some(Double.NaN), true, 90.0)), invalid)
    assertEquals(check(Agreed, obs(Some(1.2), true, 90.0)), invalid)
    assertEquals(check(Agreed, obs(Some(1.0), true, 90.0, 0.0)), invalid)
    assertEquals(check(Agreed, obs(Some(1.0), true, Double.PositiveInfinity)), invalid)
  }
  test("MVR-08 retrieval limits rule: each filter bucket and concurrency is judged independently") {
    val result = checkAll(
      Agreed,
      Map(
        (SearchEvaluationFilterGroup.Broad, 1) -> obs(Some(0.97), true, 100.0),
        (SearchEvaluationFilterGroup.Broad, 8) -> obs(Some(0.90), true, 100.0),
        (SearchEvaluationFilterGroup.Empty, 1) -> obs(None, false, 100.0)
      )
    )
    assertEquals(result((SearchEvaluationFilterGroup.Broad, 1)), Verdict.Pass)
    assert(result((SearchEvaluationFilterGroup.Broad, 8)).isInstanceOf[Verdict.Fail])
    assertEquals(result((SearchEvaluationFilterGroup.Empty, 1)), Verdict.Pass)
  }
}
