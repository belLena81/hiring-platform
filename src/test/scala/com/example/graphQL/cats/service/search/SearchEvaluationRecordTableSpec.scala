package com.example.graphQL.cats.service.search

import munit.FunSuite

final class SearchEvaluationRecordTableSpec extends FunSuite {
  private def record(identity: String, recallVsExact: String = "null") =
    s"""{"strategy":"Vector","environment":{"identity":"$identity","atlasVersion":"8.3.11"},
       |"measurements":{"successfulQueries":10,"failedQueries":0,"unavailableQueries":2,
       |"successfulOnlyMeanRelevance":{"recallAtK":0.8,"ndcgAtK":0.75},"meanRecallAtKAgainstExact":$recallVsExact,
       |"successLatencyMillis":{"p50":27.8,"p95":78.0,"p99":78.0}}}""".stripMargin

  test("tabulates quality, latency and drift and labels local containers") {
    val table = SearchEvaluationRecordTable
      .summarize(
        List(
          "a.json" -> record("curated-disposable-local-container-db", "0.65"),
          "b.json" -> record("curated-disposable-atlas-db")
        )
      )
      .fold(error => fail(error), identity)
    assert(
      table.contains("| a.json | Vector | local-container | 8.3.11 | 10 | 0 | 2 | 0.800 | 0.750 | 0.650 | 27.800 |")
    )
    assert(table.contains("| b.json | Vector | atlas-labelled | 8.3.11 | 10 | 0 | 2 | 0.800 | 0.750 | n/a |"))
    assert(table.contains("not Atlas latency"))
  }

  test("rejects malformed records without echoing their content") {
    val invalid = SearchEvaluationRecordTable.summarize(List("bad.json" -> "{not json secret-value"))
    assertEquals(invalid, Left("bad.json: invalid JSON"))
    val missing = SearchEvaluationRecordTable.summarize(List("m.json" -> """{"strategy":"Vector"}"""))
    assertEquals(missing, Left("m.json: missing environment.identity"))
  }
}
