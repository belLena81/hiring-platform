package com.example.graphQL.cats.shared.search

import io.circe.Json

class SearchEvaluationHarnessSpec extends munit.FunSuite {
  test("the bounded report records ANN quality, errors, latency, throughput, and run context") {
    val run = SearchEvaluationRun(
      strategy = "applicationRrf",
      datasetDocuments = 10000,
      filters = Json.obj("skills" -> Json.arr(Json.fromString("scala"))),
      embeddingModel = "synthetic-1024",
      embeddingDimensions = 1024,
      quantization = "none",
      numCandidates = 100,
      pageSize = 20,
      concurrency = 8,
      durationMillis = 2000,
      warmupQueries = 5,
      temperature = "warm",
      timestampUtc = "2026-09-23T12:00:00Z",
      environment = "synthetic-local",
      atlasVersion = "atlas-8.3",
      collectionIndexBytes = Some(4096L),
      vectorSearchIndexBytes = None,
      cpuMillis = Some(120L),
      peakMemoryBytes = Some(2048L),
      providerRequests = Some(0L),
      queryMetrics = Json.obj("queriesExamined" -> Json.fromLong(80L)),
      queries = List(
        SearchEvaluationQuery("q1", Set("a"), List("a", "b"), List("a", "c"), 10.0, Some(14.0), None),
        SearchEvaluationQuery("q2", Set("c"), List("b", "c"), List("c", "d"), 30.0, Some(34.0), None),
        SearchEvaluationQuery("q3", Set("e"), Nil, Nil, 0.0, None, Some("synthetic failure"))
      )
    )

    val json = SearchEvaluationHarness.report(run, k = 2)
    val measurements = json.hcursor.downField("measurements")
    assertEquals(measurements.get[Int]("successfulQueries").toOption, Some(2))
    assertEquals(measurements.get[Int]("errors").toOption, Some(1))
    assertEquals(measurements.get[Double]("throughputQueriesPerSecond").toOption, Some(1.0))
    assertEquals(measurements.downField("latencyMillis").downField("ann").get[Double]("p50").toOption, Some(10.0))
    assertEquals(measurements.downField("latencyMillis").downField("ann").get[Double]("p95").toOption, Some(30.0))
    assertEquals(measurements.downField("latencyMillis").downField("enn").get[Double]("p95").toOption, Some(34.0))
    assertEquals(measurements.get[Double]("meanRecallAtKAgainstEnn").toOption, Some(0.5))
    assertEqualsDouble(
      measurements.get[Double]("meanNdcgAtKAgainstJudgments").toOption.getOrElse(0.0),
      0.8154648767857287,
      0.0000001
    )
    assertEquals(
      json.hcursor.downField("environment").downField("queryMetrics").get[Long]("queriesExamined").toOption,
      Some(80L)
    )
    assertEquals(json.hcursor.downField("workload").get[Int]("numCandidates").toOption, Some(100))
    assertEquals(json.hcursor.downField("workload").get[String]("timestampUtc").toOption, Some("2026-09-23T12:00:00Z"))
  }
}
