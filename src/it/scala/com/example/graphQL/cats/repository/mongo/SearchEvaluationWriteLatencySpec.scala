package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.repository.mongo.SearchEvaluationWriteLatency.*
import munit.FunSuite

/** Pure checks of the write-latency aggregation and option validation; they need no Atlas deployment. */
final class SearchEvaluationWriteLatencySpec extends FunSuite {
  private def ok(values: Double*) = values.toList.map(Sample(_, failed = false))

  test("MVR-07.L write samples are off when the option is absent") {
    assertEquals(parseSampleCount(None), Right(0))
  }

  test("MVR-07.L write samples accept 0 through the bound and reject beyond it or non-numbers") {
    assertEquals(parseSampleCount(Some("0")), Right(0))
    assertEquals(parseSampleCount(Some("200")), Right(200))
    assertEquals(parseSampleCount(Some("2000")), Right(2000))
    assert(parseSampleCount(Some("2001")).isLeft)
    assert(parseSampleCount(Some("-1")).isLeft)
    assert(parseSampleCount(Some("many")).isLeft)
  }

  test("MVR-07.L warmup is bounded and never exceeds the sample count") {
    assertEquals(warmupCount(3), 3)
    assertEquals(warmupCount(2000), 10)
  }

  test("MVR-07.L percentiles use nearest rank over successful operations") {
    val figures = operationFigures(ok((1 to 100).map(_.toDouble)*))
    assertEquals(figures.count, 100)
    assertEquals(figures.p50, Some(50.0))
    assertEquals(figures.p95, Some(95.0))
    assertEquals(figures.p99, Some(99.0))
  }

  test("MVR-07.L failed operations count as errors and stay out of the percentiles") {
    val figures = operationFigures(ok(1.0, 2.0) :+ Sample(500.0, failed = true))
    assertEquals((figures.count, figures.errors), (3, 1))
    assertEquals(figures.p99, Some(2.0))
  }

  test("MVR-07.L an update that matched or modified no document is not a successful sample") {
    assert(updateApplied(1L, 1L))
    assert(!updateApplied(0L, 0L))
    assert(!updateApplied(1L, 0L))
    assert(!updateApplied(2L, 2L))
  }

  test("MVR-07.L an empty or all-failed sample has absent percentiles") {
    assertEquals(operationFigures(Nil), OperationFigures(0, 0, None, None, None))
    assertEquals(operationFigures(List(Sample(1.0, failed = true))).p95, None)
  }

  test("MVR-07.L the record names the criterion, storage form, insert and update figures") {
    val json = toJson("float64-array", figures(5, ok(2.0, 4.0), ok(1.0)))
    val cursor = json.hcursor
    assertEquals(cursor.get[String]("criterion"), Right("MVR-07.L"))
    assertEquals(cursor.get[String]("vectorStorage"), Right("float64-array"))
    assertEquals(cursor.get[Int]("concurrency"), Right(1))
    assertEquals(cursor.downField("insert").get[Double]("p95Millis"), Right(4.0))
    assertEquals(cursor.downField("update").get[Int]("count"), Right(1))
  }
}
