package com.example.hiring.analytics

import com.example.hiring.analytics.domain.AnalyticsLakehouseIdentity

import munit.FunSuite

final class AnalyticsLakehouseIdentitySpec extends FunSuite {
  test("equivalent normalized lakehouse URIs share one privacy-safe identity") {
    val aliased = AnalyticsLakehouseIdentity.from("file:///var/lib/hiring-analytics/data/../lakehouse/")
    val canonical = AnalyticsLakehouseIdentity.from("file:///var/lib/hiring-analytics/lakehouse")

    assertEquals(aliased, canonical)
  }

  test("lakehouse identity rejects URI credentials and query parameters") {
    assert(AnalyticsLakehouseIdentity.from("s3a://user:credential@example/lakehouse").isLeft)
    assert(AnalyticsLakehouseIdentity.from("file:///lakehouse?version=1").isLeft)
  }
}
