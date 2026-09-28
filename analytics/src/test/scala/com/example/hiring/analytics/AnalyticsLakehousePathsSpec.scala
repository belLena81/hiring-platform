package com.example.hiring.analytics

import com.example.hiring.analytics.service.batch.AnalyticsLakehousePaths

class AnalyticsLakehousePathsSpec extends munit.FunSuite {
  test("lakehouse paths are available only for a nonblank root") {
    assert(AnalyticsLakehousePaths.from("").isInvalid)
    assert(AnalyticsLakehousePaths.from("   ").isInvalid)
    val paths = AnalyticsLakehousePaths.from("file:///tmp/hiring-analytics/").toEither.toOption.get
    assertEquals(paths.root, "file:///tmp/hiring-analytics/")
    assertEquals(paths.bronze, "file:///tmp/hiring-analytics/bronze/operational_events")
  }
}
