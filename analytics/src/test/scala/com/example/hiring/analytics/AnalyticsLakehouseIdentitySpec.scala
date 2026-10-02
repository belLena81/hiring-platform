package com.example.hiring.analytics

import com.example.hiring.analytics.domain.AnalyticsLakehouseIdentity
import com.example.hiring.analytics.domain.AnalyticsDigest

import java.nio.charset.StandardCharsets

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

  test("canonical local file roots preserve their established identity hash") {
    val canonical = "file:///var/lib/hiring+analytics/lakehouse%20with%25percent"
    val expected = AnalyticsDigest.sha256Hex(
      "file:/var/lib/hiring+analytics/lakehouse%20with%25percent".getBytes(StandardCharsets.UTF_8)
    )
    assertEquals(AnalyticsLakehouseIdentity.from(canonical), Right(expected))
  }

  test("local escaped aliases cannot claim a second identity for one physical directory") {
    Vector(
      "file:///var/lib/hiring%2Banalytics/lakehouse",
      "file:///var/lib/%68iring/lakehouse",
      "file:///var/lib/hiring%2fanalytics/lakehouse",
      "file:///var/lib/hiring/%2E%2E/lakehouse",
      "file://localhost/var/lib/hiring/lakehouse",
      "file:relative/lakehouse"
    ).foreach(root => assert(AnalyticsLakehouseIdentity.from(root).isLeft))
  }
}
