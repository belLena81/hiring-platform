package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.model.GeoPoint
import java.time.Instant
import munit.FunSuite

/** Cursor fingerprints are persisted in client cursors; these literals must never change. */
class NearbyJobCursorFingerprintGoldenSpec extends FunSuite {
  private val now = Instant.parse("2026-09-19T10:15:30Z")
  private def query(radius: Double, city: Option[String]) =
    NearbyJobsQuery(GeoPoint(35.5d, 33.25d), radius, JobSearchFilter(city, Set("Scala"), Some(now)))

  test("fingerprint of a very small radius is a fixed golden value") {
    assertEquals(
      query(1e-4, Some("Nicosia")).fingerprint,
      "8bb48aef3e3e3e48f735236e575a9659d81a761640af1636e03217ef92896e05"
    )
  }

  test("fingerprint of a whole-number radius is a fixed golden value") {
    assertEquals(
      query(10.0, Some("Nicosia")).fingerprint,
      "6d17757fc6dbb92f59bfb731a85c604d58a21aa8f90f4c768fd568296f1a3aa3"
    )
  }

  test("fingerprint of a very large radius is a fixed golden value") {
    assertEquals(
      query(1e7, Some("Nicosia")).fingerprint,
      "4491d93650208ef6a64fa248fb0723cf2419395711f5a1ebbad4b4bbd9b09ff6"
    )
  }

  test("fingerprint of a city with a quote and non-ASCII letters is a fixed golden value") {
    assertEquals(
      query(10.0, Some("Λεμεσός \"Old\" Zażółć")).fingerprint,
      "5a63a40a9b1f7ab24c69d940b3463c708d20b0dfcaef1306b554955ccf478c9e"
    )
  }
}
