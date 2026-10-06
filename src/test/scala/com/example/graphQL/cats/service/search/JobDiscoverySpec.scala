package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.model.*
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import java.time.Instant
import java.util.UUID
import munit.FunSuite

class JobDiscoverySpec extends FunSuite {
  private val now = Instant.parse("2026-10-06T12:00:00Z")
  private def job(id: Int, skills: Set[String], city: String): Job = Job(
    JobId(new UUID(0L, id.toLong)),
    UserId(new UUID(1L, 1L)),
    "Engineer",
    "Build",
    List("Scala"),
    skills,
    Location("Cyprus", city, remote = false),
    JobStatus.Open,
    now,
    now
  )

  test("nearby criteria reject invalid centers and radii above the bound") {
    assert(!NearbyJobsQuery(GeoPoint(91d, 0d), 10d, JobSearchFilter(None, Set.empty, None)).isValid)
    assert(!NearbyJobsQuery(GeoPoint(35d, 33d), 501d, JobSearchFilter(None, Set.empty, None)).isValid)
    assert(NearbyJobsQuery(GeoPoint(35d, 33d), 500d, JobSearchFilter(None, Set.empty, None)).isValid)
  }

  test("facets count each job skill once and sort count then value") {
    val result = JobDiscoveryFacets.fromJobs(
      List(
        job(1, Set("Scala", "Cats"), "Nicosia"),
        job(2, Set("Scala"), "Limassol"),
        job(3, Set("Cats"), "Nicosia")
      )
    )
    assertEquals(result.skills, List(JobFacetBucket("Cats", 2L), JobFacetBucket("Scala", 2L)))
    assertEquals(result.cities, List(JobFacetBucket("Nicosia", 2L), JobFacetBucket("Limassol", 1L)))
    assert(!result.truncated)
  }

  test("nearby cursors are bound to center, radius, filters and ordering") {
    val query = NearbyJobsQuery(GeoPoint(35d, 33d), 20d, JobSearchFilter(Some("Nicosia"), Set("Scala"), None))
    val cursor = NearbyJobsQuery.encodeCursor(3.4d, new UUID(0L, 1L).toString, query)
    assert(NearbyJobsQuery.decodeCursor(cursor, query).isRight)
    assert(NearbyJobsQuery.decodeCursor(cursor, query.copy(center = GeoPoint(36d, 33d))).isLeft)
  }
  test("cursors reject malformed identity, radius, normalized filter and ordering mismatches") {
    val query = NearbyJobsQuery(GeoPoint(35d, 33d), 20d, JobSearchFilter(Some(" Nicosia "), Set("Scala"), Some(now)))
    val cursor = NearbyJobsQuery.encodeCursor(1.234567890123456d, new UUID(0L, 1L).toString, query)
    assert(NearbyJobsQuery.decodeCursor(cursor, query.copy(radiusKm = 21d)).isLeft)
    assert(NearbyJobsQuery.decodeCursor(cursor, query.copy(filter = query.filter.copy(skills = Set("Cats")))).isLeft)
    assert(NearbyJobsQuery.decodeCursor(cursor, query.copy(filter = query.filter.copy(createdAfter = None))).isLeft)
    assert(NearbyJobsQuery.decodeCursor(cursor, query.copy(filter = query.filter.copy(city = Some("Nicosia")))).isRight)
    assert(NearbyJobsQuery.decodeCursor(NearbyJobsQuery.encodeCursor(1d, "invalid", query), query).isLeft)
    assertEquals(NearbyJobsQuery.decodeCursor(cursor, query).toOption.map(_.distanceKm), Some(1.234567890123456d))
  }

  test("non-finite coordinates and radii fail closed") {
    List(Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity).foreach { value =>
      assert(!NearbyJobsQuery(GeoPoint(value, 0d), 10d, JobSearchFilter(None, Set.empty, None)).isValid)
      assert(!NearbyJobsQuery(GeoPoint(0d, value), 10d, JobSearchFilter(None, Set.empty, None)).isValid)
      assert(!NearbyJobsQuery(GeoPoint(0d, 0d), value, JobSearchFilter(None, Set.empty, None)).isValid)
    }
  }

  test("structured cursor fingerprints distinguish delimiter-bearing facet values") {
    val query = NearbyJobsQuery(GeoPoint(35d, 33d), 20d, JobSearchFilter(Some("Nicosia"), Set("Scala,Cats"), None))
    assertNotEquals(
      query.fingerprint,
      query.copy(filter = query.filter.copy(skills = Set("Scala", "Cats"))).fingerprint
    )
  }

}
