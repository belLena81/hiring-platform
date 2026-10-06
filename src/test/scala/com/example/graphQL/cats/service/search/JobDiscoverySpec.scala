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
    val cursor = NearbyJobsQuery.encodeCursor(3.4d, JobId(new UUID(0L, 1L)), query)
    assert(NearbyJobsQuery.decodeCursor(cursor, query).isRight)
    assert(NearbyJobsQuery.decodeCursor(cursor, query.copy(center = GeoPoint(36d, 33d))).isLeft)
  }
  test("cursors reject malformed identity, radius, normalized filter and ordering mismatches") {
    val query = NearbyJobsQuery(GeoPoint(35d, 33d), 20d, JobSearchFilter(Some(" Nicosia "), Set("Scala"), Some(now)))
    val cursor = NearbyJobsQuery.encodeCursor(1.234567890123456d, JobId(new UUID(0L, 1L)), query)
    assert(NearbyJobsQuery.decodeCursor(cursor, query.copy(radiusKm = 21d)).isLeft)
    assert(NearbyJobsQuery.decodeCursor(cursor, query.copy(filter = query.filter.copy(skills = Set("Cats")))).isLeft)
    assert(NearbyJobsQuery.decodeCursor(cursor, query.copy(filter = query.filter.copy(createdAfter = None))).isLeft)
    assert(NearbyJobsQuery.decodeCursor(cursor, query.copy(filter = query.filter.copy(city = Some("Nicosia")))).isRight)
    val malformed = java.util.Base64.getUrlEncoder
      .withoutPadding()
      .encodeToString(
        s"1.0|invalid|${query.fingerprint}".getBytes(java.nio.charset.StandardCharsets.UTF_8)
      )
    assertEquals(NearbyJobsQuery.decodeCursor(malformed, query), Left(NearbyCursorError.InvalidIdentity))
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

  test("filter validation is accumulating, bounded and normalization is idempotent") {
    val raw = JobSearchFilter(Some(" Nicosia "), Set(" Scala ", "Scala", " "), None)
    val expected = JobSearchFilter(Some("Nicosia"), Set("Scala"), None)
    assertEquals(JobDiscoveryValidation.filter(raw).toOption, Some(expected))
    assertEquals(JobDiscoveryValidation.filter(expected).toOption, Some(expected))
    assert(JobDiscoveryValidation.filter(JobSearchFilter(None, Set.empty, None)).isValid)
    assert(JobDiscoveryValidation.filter(expected.copy(city = Some("a" * 256), skills = Set("b" * 256))).isValid)
    assert(JobDiscoveryValidation.filter(expected.copy(skills = (1 to 100).map(index => s"Skill$index").toSet)).isValid)
    assert(
      JobDiscoveryValidation.filter(expected.copy(skills = (1 to 101).map(index => s"Skill$index").toSet)).isInvalid
    )
    val accumulated = JobDiscoveryValidation.filter(expected.copy(city = Some(" "), skills = Set("a" * 257)))
    assertEquals(accumulated.swap.toOption.map(_.length), Some(2L))
  }

  test("typed cursor identities preserve the existing full-precision wire format") {
    val query = NearbyJobsQuery(GeoPoint(35d, 33d), 20d, JobSearchFilter(None, Set.empty, None))
    val identity = JobId(new UUID(0L, 1L))
    val distance = 1.234567890123456d
    val expected = java.util.Base64.getUrlEncoder
      .withoutPadding()
      .encodeToString(
        s"$distance|${identity.value}|${query.fingerprint}".getBytes(java.nio.charset.StandardCharsets.UTF_8)
      )
    assertEquals(NearbyJobsQuery.encodeCursor(distance, identity, query), expected)
    assertEquals(
      NearbyJobsQuery.decodeCursor(expected, query),
      Right(NearbyJobCursor(distance, identity, query.fingerprint))
    )
    assertEquals(NearbyJobsQuery.decodeCursor("%", query), Left(NearbyCursorError.InvalidEncoding))
    List(-1d, Double.NaN, Double.PositiveInfinity).foreach { invalid =>
      assertEquals(
        NearbyJobCursorCodec.validate(NearbyJobCursor(invalid, identity, query.fingerprint), query),
        Left(NearbyCursorError.InvalidDistance)
      )
    }
  }

  test("nearby cursor canonical criteria and full-precision bytes match fixed golden values") {
    val query = NearbyJobsQuery(
      GeoPoint(35.5d, 33.25d),
      20.5d,
      JobSearchFilter(Some(" Nicosia|Old \"Town\" "), Set(" Scala,Cats ", "Pipe|Skill", " "), Some(now))
    )
    val fingerprint = "a69011814621cddab045b832630cba7e5e326ecb4d8d21f71d557879cf62e844"
    val encoded =
      "MS4yMzQ1Njc4OTAxMjM0NTZ8MDAwMDAwMDAtMDAwMC0wMDAwLTAwMDAtMDAwMDAwMDAwMDAxfGE2OTAxMTgxNDYyMWNkZGFiMDQ1YjgzMjYzMGNiYTdlNWUzMjZlY2I0ZDhkMjFmNzFkNTU3ODc5Y2Y2MmU4NDQ"
    val identity = JobId(new UUID(0L, 1L))
    val distance = 1.234567890123456d
    assertEquals(query.fingerprint, fingerprint)
    assertEquals(NearbyJobsQuery.encodeCursor(distance, identity, query), encoded)
    assertEquals(NearbyJobsQuery.decodeCursor(encoded, query), Right(NearbyJobCursor(distance, identity, fingerprint)))
    assertEquals(
      query
        .copy(filter = JobSearchFilter(Some("Nicosia|Old \"Town\""), Set("Pipe|Skill", "Scala,Cats"), Some(now)))
        .fingerprint,
      fingerprint
    )
    assertNotEquals(
      query.copy(filter = query.filter.copy(skills = Set("Scala", "Cats", "Pipe|Skill"))).fingerprint,
      fingerprint
    )
    assertEquals(
      NearbyJobsQuery(GeoPoint(0.5d, -0.25d), 0.5d, JobSearchFilter(None, Set.empty, None)).fingerprint,
      "8cb85609aace395691451337b669d90338f1a26a4209c18f5864117fa67116cd"
    )
  }

  test("facet dimensions preserve empty results and each dimension's count and order") {
    assertEquals(JobDiscoveryFacets.fromJobs(Nil), JobDiscoveryFacets(Nil, Nil, Nil, Nil, truncated = false))
    val result = JobDiscoveryFacets.fromJobs(
      List(
        job(1, Set("Scala", "Cats"), "Nicosia"),
        job(2, Set("Scala"), "Limassol").copy(location = Location("Greece", "Limassol", remote = true)),
        job(3, Set("Cats"), "Nicosia")
      )
    )
    assertEquals(
      result,
      JobDiscoveryFacets(
        List(JobFacetBucket("Cats", 2L), JobFacetBucket("Scala", 2L)),
        List(JobFacetBucket("Cyprus", 2L), JobFacetBucket("Greece", 1L)),
        List(JobFacetBucket("Nicosia", 2L), JobFacetBucket("Limassol", 1L)),
        List(JobFacetBucket("false", 2L), JobFacetBucket("true", 1L)),
        truncated = false
      )
    )
  }

  test("facet bucket bounds preserve tied value order and report truncation only above twenty") {
    val jobs = (1 to 21).toList.map { index =>
      job(index, Set(f"Skill$index%02d"), f"City$index%02d")
        .copy(location = Location(f"Country$index%02d", f"City$index%02d", remote = false))
    }
    val result = JobDiscoveryFacets.fromJobs(jobs)
    assertEquals(result.skills, (1 to 20).toList.map(index => JobFacetBucket(f"Skill$index%02d", 1L)))
    assertEquals(result.countries, (1 to 20).toList.map(index => JobFacetBucket(f"Country$index%02d", 1L)))
    assertEquals(result.cities, (1 to 20).toList.map(index => JobFacetBucket(f"City$index%02d", 1L)))
    assertEquals(result.remote, List(JobFacetBucket("false", 21L)))
    assert(result.truncated)
    assert(!JobDiscoveryFacets.fromJobs(jobs.take(20)).truncated)
  }

}
