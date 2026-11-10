package com.example.graphQL.cats.service.search

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.GeoPoint
import com.example.graphQL.cats.service.{ActorContext, ServiceFixtures}
import munit.CatsEffectSuite

/** Boundary examples for the nearby-jobs radius and center that the existing discovery specs do not pin. */
final class NearbyJobsRadiusBoundarySpec extends CatsEffectSuite {
  private val candidate = ServiceFixtures.candidate
  private val actor = ActorContext(candidate.id, candidate.role)
  private val filter = JobSearchFilter(None, Set.empty, None)
  private def query(center: GeoPoint, radiusKm: Double) = NearbyJobsQuery(center, radiusKm, filter)
  private val nicosia = GeoPoint(35d, 33d)

  test(
    "HDQ-02 radius boundaries: zero, negative, nonfinite and just over 500 km are invalid; tiny and exactly 500 km are valid"
  ) {
    List(0d, -0d, -1d, Double.NaN, Double.PositiveInfinity, Double.NegativeInfinity, 500.000001d, Double.MaxValue)
      .foreach(radius => assert(!query(nicosia, radius).isValid, clue(radius)))
    List(Double.MinPositiveValue, 0.001d, 1d, 499.999999d, 500d)
      .foreach(radius => assert(query(nicosia, radius).isValid, clue(radius)))
  }

  test("HDQ-02 center boundaries: poles and the antimeridian are valid; one step beyond or nonfinite is invalid") {
    List(
      GeoPoint(90d, 0d),
      GeoPoint(-90d, 0d),
      GeoPoint(0d, 180d),
      GeoPoint(0d, -180d),
      GeoPoint(90d, 180d),
      GeoPoint(-90d, -180d)
    ).foreach(center => assert(query(center, 10d).isValid, clue(center)))
    List(
      GeoPoint(90.000001d, 0d),
      GeoPoint(-90.000001d, 0d),
      GeoPoint(0d, 180.000001d),
      GeoPoint(0d, -180.000001d),
      GeoPoint(Double.NaN, 0d),
      GeoPoint(0d, Double.NaN),
      GeoPoint(Double.PositiveInfinity, 0d),
      GeoPoint(0d, Double.NegativeInfinity)
    ).foreach(center => assert(!query(center, 10d).isValid, clue(center)))
  }

  test("HDQ-02 service executes the exact 500 km radius and antimeridian or polar centers for a Candidate") {
    val accepted = List(
      query(nicosia, 500d),
      query(GeoPoint(0d, 180d), 10d),
      query(GeoPoint(0d, -180d), 10d),
      query(GeoPoint(90d, 0d), 10d),
      query(GeoPoint(-90d, 0d), 10d)
    )
    JobDiscoveryTestSupport.fixture(List(candidate)).flatMap { fixture =>
      accepted
        .traverse_(value => fixture.service.nearbyJobs(actor, value, 20).value.map(r => assertEquals(r, Right(Nil)))) *>
        fixture.calls.get.map(calls => assertEquals(calls.nearby, accepted))
    }
  }

  test("HDQ-02 service rejects zero, nonfinite and over-500 km radii and nonfinite centers before discovery storage") {
    val rejected = List(
      query(nicosia, 0d),
      query(nicosia, Double.NaN),
      query(nicosia, Double.PositiveInfinity),
      query(nicosia, 500.000001d),
      query(GeoPoint(Double.NaN, 33d), 10d),
      query(GeoPoint(35d, Double.PositiveInfinity), 10d),
      query(GeoPoint(0d, 180.000001d), 10d)
    )
    JobDiscoveryTestSupport.fixture(List(candidate)).flatMap { fixture =>
      rejected.traverse_(value =>
        fixture.service.nearbyJobs(actor, value, 20).value.map(r => assert(r.isLeft, clue(value)))
      ) *>
        fixture.calls.get.map(calls => assertEquals(calls, JobDiscoveryTestSupport.Calls()))
    }
  }
}
