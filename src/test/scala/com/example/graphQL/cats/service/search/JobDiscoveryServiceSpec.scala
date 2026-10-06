package com.example.graphQL.cats.service.search

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{AccountStatus, GeoPoint}
import com.example.graphQL.cats.service.{ActorContext, ServiceFixtures}
import munit.CatsEffectSuite

final class JobDiscoveryServiceSpec extends CatsEffectSuite {
  private val candidate = ServiceFixtures.candidate
  private val actor = ActorContext(candidate.id, candidate.role)
  private val filter = JobSearchFilter(None, Set.empty, None)
  private val query = NearbyJobsQuery(GeoPoint(35d, 33d), 20d, filter)

  test("direct invalid cursor distances reject before discovery repository calls") {
    JobDiscoveryTestSupport.fixture(List(candidate)).flatMap { fixture =>
      List(-1d, Double.NaN, Double.PositiveInfinity).traverse_ { distance =>
        val cursor = NearbyJobCursor(distance, ServiceFixtures.jobId, query.fingerprint)
        fixture.service
          .nearbyJobs(actor, query.copy(after = Some(cursor)), 20)
          .value
          .map(result => assert(result.isLeft))
      } *> fixture.calls.get.map(calls => assertEquals(calls, JobDiscoveryTestSupport.Calls()))
    }
  }

  test("discovery rejects blank and oversized structured filters before querying") {
    val invalid = List(
      filter.copy(city = Some(" ")),
      filter.copy(city = Some("a" * 257)),
      filter.copy(skills = Set("a" * 257)),
      filter.copy(skills = (1 to 101).map(index => s"Skill$index").toSet)
    )
    JobDiscoveryTestSupport.fixture(List(candidate)).flatMap { fixture =>
      invalid.traverse_ { value =>
        (
          fixture.service.nearbyJobs(actor, query.copy(filter = value), 20).value,
          fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(value, None)).value
        ).mapN { (nearby, facets) =>
          assert(nearby.isLeft)
          assert(facets.isLeft)
        }
      } *> fixture.calls.get.map(calls => assertEquals(calls, JobDiscoveryTestSupport.Calls()))
    }
  }

  test("nearby and facet service paths normalize filters identically") {
    val raw = filter.copy(city = Some(" Nicosia "), skills = Set(" Scala ", "Scala", " "))
    val normalized = filter.copy(city = Some("Nicosia"), skills = Set("Scala"))
    JobDiscoveryTestSupport.fixture(List(candidate)).flatMap { fixture =>
      for {
        nearby <- fixture.service.nearbyJobs(actor, query.copy(filter = raw), 20).value
        facets <- fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(raw, None)).value
        calls <- fixture.calls.get
      } yield {
        assert(nearby.isRight)
        assert(facets.isRight)
        assertEquals(calls.nearby.map(_.filter), List(normalized))
        assertEquals(calls.facets.map(_.filter), List(normalized))
      }
    }
  }

  test("recruiters and deleted accounts cannot execute discovery") {
    val actors = List(ServiceFixtures.recruiter, candidate.copy(accountStatus = AccountStatus.Deleted))
    actors.traverse_ { user =>
      JobDiscoveryTestSupport.fixture(List(user)).flatMap { fixture =>
        val denied = ActorContext(user.id, user.role)
        for {
          nearby <- fixture.service.nearbyJobs(denied, query, 20).value
          facets <- fixture.service.jobDiscoveryFacets(denied, JobFacetQuery(filter, None)).value
          calls <- fixture.calls.get
        } yield {
          assert(nearby.isLeft)
          assert(facets.isLeft)
          assertEquals(calls, JobDiscoveryTestSupport.Calls())
        }
      }
    }
  }

  test("valid candidate and singleton admin requests return empty results and facets") {
    List(candidate, ServiceFixtures.admin).traverse_ { user =>
      JobDiscoveryTestSupport.fixture(List(user)).flatMap { fixture =>
        val authorized = ActorContext(user.id, user.role)
        for {
          nearby <- fixture.service.nearbyJobs(authorized, query, 100).value
          facets <- fixture.service.jobDiscoveryFacets(authorized, JobFacetQuery(filter, None)).value
          calls <- fixture.calls.get
        } yield {
          assertEquals(nearby, Right(Nil))
          assertEquals(facets, Right(JobDiscoveryFacets(Nil, Nil, Nil, Nil, truncated = false)))
          assertEquals(calls.nearby, List(query))
          assertEquals(calls.facets, List(JobFacetQuery(filter, None)))
        }
      }
    }
  }

  test("direct invalid geographic, page and mismatched cursor inputs never execute discovery") {
    val mismatched = NearbyJobCursor(1d, ServiceFixtures.jobId, "wrong")
    val invalid = List(
      query.copy(center = GeoPoint(91d, 0d)) -> 20,
      query.copy(radiusKm = 501d) -> 20,
      query.copy(after = Some(mismatched)) -> 20,
      query -> 0,
      query -> 101
    )
    JobDiscoveryTestSupport.fixture(List(candidate)).flatMap { fixture =>
      invalid.traverse_ { case (value, limit) =>
        fixture.service.nearbyJobs(actor, value, limit).value.map(result => assert(result.isLeft))
      } *> fixture.service
        .jobDiscoveryFacets(actor, JobFacetQuery(filter, Some(NearbyRadius(GeoPoint(0d, 0d), 0d))))
        .value
        .map(result => assert(result.isLeft)) *>
        fixture.calls.get.map(calls => assertEquals(calls, JobDiscoveryTestSupport.Calls()))
    }
  }
}
