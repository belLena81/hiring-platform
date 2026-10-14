package com.example.graphQL.cats.service.search

import cats.syntax.all.*
import com.example.graphQL.cats.domain.model.{AccountStatus, GeoPoint, Job, JobStatus, Location}
import com.example.graphQL.cats.domain.model.Identifiers.{JobId, UserId}
import com.example.graphQL.cats.service.{ActorContext, RepositoryError, SearchError, ServiceFixtures, UseCaseError}
import com.example.graphQL.cats.service.port.{JobRepository, RepositoryIO}
import com.example.graphQL.cats.service.read.HiringReadScope
import java.time.Instant
import java.util.UUID
import munit.CatsEffectSuite

/** Lexical-query facets: a fake lexical matcher feeds IDs; counts come from the operational job repository. */
final class LexicalQueryFacetsSpec extends CatsEffectSuite {
  private val candidate = ServiceFixtures.candidate
  private val actor = ActorContext(candidate.id, candidate.role)
  private val noFilter = JobSearchFilter(None, Set.empty, None)
  private val created = Instant.parse("2026-10-01T00:00:00Z")
  private val nicosia = GeoPoint(35.1856d, 33.3823d)

  private def jobId(n: Int): JobId = JobId(new UUID(0L, n.toLong))
  private def job(
      n: Int,
      skills: Set[String] = Set("Scala"),
      city: String = "Nicosia",
      status: JobStatus = JobStatus.Open,
      remote: Boolean = false,
      coordinates: Option[GeoPoint] = None,
      createdAt: Instant = created
  ): Job =
    ServiceFixtures.openJob.copy(
      id = jobId(n),
      skills = skills,
      status = status,
      location = Location("Cyprus", city, remote, coordinates),
      createdAt = createdAt
    )
  private def scored(ids: Int*): Map[JobId, Double] =
    ids.toList.map(n => jobId(n) -> (1000d - n)).toMap
  private def facetsOf(jobs: List[Job], coverage: FacetCoverage = FacetCoverage.AllMatches): JobDiscoveryFacets =
    JobDiscoveryFacets.fromJobs(jobs).copy(coverage = coverage)

  test("HDQ-12.1 counts exactly the matched Open jobs, equal to counts derived from the lexical hit IDs") {
    val jobs = (1 to 5).toList.map(n => job(n, city = if (n % 2 == 0) "Limassol" else "Nicosia"))
    for {
      matcher <- JobDiscoveryTestSupport.matcher(scored(1, 2, 3))
      fixture <- JobDiscoveryTestSupport.fixture(List(candidate), jobs, Some(matcher))
      result <- fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(noFilter, None, Some(" scala "))).value
      matcherCalls <- matcher.calls.get
      calls <- fixture.calls.get
    } yield {
      val matched = jobs.filter(job => Set(1, 2, 3).map(jobId).contains(job.id))
      assertEquals(result, Right(facetsOf(matched)))
      assertEquals(result.map(_.cities), Right(List(JobFacetBucket("Nicosia", 2L), JobFacetBucket("Limassol", 1L))))
      assertEquals(matcherCalls, List(("scala", noFilter, LexicalMatchLimit.Default.value)))
      assertEquals(calls.facetIds, List(Some(Set(1, 2, 3).map(jobId))))
    }
  }

  test("HDQ-12.2 counts the intersection of query, structured filter and createdAfter") {
    val cutoff = Instant.parse("2026-10-05T00:00:00Z")
    val jobs = List(
      job(1, Set("Scala", "Cats")),
      job(2, Set("Scala", "Cats"), city = "Limassol"),
      job(3, Set("Scala")),
      job(4, Set("Scala", "Cats"), createdAt = Instant.parse("2026-10-06T00:00:00Z")),
      job(5, Set("Scala", "Cats"), status = JobStatus.Closed)
    )
    val filter = JobSearchFilter(Some("Nicosia"), Set("Cats"), Some(created))
    for {
      matcher <- JobDiscoveryTestSupport.matcher(scored(1, 2, 3, 4, 5))
      fixture <- JobDiscoveryTestSupport.fixture(List(candidate), jobs, Some(matcher))
      result <- fixture.service
        .jobDiscoveryFacets(actor, JobFacetQuery(filter.copy(createdAfter = Some(cutoff)), None, Some("scala")))
        .value
      all <- fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(filter, None, Some("scala"))).value
    } yield {
      assertEquals(result, Right(facetsOf(List(jobs(3)))))
      assertEquals(all, Right(facetsOf(List(jobs(0), jobs(3)))))
    }
  }

  test("HDQ-12.2 with a radius, excluded, remote and coordinate-free jobs are absent") {
    val jobs = List(
      job(1, coordinates = Some(nicosia)),
      job(2, coordinates = Some(GeoPoint(34.9d, 33.0d)), city = "Limassol"),
      job(3, coordinates = Some(nicosia), remote = true),
      job(4, coordinates = None),
      job(5, coordinates = Some(GeoPoint(40d, 20d)))
    )
    val radius = Some(NearbyRadius(nicosia, 60d))
    for {
      matcher <- JobDiscoveryTestSupport.matcher(scored(1, 2, 3, 4, 5))
      fixture <- JobDiscoveryTestSupport.fixture(List(candidate), jobs, Some(matcher))
      result <- fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(noFilter, radius, Some("scala"))).value
    } yield assertEquals(result, Right(facetsOf(List(jobs(0), jobs(1)))))
  }

  test("HDQ-12.3 exactly N matches is ALL_MATCHES, N-1 is ALL_MATCHES, N+1 is TOP_MATCHES with the N best") {
    val cap = 3
    val jobs = (1 to 4).toList.map(n => job(n, Set(s"Skill$n")))
    def run(ids: Int*) =
      for {
        matcher <- JobDiscoveryTestSupport.matcher(scored(ids*))
        fixture <- JobDiscoveryTestSupport.fixture(List(candidate), jobs, Some(matcher), cap)
        result <- fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(noFilter, None, Some("scala"))).value
        matcherCalls <- matcher.calls.get
      } yield (result, matcherCalls)
    for {
      below <- run(1, 2)
      exact <- run(1, 2, 3)
      above <- run(1, 2, 3, 4)
    } yield {
      assertEquals(below._1, Right(facetsOf(jobs.take(2))))
      assertEquals(exact._1, Right(facetsOf(jobs.take(3))))
      // Job 4 has the lowest score, so it is the (N+1)th match and is not counted.
      assertEquals(above._1, Right(facetsOf(jobs.take(3), FacetCoverage.TopMatches(cap))))
      assertEquals(above._2.map(_._3), List(cap))
      assert(!above._1.exists(_.skills.exists(_.value == "Skill4")))
    }
  }

  test("HDQ-12.3 a score tie across ranks N and N+1 keeps the lower job ID inside the window") {
    val tied = List(
      ScoredJobId(jobId(1), 9d),
      ScoredJobId(jobId(4), 5d),
      ScoredJobId(jobId(3), 5d),
      ScoredJobId(jobId(2), 7d)
    )
    val matches = LexicalJobMatches.fromWindow(tied.reverse, 3)
    assertEquals(matches.ids, Set(1, 2, 3).map(jobId))
    assertEquals(matches.coverage, FacetCoverage.TopMatches(3))
    assertEquals(LexicalJobMatches.fromWindow(tied.take(2), 3).coverage, FacetCoverage.AllMatches)
  }

  test("HDQ-12.4 more than 20 tied skill buckets return 20 sorted by value and report truncation") {
    val skills = (1 to 25).map(n => f"Skill$n%02d").toSet
    for {
      matcher <- JobDiscoveryTestSupport.matcher(scored(1))
      fixture <- JobDiscoveryTestSupport.fixture(List(candidate), List(job(1, skills)), Some(matcher))
      result <- fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(noFilter, None, Some("scala"))).value
    } yield {
      val facets = result.toOption.get
      assertEquals(facets.skills.map(_.value), (1 to 20).map(n => f"Skill$n%02d").toList)
      assert(facets.truncated)
      assertEquals(facets.coverage, FacetCoverage.AllMatches)
    }
  }

  test("HDQ-12.5 blank, oversized and invalid-filter requests fail before the matcher or repository run") {
    val invalid = List(
      JobFacetQuery(noFilter, None, Some("   ")) -> UseCaseError.Search(SearchError.InvalidFilter("query")),
      JobFacetQuery(noFilter, None, Some("a" * 2049)) -> UseCaseError.Search(SearchError.InputTooLarge("query", 2048)),
      JobFacetQuery(noFilter.copy(city = Some(" ")), None, Some("scala")) ->
        UseCaseError.Search(SearchError.InvalidFilter("city")),
      JobFacetQuery(noFilter, Some(NearbyRadius(nicosia, 0d)), Some("scala")) ->
        UseCaseError.Search(SearchError.InvalidFilter("radius"))
    )
    for {
      matcher <- JobDiscoveryTestSupport.matcher(scored(1))
      fixture <- JobDiscoveryTestSupport.fixture(List(candidate), List(job(1)), Some(matcher))
      results <- invalid.traverse { case (query, _) => fixture.service.jobDiscoveryFacets(actor, query).value }
      matcherCalls <- matcher.calls.get
      calls <- fixture.calls.get
    } yield {
      assertEquals(results.map(_.left.toOption), invalid.map(entry => Some(entry._2)))
      assertEquals(matcherCalls, Nil)
      assertEquals(calls, JobDiscoveryTestSupport.Calls())
    }
  }

  test("HDQ-12.5 operator-like text and a 2048-character query pass to the matcher only as trimmed inert text") {
    val texts = List("""{"$ne":1}""", "\"quoted\" *wild? ", "$where: 1", "a" * 2048)
    for {
      matcher <- JobDiscoveryTestSupport.matcher(Map.empty)
      fixture <- JobDiscoveryTestSupport.fixture(List(candidate), Nil, Some(matcher))
      results <- texts.traverse(text =>
        fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(noFilter, None, Some(text))).value
      )
      matcherCalls <- matcher.calls.get
    } yield {
      assert(results.forall(_.isRight))
      assertEquals(matcherCalls.map(_._1), texts.map(_.trim))
    }
  }

  test("HDQ-12.6 recruiters, deleted candidates and unknown actors are denied before the matcher is called") {
    val deleted = candidate.copy(accountStatus = AccountStatus.Deleted)
    val denied = List(ServiceFixtures.recruiter, deleted).map(user => ActorContext(user.id, user.role)) :+
      ActorContext(UserId(new UUID(9L, 9L)), candidate.role)
    for {
      matcher <- JobDiscoveryTestSupport.matcher(scored(1))
      fixture <- JobDiscoveryTestSupport.fixture(
        List(ServiceFixtures.recruiter, deleted, ServiceFixtures.admin),
        List(job(1)),
        Some(matcher)
      )
      results <- denied.traverse(who =>
        fixture.service.jobDiscoveryFacets(who, JobFacetQuery(noFilter, None, Some("scala"))).value
      )
      admin <- fixture.service
        .jobDiscoveryFacets(
          ActorContext(ServiceFixtures.admin.id, ServiceFixtures.admin.role),
          JobFacetQuery(noFilter, None, Some("scala"))
        )
        .value
      callsAfterDenied <- matcher.calls.get
    } yield {
      assert(results.forall(_.isLeft), results.toString)
      assert(admin.isRight)
      assertEquals(callsAfterDenied.size, 1)
    }
  }

  test("HDQ-12.7 a matched job closed or deleted after indexing is excluded and a not-yet-indexed job is absent") {
    val jobs = List(job(1), job(2, status = JobStatus.Closed), job(4))
    // Job 3 was deleted (not stored); job 4 is Open but the index has not seen it yet.
    for {
      matcher <- JobDiscoveryTestSupport.matcher(scored(1, 2, 3))
      fixture <- JobDiscoveryTestSupport.fixture(List(candidate), jobs, Some(matcher))
      result <- fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(noFilter, None, Some("scala"))).value
    } yield assertEquals(result, Right(facetsOf(List(jobs.head))))
  }

  test(
    "HDQ-12.8 an unavailable matcher, a missing matcher or a failing recount give FACETS_UNAVAILABLE without fallback"
  ) {
    val unavailable = Left(UseCaseError.Search(SearchError.FacetsUnavailable))
    val request = JobFacetQuery(noFilter, None, Some("scala"))
    for {
      failingMatcher <- JobDiscoveryTestSupport.matcher(scored(1), Left(RepositoryError.Unavailable))
      withFailure <- JobDiscoveryTestSupport.fixture(List(candidate), List(job(1)), Some(failingMatcher))
      matcherDown <- withFailure.service.jobDiscoveryFacets(actor, request).value
      matcherDownCalls <- withFailure.calls.get
      withoutMatcher <- JobDiscoveryTestSupport.fixture(List(candidate), List(job(1)), None)
      notConfigured <- withoutMatcher.service.jobDiscoveryFacets(actor, request).value
      okMatcher <- JobDiscoveryTestSupport.matcher(scored(1))
      base <- JobDiscoveryTestSupport.fixture(List(candidate), List(job(1)), Some(okMatcher))
      failingRepository = new UnavailableFacets(base.jobs)
      recountService = com.example.graphQL.cats.service.TestHiringServices
        .job(base.users, failingRepository, Some(okMatcher))
      recountFailed <- recountService.jobDiscoveryFacets(actor, request).value
    } yield {
      assertEquals(matcherDown, unavailable)
      assertEquals(matcherDownCalls.facets, Nil)
      assertEquals(notConfigured, unavailable)
      assertEquals(recountFailed, unavailable)
    }
  }

  /** A matcher that spends part of the shared budget on a scripted monotonic clock. */
  private def budgetFixture(
      spent: scala.concurrent.duration.FiniteDuration,
      deadline: scala.concurrent.duration.FiniteDuration
  ) =
    for {
      nanos <- cats.effect.Ref.of[cats.effect.IO, Long](0L)
      clock = new cats.effect.kernel.Clock[cats.effect.IO] {
        def applicative = cats.Applicative[cats.effect.IO]
        def monotonic =
          nanos.get.map(scala.concurrent.duration.FiniteDuration(_, scala.concurrent.duration.NANOSECONDS))
        def realTime = cats.effect.IO.pure(scala.concurrent.duration.Duration.Zero)
      }
      spending = new com.example.graphQL.cats.service.port.LexicalJobMatcher {
        def matches(text: String, filter: JobSearchFilter, limit: Int) =
          RepositoryIO
            .lift(nanos.update(_ + spent.toNanos))
            .as(LexicalJobMatches.fromWindow(List(ScoredJobId(jobId(1), 1d)), limit))
      }
      fixture <- JobDiscoveryTestSupport.fixture(
        List(candidate),
        List(job(1)),
        Some(spending),
        deadline = Some(deadline),
        clock = clock
      )
    } yield fixture

  test("HDQ-12.8 a lexical match that consumes the whole shared budget gives FACETS_UNAVAILABLE and no recount") {
    import scala.concurrent.duration.*
    for {
      fixture <- budgetFixture(spent = 2.seconds, deadline = 2.seconds)
      result <- fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(noFilter, None, Some("scala"))).value
      calls <- fixture.calls.get
    } yield {
      assertEquals(result, Left(UseCaseError.Search(SearchError.FacetsUnavailable)))
      assertEquals(calls.facets, Nil)
    }
  }

  test("HDQ-12.8 a sub-millisecond remainder counts as exhausted so maxTimeMS is never zero") {
    import scala.concurrent.duration.*
    for {
      fixture <- budgetFixture(spent = 1999500.micros, deadline = 2.seconds)
      result <- fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(noFilter, None, Some("scala"))).value
      calls <- fixture.calls.get
    } yield {
      assertEquals(result, Left(UseCaseError.Search(SearchError.FacetsUnavailable)))
      assertEquals(calls.facets, Nil)
    }
  }

  test("HDQ-12.8 the recount receives only the time remaining after the lexical match") {
    import scala.concurrent.duration.*
    for {
      fixture <- budgetFixture(spent = 300.millis, deadline = 2.seconds)
      result <- fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(noFilter, None, Some("scala"))).value
      calls <- fixture.calls.get
    } yield {
      assert(result.isRight, result.toString)
      assertEquals(calls.facetBudgets, List(Some(1700.millis)))
    }
  }

  test("HDQ-12.9 without a query the matcher is never called and counts equal the structured facets") {
    val jobs = List(job(1), job(2, Set("Cats"), "Limassol"), job(3, status = JobStatus.Closed))
    for {
      matcher <- JobDiscoveryTestSupport.matcher(scored(1))
      fixture <- JobDiscoveryTestSupport.fixture(List(candidate), jobs, Some(matcher))
      result <- fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(noFilter, None)).value
      matcherCalls <- matcher.calls.get
      calls <- fixture.calls.get
    } yield {
      assertEquals(result, Right(facetsOf(jobs.take(2))))
      assertEquals(matcherCalls, Nil)
      assertEquals(calls.facetIds, List(None))
    }
  }

  test("HDQ-12.10 a query matching nothing gives empty buckets and runs no aggregation") {
    for {
      matcher <- JobDiscoveryTestSupport.matcher(Map.empty)
      fixture <- JobDiscoveryTestSupport.fixture(List(candidate), List(job(1)), Some(matcher))
      result <- fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(noFilter, None, Some("nothing"))).value
      calls <- fixture.calls.get
    } yield {
      assertEquals(result, Right(JobDiscoveryFacets(Nil, Nil, Nil, Nil, truncated = false)))
      assertEquals(calls.facets, Nil)
    }
  }

  test("HDQ-12.12 coverage is ALL_MATCHES for no-query, radius and few-match requests, TOP_MATCHES only when capped") {
    val jobs = (1 to 3).toList.map(n => job(n, coordinates = Some(nicosia)))
    for {
      matcher <- JobDiscoveryTestSupport.matcher(scored(1, 2, 3))
      fixture <- JobDiscoveryTestSupport.fixture(List(candidate), jobs, Some(matcher), 2)
      plain <- fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(noFilter, None)).value
      radius <- fixture.service
        .jobDiscoveryFacets(actor, JobFacetQuery(noFilter, Some(NearbyRadius(nicosia, 10d))))
        .value
      capped <- fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(noFilter, None, Some("scala"))).value
      fewMatcher <- JobDiscoveryTestSupport.matcher(scored(1))
      few <- JobDiscoveryTestSupport.fixture(List(candidate), jobs, Some(fewMatcher), 2)
      fewResult <- few.service.jobDiscoveryFacets(actor, JobFacetQuery(noFilter, None, Some("scala"))).value
    } yield {
      assertEquals(plain.map(_.coverage), Right(FacetCoverage.AllMatches))
      assertEquals(radius.map(_.coverage), Right(FacetCoverage.AllMatches))
      assertEquals(fewResult.map(_.coverage), Right(FacetCoverage.AllMatches))
      assertEquals(capped.map(_.coverage), Right(FacetCoverage.TopMatches(2)))
    }
  }

  test("HDQ-12.13 an Open matching job without an embedding is counted and a Closed one is not") {
    val jobs = List(job(1), job(2, status = JobStatus.Closed))
    assert(jobs.forall(_.embedding.isEmpty))
    for {
      matcher <- JobDiscoveryTestSupport.matcher(scored(1, 2))
      fixture <- JobDiscoveryTestSupport.fixture(List(candidate), jobs, Some(matcher))
      result <- fixture.service.jobDiscoveryFacets(actor, JobFacetQuery(noFilter, None, Some("scala"))).value
    } yield assertEquals(result.map(_.skills), Right(List(JobFacetBucket("Scala", 1L))))
  }

  /** Recount failure: the operational store is down after the matcher succeeded. */
  private final class UnavailableFacets(delegate: JobRepository) extends JobRepository {
    export delegate.{
      relatedJobs,
      find,
      findVersioned,
      findSubmissionSnapshot,
      findOpen,
      findAll,
      findByRecruiter,
      createWithEvents,
      updateWithEvents,
      updateEmbedding,
      nearbyJobs
    }
    override def jobDiscoveryFacets(
        scope: HiringReadScope,
        query: JobFacetQuery,
        matchedIds: Option[Set[JobId]],
        maxTime: Option[scala.concurrent.duration.FiniteDuration]
    ): RepositoryIO[JobDiscoveryFacets] = RepositoryIO.raise(RepositoryError.Unavailable)
  }
}
