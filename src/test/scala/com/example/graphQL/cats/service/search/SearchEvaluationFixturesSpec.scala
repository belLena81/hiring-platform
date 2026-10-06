package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.infrastructure.search.SearchEvaluationReportJson
import java.time.Instant

class SearchEvaluationFixturesSpec extends munit.FunSuite {
  private val timestamp = Instant.parse("2026-10-06T12:00:00Z")
  private def run(strategy: SearchEvaluationStrategy): SearchEvaluationRun =
    SearchEvaluationFixtures.run(strategy, "explicit-test-snapshot", "test-source-fingerprint", timestamp)
  private def report(strategy: SearchEvaluationStrategy): SearchEvaluationReport =
    SearchEvaluationHarness
      .report(SearchEvaluationFixtures.corpus, run(strategy), SearchEvaluationFixtures.K)
      .fold(errors => fail(errors.toString), identity)

  test("bounded provisional corpus has eight jobs, eight candidates and six tuning/six held-out queries") {
    val corpus = SearchEvaluationFixtures.corpus
    assertEquals(SearchEvaluationFixtures.jobs.size, 8)
    assertEquals(SearchEvaluationFixtures.candidates.size, 8)
    assertEquals(corpus.entityIds.distinct.size, 16)
    assertEquals(corpus.queries.size, 12)
    assertEquals(corpus.queries.count(_.split == SearchEvaluationSplit.Tuning), 6)
    assertEquals(corpus.queries.count(_.split == SearchEvaluationSplit.HeldOut), 6)
    assertEquals(corpus.rubric.origin, SearchEvaluationJudgmentOrigin.ProvisionalFabricated)
    assert(corpus.rubric.approved)
    assert(corpus.queries.forall(_.labelReview == SearchEvaluationLabelReview.Pending))
    corpus.queries.foreach { query =>
      val intent =
        SearchEvaluationFixtures.intents.find(_.queryId == query.queryId).getOrElse(fail("Missing query intent"))
      val entities =
        if (query.useCase == SearchEvaluationUseCase.RecruiterMatching) SearchEvaluationFixtures.candidates
        else SearchEvaluationFixtures.jobs
      val expected = entities
        .filter(entity =>
          query.eligibleIds.contains(entity.id) &&
            entity.intendedRole == intent.intendedRole && intent.requiredSkills.subsetOf(entity.skills)
        )
        .map(_.id)
        .toSet
      assertEquals(query.relevantIds, expected)
      assertEquals(intent.relevanceRationales.keySet, query.eligibleIds.toSet)
    }
    assert(SearchEvaluationFixtures.candidates.exists(_.optIn.contains(false)))
    assert(SearchEvaluationFixtures.candidates.exists(_.optIn.isEmpty))
    assert(
      SearchEvaluationFixtures.candidates.exists(value => value.optIn.contains(true) && !value.privateAttributesPresent)
    )
  }

  test("authored branch replay exercises actual RRF ties and excludes stale/deleted/closed fixtures") {
    val fused = report(SearchEvaluationStrategy.ApplicationRrf)
    val first = fused.queries.find(_.fixture.queryId == "JobSearch-0").map(_.observation.ranking)
    assertEquals(
      first,
      Some(SearchEvaluationRanking.Succeeded(List(5, 1, 4, 2, 3).map(SearchEvaluationFixtures.jobId), None))
    )
    assert(
      fused.queries.forall(value =>
        value.observation.ranking match {
          case SearchEvaluationRanking.Succeeded(ids, _) => ids.forall(value.fixture.eligibleIds.contains)
          case _                                         => true
        }
      )
    )
    assertEquals(fused.summary.successful, 8)
    assertEquals(fused.summary.unavailable, 4)
    assertEquals(fused.summary.fidelityDenominator, 0)
    assertEquals(fused.summary.successLatency, None)
    assertEquals(fused.summary.attemptedThroughput, None)
  }

  test("recommendations remain vector-only and typed validation rejects forged successful lexical results") {
    val vector = report(SearchEvaluationStrategy.Vector)
    assertEquals(vector.summary.successful, 12)
    val lexical = run(SearchEvaluationStrategy.Lexical)
    val forged = lexical.copy(queries =
      lexical.queries.map(value =>
        if (value.queryId.startsWith("Recommendations"))
          value.copy(ranking = SearchEvaluationRanking.Succeeded(Nil, None))
        else value
      )
    )
    assert(SearchEvaluationHarness.report(SearchEvaluationFixtures.corpus, forged, SearchEvaluationFixtures.K).isLeft)
    assertEquals(SearchEvaluationHarness.compare(vector, report(SearchEvaluationStrategy.ApplicationRrf)), Right(()))
  }

  test(
    "renderer exposes provisional labels, unavailable telemetry and deferred reliability without fixture source text"
  ) {
    val json = SearchEvaluationReportJson.render(report(SearchEvaluationStrategy.Vector))
    assertEquals(json.hcursor.downField("corpus").get[String]("judgmentOrigin").toOption, Some("ProvisionalFabricated"))
    assertEquals(json.hcursor.downField("workload").get[Int]("k").toOption, Some(7))
    assertEquals(json.hcursor.downField("environment").get[Long]("providerRequests").toOption, Some(0L))
    assertEquals(
      json.hcursor.downField("acceptance").get[String]("reliabilityGate").toOption,
      Some("NotAssessableThresholdNotAgreed")
    )
    assert(!json.noSpaces.contains("Built functional service APIs"))
    assert(!json.noSpaces.contains("privateAttributesPresent"))
    assert(!json.noSpaces.contains("experienceSummary"))
    assertEquals(
      json.hcursor.downField("measurements").downField("meanRecallAtKAgainstExact").focus,
      Some(io.circe.Json.Null)
    )
    val reviewed = SearchEvaluationFixtures.corpus.copy(rubric =
      SearchEvaluationFixtures.rubric.copy(origin = SearchEvaluationJudgmentOrigin.HumanReviewedFabricated)
    )
    assert(SearchEvaluationHarness.report(reviewed, run(SearchEvaluationStrategy.Vector), 7).isLeft)
  }
}
