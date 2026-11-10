package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.domain.search.RankingBasis
import com.example.graphQL.cats.service.search.SkillEvidenceRankedEvaluation.RankerOutcome
import munit.CatsEffectSuite
import java.time.Instant
import scala.util.Random

/** HDQ-07.6 (replay), HDQ-07.11 and the paired assessment support for the skill-evidence ranker. */
final class SkillEvidenceRankedEvaluationSpec extends CatsEffectSuite {
  private val timestamp = Instant.parse("2026-11-10T00:00:00Z")
  private val Retrieval = SearchEvaluationStrategy.Vector
  private val Ranked = SearchEvaluationStrategy.SkillEvidenceRanked(Retrieval)

  private def baselineRun(strategy: SearchEvaluationStrategy = Retrieval): SearchEvaluationRun =
    SearchEvaluationFixtures.run(strategy, "revision", "fingerprint", timestamp)

  private def report(run: SearchEvaluationRun): SearchEvaluationReport =
    SearchEvaluationHarness
      .report(SearchEvaluationFixtures.corpus, run, SearchEvaluationFixtures.K)
      .fold(errors => fail(s"invalid run: $errors"), identity)

  private def rankedRun(
      captured: SearchEvaluationRun,
      timer: SkillEvidenceRankedEvaluation.RankerTimer = SkillEvidenceRankedEvaluation.RankerTimer.live,
      arrange: List[com.example.graphQL.cats.domain.search.RankingHit[String]] => List[
        com.example.graphQL.cats.domain.search.RankingHit[String]
      ] = identity
  ): SkillEvidenceRankedEvaluation.RankedRun =
    SkillEvidenceRankedEvaluation
      .rankedRun(captured, SearchEvaluationFixtures.K, timer = timer, arrange = arrange)
      .fold(errors => fail(s"ranked run refused: $errors"), identity)

  private val policy = SearchEvaluationAssessmentPolicy(
    "conservative-no-regression-20261006",
    SearchEvaluationPolicyAgreement.Agreed,
    Map(
      SearchEvaluationUseCase.Recommendations -> SearchEvaluationUseCasePolicy(),
      SearchEvaluationUseCase.RecruiterMatching -> SearchEvaluationUseCasePolicy()
    ),
    latencyNoiseAllowanceMillis = 0.0,
    absoluteP95Budget = None
  )

  test("HDQ-07.6 replay of the same captured results through the ranker twice gives identical reports") {
    val captured = baselineRun()
    val first = report(rankedRun(captured).run)
    val second = report(rankedRun(captured).run)
    assertEquals(first, second)
    assertEquals(first.run.strategy, Ranked)
  }

  test("HDQ-07.6 replay over shuffled input order, with ranks fixed, gives the report of the unshuffled replay") {
    val captured = baselineRun()
    val expected = report(rankedRun(captured).run)
    (1L to 25L).foreach { seed =>
      val random = new Random(seed)
      val shuffled = report(rankedRun(captured, arrange = random.shuffle(_)).run)
      assertEquals(shuffled, expected, clue(seed))
    }
  }

  test("HDQ-07.6 replay at concurrency 1 and 8 gives identical orderings") {
    for {
      one <- SearchEvaluationFixtureReplay.captureAuthoredRun(Ranked, "revision", "fingerprint", timestamp, 1)
      eight <- SearchEvaluationFixtureReplay.captureAuthoredRun(Ranked, "revision", "fingerprint", timestamp, 8)
      viaBaseline <- SearchEvaluationFixtureReplay
        .captureAuthoredRun(Retrieval, "revision", "fingerprint", timestamp, 8)
        .map(rankedRun(_).run)
    } yield {
      assertEquals(one.queries, eight.queries)
      assertEquals(eight.queries, viaBaseline.queries)
      assertEquals(one.strategy, Ranked)
    }
  }

  test("HDQ-07.11 the record names the ranker, its tie rule, the underlying retrieval and the basis counts") {
    val ranked = rankedRun(baselineRun())
    val record = SkillEvidenceRankedEvaluation.record(ranked)
    assertEquals(record.ranker, "skill-evidence-rrf-k60")
    assertEquals(
      record.tieRule,
      "skill-evidence RRF k=60 over retrieval rank and coverage competition rank; then retrieval rank ascending"
    )
    assertEquals(record.underlyingRetrieval, Retrieval)
    // Recommendations and RecruiterMatching each have four queries: one with an empty eligible set (no evidence,
    // retrieval order) and three with skill evidence. JobSearch (four queries) is outside the ranker.
    assertEquals(record.retrievalOrderQueries, 2)
    assertEquals(record.skillEvidenceQueries, 6)
    assertEquals(record.fallbackQueries, 0)
    assertEquals(record.outOfScopeQueries, 4)
    val json = SkillEvidenceRankedEvaluation.renderReport(report(ranked.run), record)
    val ranker = json.hcursor.downField("skillEvidenceRanker")
    assertEquals(ranker.get[String]("tieRule"), Right(record.tieRule))
    assertEquals(ranker.get[String]("underlyingRetrieval"), Right("Vector"))
    assertEquals(ranker.get[Int]("retrievalOrderQueries"), Right(2))
    assertEquals(json.hcursor.downField("workload").get[String]("tieRule"), Right(record.tieRule))
    assertEquals(json.hcursor.get[String]("strategy"), Right("SkillEvidenceRanked(Vector)"))
  }

  test("HDQ-07.11 a query whose retrieval returned nothing is recorded as retrieval order, not as an applied ranking") {
    val empty = SearchEvaluationFixtures.queries.find(_.filterGroup == SearchEvaluationFilterGroup.Empty).get
    val outcome = rankedRun(baselineRun()).outcomes
    val recommendation = SearchEvaluationFixtures.queries
      .filter(q => q.filterGroup == SearchEvaluationFilterGroup.Empty && q.useCase != SearchEvaluationUseCase.JobSearch)
    assertEquals(empty.eligibleIds, Nil)
    recommendation.foreach(q => assertEquals(outcome(q.queryId), RankerOutcome.Applied(RankingBasis.RetrievalOrder)))
    assertEquals(outcome("JobSearch-0"), RankerOutcome.OutOfScope)
  }

  test("HDQ-07.11 the ranked report pairs with its own Vector baseline and with no other retrieval") {
    val baseline = report(baselineRun())
    val ranked = report(rankedRun(baselineRun()).run)
    assertEquals(SearchEvaluationHarness.compare(baseline, ranked), Right(()))
    val rrfBaseline = report(baselineRun(SearchEvaluationStrategy.ApplicationRrf))
    assert(SearchEvaluationHarness.compare(rrfBaseline, ranked).isLeft)
    val forgedTieRule =
      ranked.copy(run = ranked.run.copy(coordinates = ranked.run.coordinates.copy(tieRule = "RRF score descending")))
    assert(SearchEvaluationHarness.compare(baseline, forgedTieRule).isLeft)
  }

  test("HDQ-07.11 the assessment counts a ranked Recommendations query as supported exactly where its retrieval is") {
    val baseline = report(baselineRun())
    val ranked = report(rankedRun(baselineRun()).run)
    val result = SearchEvaluationAssessment
      .assess(baseline, ranked, policy, None, None, None)
      .fold(errors => fail(s"assessment refused: $errors"), identity)
    assertEquals(
      result.evaluatedUseCases,
      List(SearchEvaluationUseCase.Recommendations, SearchEvaluationUseCase.RecruiterMatching)
    )
    // Authored fixtures can never be recommended; the ranker must add no coverage, retrieval or relevance failure.
    assertNotEquals(result.decision, SearchEvaluationDecision.Recommend)
    val reasons = result.reasons.toSet
    assert(!reasons(SearchEvaluationAssessmentReason.IncompleteCoverage), clue(result))
    assert(!reasons(SearchEvaluationAssessmentReason.RetrievalFailure), clue(result))
    assert(!reasons(SearchEvaluationAssessmentReason.RelevanceRegression), clue(result))

    // Under ApplicationRrf Recommendations is unsupported, so a ranked ApplicationRrf run carries it as unavailable.
    val rrfBaseline = baselineRun(SearchEvaluationStrategy.ApplicationRrf)
    val rrfRanked = rankedRun(rrfBaseline)
    val unavailable = rrfRanked.run.queries.filter(_.queryId.startsWith("Recommendations"))
    assert(unavailable.nonEmpty)
    assert(
      unavailable.forall(
        _.ranking == SearchEvaluationRanking.Unavailable(SearchEvaluationUnavailable.UnsupportedUseCase)
      )
    )
    assert(
      SearchEvaluationHarness.report(SearchEvaluationFixtures.corpus, rrfRanked.run, SearchEvaluationFixtures.K).isRight
    )
  }

  test("HDQ-07.11 the ranker on a held-out query promotes the intended match without losing recall") {
    val baseline = report(baselineRun())
    val ranked = report(rankedRun(baselineRun()).run)
    def quality(r: SearchEvaluationReport, id: String) =
      r.queries.find(_.fixture.queryId == id).flatMap(_.relevance).get
    List("Recommendations-2", "RecruiterMatching-2", "RecruiterMatching-3").foreach { id =>
      assertEquals(quality(ranked, id).recallAtK, quality(baseline, id).recallAtK, clue(id))
      assert(quality(ranked, id).ndcgAtK >= quality(baseline, id).ndcgAtK, clue(id))
    }
    assert(quality(ranked, "RecruiterMatching-2").ndcgAtK > quality(baseline, "RecruiterMatching-2").ndcgAtK)
    // JobSearch passes through untouched.
    assertEquals(
      ranked.run.queries.filter(_.queryId.startsWith("JobSearch")),
      baseline.run.queries.filter(_.queryId.startsWith("JobSearch"))
    )
  }

  private def inScope(id: String): Boolean = !id.startsWith("JobSearch")

  private def timedBaseline(millis: Double): SearchEvaluationRun = {
    val captured = baselineRun()
    captured.copy(queries =
      captured.queries.map(q =>
        q.copy(ranking = q.ranking match {
          case SearchEvaluationRanking.Succeeded(ids, _) => SearchEvaluationRanking.Succeeded(ids, Some(millis))
          case other                                     => other
        })
      )
    )
  }

  private def latencyOf(q: SearchEvaluationQuery): Option[Double] = q.ranking match {
    case SearchEvaluationRanking.Succeeded(_, latency) => latency
    case _                                             => None
  }

  private def assessed(rankerMillis: Double, allowance: Double) = {
    val baselineRunValue = timedBaseline(10.0)
    val ranked = rankedRun(baselineRunValue, SkillEvidenceRankedEvaluation.RankerTimer.fixed(rankerMillis))
    SearchEvaluationAssessment
      .assess(
        report(baselineRunValue),
        report(ranked.run),
        policy.copy(latencyNoiseAllowanceMillis = allowance),
        None,
        None,
        None
      )
      .fold(errors => fail(s"assessment refused: $errors"), identity)
  }

  test(
    "HDQ-07.11 the ranked latency is the baseline latency plus the injected ranker time, only where the ranker ran"
  ) {
    val baseline = timedBaseline(10.0)
    val ranked = rankedRun(baseline, SkillEvidenceRankedEvaluation.RankerTimer.fixed(2.5))
    ranked.run.queries.foreach { q =>
      assertEquals(latencyOf(q), Some(if (inScope(q.queryId)) 12.5 else 10.0), clue(q.queryId))
    }
    // An absent baseline latency is never invented.
    val untimed = rankedRun(baselineRun(), SkillEvidenceRankedEvaluation.RankerTimer.fixed(2.5))
    assert(untimed.run.queries.forall(q => latencyOf(q).isEmpty))
  }

  test("HDQ-07.11 the live timer adds a non-negative measured ranker time") {
    val ranked = rankedRun(timedBaseline(10.0))
    ranked.run.queries.foreach(q => assert(latencyOf(q).exists(_ >= 10.0), clue(q.queryId)))
  }

  test("HDQ-08 a ranker time above the noise allowance is a LatencyRegression and rejects") {
    val result = assessed(rankerMillis = 6.0, allowance = 5.0)
    assert(result.reasons.contains(SearchEvaluationAssessmentReason.LatencyRegression), clue(result))
    assertEquals(result.decision, SearchEvaluationDecision.Reject)
  }

  test("HDQ-08 a ranker time within the noise allowance is no LatencyRegression") {
    val result = assessed(rankerMillis = 5.0, allowance = 5.0)
    assert(!result.reasons.contains(SearchEvaluationAssessmentReason.LatencyRegression), clue(result))
  }

  private def withCapturedIds(captured: SearchEvaluationRun, queryId: String, ids: List[String]) =
    captured.copy(queries =
      captured.queries.map(q =>
        if (q.queryId == queryId) q.copy(ranking = SearchEvaluationRanking.Succeeded(ids, None)) else q
      )
    )

  test("HDQ-07.11 a ranked query whose captured list is exactly the capture bound is rejected as possibly truncated") {
    val ids = SearchEvaluationFixtures.corpus.entityIds.take(SearchEvaluationFixtures.K)
    assertEquals(ids.size, SearchEvaluationFixtures.K)
    val captured = withCapturedIds(baselineRun(), "Recommendations-0", ids)
    val result = SkillEvidenceRankedEvaluation.rankedRun(captured, SearchEvaluationFixtures.K)
    assertEquals(
      result.left.map(_.toNonEmptyList.toList),
      Left(List(SearchEvaluationError.PossiblyTruncatedCapture("Recommendations-0")))
    )
  }

  test("HDQ-07.11 a captured list shorter than the capture bound, or a bound reaching the ranker bound, is accepted") {
    val ids = SearchEvaluationFixtures.corpus.entityIds.take(SearchEvaluationFixtures.K)
    val shorter = withCapturedIds(baselineRun(), "Recommendations-0", ids.take(SearchEvaluationFixtures.K - 1))
    assert(SkillEvidenceRankedEvaluation.rankedRun(shorter, SearchEvaluationFixtures.K).isRight)
    val full = withCapturedIds(baselineRun(), "Recommendations-0", ids)
    assert(
      SkillEvidenceRankedEvaluation
        .rankedRun(full, com.example.graphQL.cats.domain.pagination.PageSize.Max)
        .isRight
    )
  }

  test("HDQ-07.11 a JobSearch capture of exactly the bound is outside the ranker and not rejected") {
    val ids = SearchEvaluationFixtures.corpus.entityIds.take(SearchEvaluationFixtures.K)
    val captured = withCapturedIds(baselineRun(), "JobSearch-0", ids)
    assert(SkillEvidenceRankedEvaluation.rankedRun(captured, SearchEvaluationFixtures.K).isRight)
  }

  test("HDQ-07.11 a nested ranked strategy is an invalid run") {
    val nested = SearchEvaluationStrategy.SkillEvidenceRanked(Ranked)
    val run = rankedRun(baselineRun()).run.copy(strategy = nested)
    val result = SearchEvaluationHarness.report(SearchEvaluationFixtures.corpus, run, SearchEvaluationFixtures.K)
    assert(result.left.exists(_.toNonEmptyList.toList.contains(SearchEvaluationError.InvalidField("strategy"))))
  }

  test("HDQ-07.11 a ranked run carrying the retrieval's tie rule instead of the ranker's does not pair") {
    val baseline = report(baselineRun())
    val ranked = rankedRun(baselineRun()).run
    val borrowed = ranked.copy(coordinates = ranked.coordinates.copy(tieRule = baseline.run.coordinates.tieRule))
    assert(SearchEvaluationHarness.compare(baseline, report(borrowed)).isLeft)
  }
}
