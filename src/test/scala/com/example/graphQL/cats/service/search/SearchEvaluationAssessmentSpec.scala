package com.example.graphQL.cats.service.search

import java.time.Instant
import com.example.graphQL.cats.infrastructure.search.SearchEvaluationReportJson

class SearchEvaluationAssessmentSpec extends munit.FunSuite {
  import SearchEvaluationAssessmentReason.*
  private val time = Instant.parse("2026-10-06T12:00:00Z")
  private val full = SearchEvaluationFixtures.corpus
  private val corpus = full.copy(
    rubric = full.rubric.copy(origin = SearchEvaluationJudgmentOrigin.HumanReviewedFabricated, approved = true),
    queries = full.queries
      .filter(_.useCase == SearchEvaluationUseCase.JobSearch)
      .map(_.copy(labelReview = SearchEvaluationLabelReview.Reviewed))
  )
  private val run = SearchEvaluationFixtures.run(SearchEvaluationStrategy.Vector, "test", "fingerprint", time)
  private val observed = run.copy(
    coordinates = run.coordinates.copy(
      rankingOrigin = SearchEvaluationRankingOrigin.ObservedApplication,
      embeddingProvenance = SearchEvaluationEmbeddingProvenance.ProviderGenerated
    ),
    environment = run.environment.copy(providerRequests = Some(2L)),
    queries = corpus.queries.map(q =>
      SearchEvaluationQuery(
        q.queryId,
        SearchEvaluationRanking.Succeeded(q.relevantIds.toList.sorted, Some(10.0)),
        SearchEvaluationRanking.Unavailable(SearchEvaluationUnavailable.NotCaptured)
      )
    )
  )
  private def report(value: SearchEvaluationRun = observed) =
    SearchEvaluationHarness.report(corpus, value, 2).fold(e => fail(e.toString), identity)
  private val policy = SearchEvaluationAssessmentPolicy(
    "conservative",
    SearchEvaluationPolicyAgreement.Agreed,
    Map(SearchEvaluationUseCase.JobSearch -> SearchEvaluationUseCasePolicy(Some(BigDecimal(1))))
  )
  private val labels = Some(
    SearchEvaluationLabelAttestation(corpus.digest, corpus.rubric.identity, "independent-reviewer")
  )
  private val evidence = SearchEvaluationObservedEvidence(
    corpus,
    observed,
    "authenticated-live-probes",
    SearchEvaluationEvidenceOutcome.Passed,
    SearchEvaluationEvidenceOutcome.Passed,
    Some("same-workload-billing"),
    Some("USD"),
    Map(SearchEvaluationUseCase.JobSearch -> BigDecimal("0.1"))
  )
  private def assess(
      candidate: SearchEvaluationReport = report(),
      p: SearchEvaluationAssessmentPolicy = policy,
      label: Option[SearchEvaluationLabelAttestation] = labels,
      actual: Option[SearchEvaluationObservedEvidence] = Some(evidence)
  ) =
    SearchEvaluationAssessment
      .assess(
        report(),
        candidate,
        p,
        label,
        Some(evidence),
        actual.map(_.copy(corpus = candidate.corpus, run = candidate.run))
      )
      .fold(e => fail(e.toString), identity)

  test("observed reviewed held-out paired evidence recommends without changing runtime ranking") {
    val result = assess()
    assertEquals(result.decision, SearchEvaluationDecision.Recommend)
    assertEquals(result.reasons, Nil)
    val json = SearchEvaluationReportJson.render(report().copy(assessment = Some(result)))
    assertEquals(json.hcursor.downField("acceptance").get[String]("adoption"), Right("Recommend"))
  }
  test("proposed policy and missing or digest-mismatched review attestation defer") {
    assert(
      assess(p = policy.copy(agreement = SearchEvaluationPolicyAgreement.Proposed)).reasons.contains(PolicyProposed)
    )
    assertEquals(assess(label = None).decision, SearchEvaluationDecision.Defer)
    assert(assess(label = labels.map(_.copy(corpusDigest = "other"))).reasons.contains(MissingLabelAttestation))
  }
  test("privacy and eligibility failures reject independently of result identifiers") {
    assertEquals(
      assess(actual = Some(evidence.copy(privacy = SearchEvaluationEvidenceOutcome.Failed))).decision,
      SearchEvaluationDecision.Reject
    )
    assert(
      assess(actual = Some(evidence.copy(eligibility = SearchEvaluationEvidenceOutcome.Failed))).reasons
        .contains(EligibilityFailure)
    )
  }
  test("missing observations billing and provider counts defer rather than impersonate cost") {
    assert(assess(actual = None).reasons.contains(MissingObservedEvidence))
    assert(assess(actual = Some(evidence.copy(billingIdentity = None))).reasons.contains(MissingComparableBilling))
    val missing = report(observed.copy(environment = observed.environment.copy(providerRequests = None)))
    assert(assess(missing).reasons.contains(MissingProviderMeasurements))
  }
  test("latency relevance cost and provider regressions reject") {
    val slower = report(
      observed.copy(queries =
        observed.queries.map(_.copy(ranking = SearchEvaluationRanking.Succeeded(Nil, Some(20.0))))
      )
    )
    assert(assess(slower).reasons.contains(LatencyRegression))
    assert(assess(slower).reasons.contains(RelevanceRegression))
    assert(
      assess(actual =
        Some(evidence.copy(useCaseCosts = Map(SearchEvaluationUseCase.JobSearch -> BigDecimal(2))))
      ).reasons.contains(CostLimitExceeded)
    )
    val expensive = report(observed.copy(environment = observed.environment.copy(providerRequests = Some(3L))))
    assert(assess(expensive).reasons.contains(ProviderRequestRegression))
  }
  test("failed unavailable and missing held-out groups cannot pass coverage") {
    val unavailable = report(
      observed.copy(queries =
        observed.queries.map(
          _.copy(ranking = SearchEvaluationRanking.Unavailable(SearchEvaluationUnavailable.NotCaptured))
        )
      )
    )
    assert(assess(unavailable).reasons.contains(IncompleteCoverage))
    assert(
      SearchEvaluationAssessment
        .assess(report(), report().copy(groups = Nil), policy, labels, Some(evidence), Some(evidence))
        .isLeft
    )
    val failed = report(
      observed.copy(queries =
        observed.queries.map(
          _.copy(ranking = SearchEvaluationRanking.Failed(SearchEvaluationFailure.RetrievalFailed, None))
        )
      )
    )
    assert(assess(failed).reasons.contains(RetrievalFailure))
  }
  test("tuning failure rejects and forged summary is a typed error") {
    val tuningIds = corpus.queries.filter(_.split == SearchEvaluationSplit.Tuning).map(_.queryId).toSet
    val failed = report(
      observed.copy(queries =
        observed.queries.map(q =>
          if (tuningIds.contains(q.queryId))
            q.copy(ranking = SearchEvaluationRanking.Failed(SearchEvaluationFailure.RetrievalFailed, None))
          else q
        )
      )
    )
    assert(assess(failed).reasons.contains(RetrievalFailure))
    val forged = report().copy(summary = report().summary.copy(failed = 42))
    assert(SearchEvaluationAssessment.assess(report(), forged, policy, labels, Some(evidence), Some(evidence)).isLeft)
  }
  test("synthetic embedding provenance and authored observations defer") {
    val fixture = report(
      observed.copy(coordinates =
        observed.coordinates.copy(
          embeddingProvenance = SearchEvaluationEmbeddingProvenance.SyntheticFixture,
          rankingOrigin = SearchEvaluationRankingOrigin.AuthoredFixture
        )
      )
    )
    val result = SearchEvaluationAssessment
      .assess(
        fixture,
        fixture,
        policy,
        labels,
        Some(evidence.copy(run = fixture.run)),
        Some(evidence.copy(run = fixture.run))
      )
      .toOption
    assert(result.exists(_.reasons.contains(FixtureEmbeddings)))
    assert(result.exists(_.reasons.contains(FixtureRankings)))
  }
  test("selected job-search recommendation excludes unchanged unsupported recommendations explicitly") {
    val mixedCorpus = full.copy(
      rubric = corpus.rubric,
      queries = full.queries.map(_.copy(labelReview = SearchEvaluationLabelReview.Reviewed))
    )
    val captured = observed.copy(
      strategy = SearchEvaluationStrategy.ApplicationRrf,
      queries = mixedCorpus.queries.map(q =>
        SearchEvaluationQuery(
          q.queryId,
          if (q.useCase == SearchEvaluationUseCase.Recommendations)
            SearchEvaluationRanking.Unavailable(SearchEvaluationUnavailable.UnsupportedUseCase)
          else SearchEvaluationRanking.Succeeded(q.relevantIds.toList.sorted, Some(10.0)),
          SearchEvaluationRanking.Unavailable(SearchEvaluationUnavailable.NotCaptured)
        )
      )
    )
    val measured = SearchEvaluationHarness.report(mixedCorpus, captured, 2).fold(e => fail(e.toString), identity)
    val scopedEvidence = evidence.copy(corpus = mixedCorpus, run = captured)
    val selected = SearchEvaluationAssessment
      .assess(measured, measured, policy, labels, Some(scopedEvidence), Some(scopedEvidence))
      .fold(e => fail(e.toString), identity)
    assertEquals(selected.decision, SearchEvaluationDecision.Recommend)
    assertEquals(selected.evaluatedUseCases, List(SearchEvaluationUseCase.JobSearch))
    val allPolicy =
      policy.copy(useCases = SearchEvaluationUseCase.values.map(_ -> SearchEvaluationUseCasePolicy()).toMap)
    val allEvidence =
      scopedEvidence.copy(useCaseCosts = SearchEvaluationUseCase.values.map(_ -> BigDecimal("0.1")).toMap)
    val all = SearchEvaluationAssessment
      .assess(measured, measured, allPolicy, labels, Some(allEvidence), Some(allEvidence))
      .fold(e => fail(e.toString), identity)
    assertEquals(all.decision, SearchEvaluationDecision.Defer)
    assert(all.reasons.contains(IncompleteCoverage))
  }
  test("evidence binds corpus strategy workload and exact observations") {
    val altered = List(
      evidence.copy(corpus = corpus.copy(digest = "other")),
      evidence.copy(corpus =
        corpus.copy(queries = corpus.queries.map(_.copy(labelReview = SearchEvaluationLabelReview.Pending)))
      ),
      evidence.copy(run = observed.copy(strategy = SearchEvaluationStrategy.Lexical)),
      evidence.copy(run = observed.copy(warmupQueries = observed.warmupQueries + 1)),
      evidence.copy(run =
        observed.copy(queries =
          observed.queries.map(_.copy(ranking = SearchEvaluationRanking.Succeeded(Nil, Some(11.0))))
        )
      )
    )
    altered.foreach(e =>
      assert(SearchEvaluationAssessment.assess(report(), report(), policy, labels, Some(evidence), Some(e)).isLeft)
    )
  }
  test("proposed policy cannot hide failed observed reliability") {
    val failed = report(
      observed.copy(queries =
        observed.queries.map(
          _.copy(ranking = SearchEvaluationRanking.Failed(SearchEvaluationFailure.RetrievalFailed, None))
        )
      )
    )
    val result = assess(failed, p = policy.copy(agreement = SearchEvaluationPolicyAgreement.Proposed))
    assertEquals(result.decision, SearchEvaluationDecision.Reject)
    assertEquals(
      SearchEvaluationReportJson
        .render(failed.copy(assessment = Some(result)))
        .hcursor
        .downField("acceptance")
        .get[String]("reliabilityGate"),
      Right("Failed")
    )
  }
  test("invalid policy and mismatched evidence fail with typed errors") {
    assert(
      SearchEvaluationAssessment
        .assess(report(), report(), policy.copy(useCases = Map.empty), labels, Some(evidence), Some(evidence))
        .isLeft
    )
    assert(
      SearchEvaluationAssessment
        .assess(
          report(),
          report(),
          policy,
          labels,
          Some(evidence),
          Some(evidence.copy(run = observed.copy(coordinates = observed.coordinates.copy(sourceFingerprint = "other"))))
        )
        .isLeft
    )
    assert(
      SearchEvaluationAssessment
        .assess(report(), report().copy(k = 1), policy, labels, Some(evidence), Some(evidence))
        .isLeft
    )
  }
}
