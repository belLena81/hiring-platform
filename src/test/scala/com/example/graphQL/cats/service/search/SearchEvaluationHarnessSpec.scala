package com.example.graphQL.cats.service.search

import com.example.graphQL.cats.infrastructure.search.SearchEvaluationReportJson
import java.time.Instant

class SearchEvaluationHarnessSpec extends munit.FunSuite {
  import SearchEvaluationRanking.*
  private val timestamp = Instant.parse("2026-10-06T12:00:00Z")
  private val full = SearchEvaluationFixtures.corpus
  private val corpus = full.copy(queries = full.queries.filter(_.useCase == SearchEvaluationUseCase.JobSearch).take(3))
  private val base = SearchEvaluationFixtures.run(
    SearchEvaluationStrategy.Vector,
    "explicit-test-snapshot",
    "test-source-fingerprint",
    timestamp
  )
  private def run(observations: List[SearchEvaluationQuery]): SearchEvaluationRun =
    base.copy(queries = observations, durationMillis = Some(2000L))
  private val queries = corpus.queries.map(_.queryId)
  private val ids = (1 to 5).map(SearchEvaluationFixtures.jobId).toList
  private val good = List(
    SearchEvaluationQuery(
      queries(0),
      Succeeded(List(ids(0), ids(1)), Some(10.0)),
      Succeeded(List(ids(0), ids(4)), Some(12.0))
    ),
    SearchEvaluationQuery(
      queries(1),
      Failed(SearchEvaluationFailure.RetrievalFailed, Some(30.0)),
      Unavailable(SearchEvaluationUnavailable.NotCaptured)
    ),
    SearchEvaluationQuery(
      queries(2),
      Succeeded(List(ids(3)), Some(20.0)),
      Failed(SearchEvaluationFailure.ReferenceFailed, Some(35.0))
    )
  )
  private def evaluated(observations: List[SearchEvaluationQuery]): SearchEvaluationReport =
    SearchEvaluationHarness.report(corpus, run(observations), 2).fold(errors => fail(errors.toString), identity)

  test("successful-only relevance discloses its denominator and exact failures do not erase relevance") {
    val report = evaluated(good)
    assertEquals(report.summary.attempted, 3)
    assertEquals(report.summary.successful, 2)
    assertEquals(report.summary.failed, 1)
    assertEquals(report.summary.relevanceDenominator, 2)
    assertEquals(report.summary.fidelityDenominator, 1)
    assertEquals(report.summary.exactReferenceFailures, 1)
    assertEquals(report.summary.meanRelevance.map(_.recallAtK), Some(0.75))
    assertEquals(report.summary.meanFidelityRecallAtK, Some(0.5))
    assertEquals(report.summary.attemptedThroughput, Some(1.5))
    assertEquals(report.summary.successfulThroughput, Some(1.0))
    assertEquals(report.summary.successLatency.map(_.p95), Some(20.0))
    assertEquals(report.summary.failureLatency.map(_.p95), Some(30.0))
    assertEquals(report.queries.count(_.relevance.nonEmpty), 2)
    assertEquals(report.adoption, SearchEvaluationAdoption.DeferredPendingReviewedJudgmentsAndThresholds)
  }

  test("successful empty and no-label queries retain zero; failed and unavailable quality is absent") {
    val values = good.updated(1, good(1).copy(ranking = Succeeded(Nil, None)))
    val report = evaluated(values)
    assertEquals(report.summary.relevanceDenominator, 3)
    assertEquals(report.summary.emptyEligible, 1)
    assertEquals(report.summary.noRelevantLabels, 1)
    assertEquals(report.summary.meanRelevance.map(_.recallAtK), Some(0.5))
    val absent =
      evaluated(good.map(value => value.copy(ranking = Unavailable(SearchEvaluationUnavailable.RequiresAtlas))))
    assertEquals(absent.summary.attempted, 0)
    assertEquals(absent.summary.unavailable, 3)
    assertEquals(absent.summary.meanRelevance, None)
    val failed =
      evaluated(good.map(value => value.copy(ranking = Failed(SearchEvaluationFailure.RetrievalFailed, None))))
    assertEquals(failed.summary.failed, 3)
    assertEquals(failed.summary.meanRelevance, None)
  }

  test("invalid K, duplicates, malformed labels, nonfinite timings and coordinates accumulate typed errors") {
    val invalidCorpus = corpus.copy(
      entityIds = corpus.entityIds ++ corpus.entityIds.take(1),
      queries = corpus.queries
        .updated(0, corpus.queries(0).copy(relevantIds = Set("unknown"), eligibleIds = List(ids(0), ids(0))))
    )
    val invalid = run(good.updated(0, good(0).copy(ranking = Succeeded(List(ids(0), ids(0)), Some(Double.NaN)))))
      .copy(coordinates = base.coordinates.copy(corpusDigest = "mismatch"))
    val result = SearchEvaluationHarness.report(invalidCorpus, invalid, 0)
    val errors = result.swap.toOption.toList.flatMap(_.toChain.toList)
    assert(errors.contains(SearchEvaluationError.InvalidField("k")))
    assert(errors.contains(SearchEvaluationError.DuplicateIdentity("entities")))
    assert(errors.contains(SearchEvaluationError.DuplicateRanking(queries(0))))
    assert(errors.contains(SearchEvaluationError.InvalidField("latencyMillis")))
    assert(errors.contains(SearchEvaluationError.InconsistentCoordinates("corpusIdentity")))
    assert(errors.exists { case SearchEvaluationError.UnknownIdentity(_) => true; case _ => false })
  }

  test("duplicate queries, unknown observations, ineligible hits, invalid budgets and finite negatives fail closed") {
    assert(
      SearchEvaluationHarness
        .report(corpus.copy(queries = corpus.queries ++ corpus.queries.take(1)), run(good), 2)
        .isLeft
    )
    assert(SearchEvaluationHarness.report(corpus, run(good.updated(0, good(0).copy(queryId = "unknown"))), 2).isLeft)
    assert(
      SearchEvaluationHarness
        .report(
          corpus,
          run(good.updated(0, good(0).copy(ranking = Succeeded(List(SearchEvaluationFixtures.jobId(6)), None)))),
          2
        )
        .isLeft
    )
    assert(
      SearchEvaluationHarness
        .report(corpus, run(good).copy(coordinates = base.coordinates.copy(branchResultLimit = 1)), 2)
        .isLeft
    )
    assert(
      SearchEvaluationHarness
        .report(corpus, run(good.updated(0, good(0).copy(ranking = Succeeded(Nil, Some(-1.0))))), 2)
        .isLeft
    )
    assert(SearchEvaluationHarness.report(corpus, run(good).copy(durationMillis = Some(0L)), 2).isLeft)
  }

  test("comparisons reject differing corpus, model, K, budgets, environment and split") {
    val report = evaluated(good)
    assertEquals(
      SearchEvaluationHarness
        .compare(report, report.copy(run = report.run.copy(strategy = SearchEvaluationStrategy.Lexical))),
      Right(())
    )
    List(
      report.copy(k = 1),
      report.copy(corpus = report.corpus.copy(digest = "different")),
      report.copy(run = report.run.copy(coordinates = report.run.coordinates.copy(embeddingModel = "different"))),
      report.copy(run = report.run.copy(coordinates = report.run.coordinates.copy(branchResultLimit = 9))),
      report.copy(run = report.run.copy(environment = report.run.environment.copy(identity = "different"))),
      report.copy(corpus =
        report.corpus.copy(queries = report.corpus.queries.map(_.copy(split = SearchEvaluationSplit.Tuning)))
      )
    )
      .foreach(alternative => assert(SearchEvaluationHarness.compare(report, alternative).isLeft))
  }

  test("typed results repeat regardless of captured query order or JSON rendering") {
    val first = evaluated(good)
    val repeated = evaluated(good.reverse)
    assertEquals(first.queries, repeated.queries)
    assertEquals(first.summary, repeated.summary)
    assertEquals(first.groups, repeated.groups)
    val compact = SearchEvaluationReportJson.render(first).noSpaces
    val pretty = SearchEvaluationReportJson.render(first).spaces2
    assertEquals(io.circe.parser.parse(compact), io.circe.parser.parse(pretty))
  }

  test("public page/dimension ceilings and missing or contradictory measurement explanations are rejected") {
    assert(
      SearchEvaluationHarness
        .report(
          corpus,
          run(good)
            .copy(coordinates = base.coordinates.copy(pageSize = 101, branchResultLimit = 101, numCandidates = 101)),
          2
        )
        .isLeft
    )
    assert(
      SearchEvaluationHarness
        .report(corpus, run(good).copy(coordinates = base.coordinates.copy(embeddingDimensions = 4097)), 2)
        .isLeft
    )
    val missing = base.environment.copy(telemetryUnavailable =
      base.environment.telemetryUnavailable - SearchEvaluationTelemetry.Cpu
    )
    assert(SearchEvaluationHarness.report(corpus, run(good).copy(environment = missing), 2).isLeft)
    val contradictory = base.environment.copy(cpuMillis = Some(1L))
    assert(SearchEvaluationHarness.report(corpus, run(good).copy(environment = contradictory), 2).isLeft)
    val measured =
      contradictory.copy(telemetryUnavailable = contradictory.telemetryUnavailable - SearchEvaluationTelemetry.Cpu)
    assert(SearchEvaluationHarness.report(corpus, run(good).copy(environment = measured), 2).isRight)
  }
}
