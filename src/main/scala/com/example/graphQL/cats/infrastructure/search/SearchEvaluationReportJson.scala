package com.example.graphQL.cats.infrastructure.search

import com.example.graphQL.cats.service.search.*
import io.circe.Json

/** The single active evaluation artifact shape; numerical values are already computed. */
object SearchEvaluationReportJson {
  private def text(value: String): Json = Json.fromString(value)
  private def optional[A](value: Option[A])(encode: A => Json): Json = value.fold(Json.Null)(encode)
  private def number(value: Double): Json = Json.fromDoubleOrNull(value)
  private def quality(value: SearchEvaluationQuality): Json =
    Json.obj("recallAtK" -> number(value.recallAtK), "ndcgAtK" -> number(value.ndcgAtK))
  private def percentiles(value: SearchEvaluationPercentiles): Json =
    Json.obj("p50" -> number(value.p50), "p95" -> number(value.p95), "p99" -> number(value.p99))
  private def summary(value: SearchEvaluationSummary): Json = Json.obj(
    "attemptedQueries" -> Json.fromInt(value.attempted),
    "successfulQueries" -> Json.fromInt(value.successful),
    "failedQueries" -> Json.fromInt(value.failed),
    "unavailableQueries" -> Json.fromInt(value.unavailable),
    "successfulEmptyEligibleQueries" -> Json.fromInt(value.emptyEligible),
    "successfulNoRelevantLabelQueries" -> Json.fromInt(value.noRelevantLabels),
    "relevanceDenominator" -> Json.fromInt(value.relevanceDenominator),
    "fidelityDenominator" -> Json.fromInt(value.fidelityDenominator),
    "exactReferenceFailures" -> Json.fromInt(value.exactReferenceFailures),
    "successfulOnlyMeanRelevance" -> optional(value.meanRelevance)(quality),
    "meanRecallAtKAgainstExact" -> optional(value.meanFidelityRecallAtK)(number),
    "successLatencyMillis" -> optional(value.successLatency)(percentiles),
    "failureLatencyMillis" -> optional(value.failureLatency)(percentiles),
    "exactSuccessLatencyMillis" -> optional(value.referenceLatency)(percentiles),
    "attemptedThroughputQueriesPerSecond" -> optional(value.attemptedThroughput)(number),
    "successfulThroughputQueriesPerSecond" -> optional(value.successfulThroughput)(number)
  )

  private def ranking(value: SearchEvaluationRanking): Json = value match {
    case SearchEvaluationRanking.Succeeded(ids, latency) =>
      Json.obj(
        "outcome" -> text("Succeeded"),
        "ids" -> Json.arr(ids.map(text)*),
        "latencyMillis" -> optional(latency)(number)
      )
    case SearchEvaluationRanking.Failed(category, latency) =>
      Json.obj(
        "outcome" -> text("Failed"),
        "category" -> text(category.toString),
        "latencyMillis" -> optional(latency)(number)
      )
    case SearchEvaluationRanking.Unavailable(reason) =>
      Json.obj("outcome" -> text("Unavailable"), "reason" -> text(reason.toString))
  }

  def renderAssessment(value: SearchEvaluationAssessmentResult): Json = Json.obj(
    "decision" -> text(value.decision.toString),
    "policyIdentity" -> text(value.policyIdentity),
    "evaluatedUseCases" -> Json.arr(value.evaluatedUseCases.map(useCase => text(useCase.toString))*),
    "reasons" -> Json.arr(value.reasons.map(reason => text(reason.toString))*)
  )

  def render(report: SearchEvaluationReport): Json = {
    val run = report.run
    val coordinates = run.coordinates
    val environment = run.environment
    Json.obj(
      "strategy" -> text(run.strategy.toString),
      "source" -> Json
        .obj("revision" -> text(coordinates.sourceRevision), "fingerprint" -> text(coordinates.sourceFingerprint)),
      "corpus" -> Json.obj(
        "identity" -> text(report.corpus.identity),
        "digest" -> text(report.corpus.digest),
        "seed" -> Json.fromLong(report.corpus.seed),
        "entityCount" -> Json.fromInt(report.corpus.entityIds.size),
        "judgmentIdentity" -> text(report.corpus.rubric.identity),
        "judgmentOrigin" -> text(report.corpus.rubric.origin.toString),
        "rubricApproved" -> Json.fromBoolean(report.corpus.rubric.approved),
        "rubricOwner" -> text(report.corpus.rubric.owner)
      ),
      "workload" -> Json.obj(
        "rankingOrigin" -> text(coordinates.rankingOrigin.toString),
        "embeddingModel" -> text(coordinates.embeddingModel),
        "embeddingProvenance" -> text(coordinates.embeddingProvenance.toString),
        "embeddingDimensions" -> Json.fromInt(coordinates.embeddingDimensions),
        "indexIdentity" -> text(coordinates.indexIdentity),
        "numCandidates" -> Json.fromInt(coordinates.numCandidates),
        "branchResultLimit" -> Json.fromInt(coordinates.branchResultLimit),
        "pageSize" -> Json.fromInt(coordinates.pageSize),
        "k" -> Json.fromInt(report.k),
        "tieRule" -> text(coordinates.tieRule),
        "queryCount" -> Json.fromInt(run.queries.size),
        "concurrency" -> Json.fromInt(run.concurrency),
        "warmupQueries" -> Json.fromInt(run.warmupQueries),
        "warm" -> Json.fromBoolean(run.warm),
        "timestampUtc" -> text(run.timestampUtc.toString),
        "durationMillis" -> optional(run.durationMillis)(Json.fromLong)
      ),
      "environment" -> Json.obj(
        "identity" -> text(environment.identity),
        "atlasVersion" -> optional(environment.atlasVersion)(text),
        "collectionIndexBytes" -> optional(environment.collectionIndexBytes)(Json.fromLong),
        "vectorSearchIndexBytes" -> optional(environment.vectorSearchIndexBytes)(Json.fromLong),
        "cpuMillis" -> optional(environment.cpuMillis)(Json.fromLong),
        "peakMemoryBytes" -> optional(environment.peakMemoryBytes)(Json.fromLong),
        "providerRequests" -> optional(environment.providerRequests)(Json.fromLong),
        "telemetryUnavailable" -> Json.arr(environment.telemetryUnavailable.toList.sortBy(_._1.ordinal).map {
          case (field, reason) => Json.obj("field" -> text(field.toString), "reason" -> text(reason.toString))
        }*)
      ),
      "measurements" -> summary(report.summary),
      "groups" -> Json.arr(
        report.groups.map(value =>
          Json.obj(
            "useCase" -> text(value.useCase.toString),
            "filterGroup" -> text(value.filterGroup.toString),
            "split" -> text(value.split.toString),
            "measurements" -> summary(value.summary)
          )
        )*
      ),
      "queries" -> Json.arr(
        report.queries.map(value =>
          Json.obj(
            "queryId" -> text(value.fixture.queryId),
            "filterIdentity" -> text(value.fixture.filterIdentity),
            "useCase" -> text(value.fixture.useCase.toString),
            "filterGroup" -> text(value.fixture.filterGroup.toString),
            "split" -> text(value.fixture.split.toString),
            "eligibleCount" -> Json.fromInt(value.fixture.eligibleIds.size),
            "relevantCount" -> Json.fromInt(value.fixture.relevantIds.size),
            "labelReview" -> text(value.fixture.labelReview.toString),
            "ranking" -> ranking(value.observation.ranking),
            "exactReference" -> ranking(value.observation.exactReference),
            "relevance" -> optional(value.relevance)(quality),
            "fidelityRecallAtK" -> optional(value.fidelityRecallAtK)(number)
          )
        )*
      ),
      "acceptance" -> Json.obj(
        "adoption" -> text(report.assessment.fold(report.adoption.toString)(_.decision.toString)),
        "assessment" -> optional(report.assessment)(renderAssessment),
        "reliabilityGate" -> text(report.assessment.fold("NotAssessableThresholdNotAgreed") { a =>
          if (a.reasons.contains(SearchEvaluationAssessmentReason.RetrievalFailure)) "Failed"
          else if (a.reasons.contains(SearchEvaluationAssessmentReason.PolicyProposed))
            "NotAssessableThresholdNotAgreed"
          else if (a.reasons.contains(SearchEvaluationAssessmentReason.IncompleteCoverage))
            "NotAssessableIncompleteCoverage"
          else "Passed"
        }),
        "qualityConvention" -> text("SuccessfulOnlyIncludingEmptyAndNoPositiveLabels"),
        "zeroMetricConvention" -> text("EmptyJudgmentsOrExactSetProduceZero")
      )
    )
  }
}
