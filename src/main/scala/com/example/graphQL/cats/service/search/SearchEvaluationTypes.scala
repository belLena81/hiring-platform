package com.example.graphQL.cats.service.search

import java.time.Instant

enum SearchEvaluationUseCase { case JobSearch, Recommendations, RecruiterMatching }
enum SearchEvaluationFilterGroup { case Broad, Selective, Empty }
enum SearchEvaluationSplit { case Tuning, HeldOut }
enum SearchEvaluationJudgmentOrigin { case Synthetic, ProvisionalFabricated, HumanReviewedFabricated }
enum SearchEvaluationLabelReview { case Pending, Reviewed }
enum SearchEvaluationStrategy { case Lexical, Vector, ApplicationRrf, AtlasAnn }
enum SearchEvaluationRankingOrigin { case AuthoredFixture, ObservedAtlas }
enum SearchEvaluationFailure { case RetrievalFailed, ReferenceFailed, TimedOut }
enum SearchEvaluationUnavailable { case UnsupportedUseCase, NotCaptured, RequiresAtlas }
enum SearchEvaluationTelemetry {
  case Cpu, Memory, CollectionIndex, VectorIndex, Billing, AtlasQueryMetrics, Latency, AtlasVersion
}
enum SearchEvaluationTelemetryReason { case RequiresAtlas, NotCaptured, NoProviderUsed }

final case class SearchEvaluationRubric(
    identity: String,
    origin: SearchEvaluationJudgmentOrigin,
    approved: Boolean,
    owner: String
)
final case class SearchEvaluationFixtureQuery(
    queryId: String,
    useCase: SearchEvaluationUseCase,
    filterGroup: SearchEvaluationFilterGroup,
    split: SearchEvaluationSplit,
    filterIdentity: String,
    eligibleIds: List[String],
    relevantIds: Set[String],
    labelReview: SearchEvaluationLabelReview
)
final case class SearchEvaluationCorpus(
    identity: String,
    digest: String,
    seed: Long,
    entityIds: List[String],
    rubric: SearchEvaluationRubric,
    queries: List[SearchEvaluationFixtureQuery]
)
enum SearchEvaluationRanking {
  case Succeeded(ids: List[String], latencyMillis: Option[Double])
  case Failed(category: SearchEvaluationFailure, latencyMillis: Option[Double])
  case Unavailable(reason: SearchEvaluationUnavailable)
}
final case class SearchEvaluationQuery(
    queryId: String,
    ranking: SearchEvaluationRanking,
    exactReference: SearchEvaluationRanking
)
final case class SearchEvaluationCoordinates(
    sourceRevision: String,
    sourceFingerprint: String,
    corpusIdentity: String,
    corpusDigest: String,
    judgmentIdentity: String,
    rankingOrigin: SearchEvaluationRankingOrigin,
    embeddingModel: String,
    embeddingDimensions: Int,
    indexIdentity: String,
    numCandidates: Int,
    branchResultLimit: Int,
    pageSize: Int,
    tieRule: String
)
final case class SearchEvaluationEnvironment(
    identity: String,
    atlasVersion: Option[String],
    collectionIndexBytes: Option[Long],
    vectorSearchIndexBytes: Option[Long],
    cpuMillis: Option[Long],
    peakMemoryBytes: Option[Long],
    providerRequests: Option[Long],
    telemetryUnavailable: Map[SearchEvaluationTelemetry, SearchEvaluationTelemetryReason]
)
final case class SearchEvaluationRun(
    strategy: SearchEvaluationStrategy,
    coordinates: SearchEvaluationCoordinates,
    concurrency: Int,
    warmupQueries: Int,
    warm: Boolean,
    timestampUtc: Instant,
    durationMillis: Option[Long],
    environment: SearchEvaluationEnvironment,
    queries: List[SearchEvaluationQuery]
)
enum SearchEvaluationError {
  case InvalidField(field: String)
  case DuplicateIdentity(field: String)
  case UnknownIdentity(field: String)
  case InconsistentCoordinates(field: String)
  case DuplicateRanking(queryId: String)
  case IneligibleRanking(queryId: String)
  case UnreviewedJudgments(queryId: String)
}
final case class SearchEvaluationQuality(recallAtK: Double, ndcgAtK: Double)
final case class SearchEvaluationQueryResult(
    fixture: SearchEvaluationFixtureQuery,
    observation: SearchEvaluationQuery,
    relevance: Option[SearchEvaluationQuality],
    fidelityRecallAtK: Option[Double]
)
final case class SearchEvaluationPercentiles(p50: Double, p95: Double, p99: Double)
final case class SearchEvaluationSummary(
    attempted: Int,
    successful: Int,
    failed: Int,
    unavailable: Int,
    emptyEligible: Int,
    noRelevantLabels: Int,
    relevanceDenominator: Int,
    fidelityDenominator: Int,
    exactReferenceFailures: Int,
    meanRelevance: Option[SearchEvaluationQuality],
    meanFidelityRecallAtK: Option[Double],
    successLatency: Option[SearchEvaluationPercentiles],
    failureLatency: Option[SearchEvaluationPercentiles],
    referenceLatency: Option[SearchEvaluationPercentiles],
    attemptedThroughput: Option[Double],
    successfulThroughput: Option[Double]
)
final case class SearchEvaluationGroup(
    useCase: SearchEvaluationUseCase,
    filterGroup: SearchEvaluationFilterGroup,
    split: SearchEvaluationSplit,
    summary: SearchEvaluationSummary
)
enum SearchEvaluationAdoption { case DeferredPendingReviewedJudgmentsAndThresholds }
final case class SearchEvaluationReport(
    corpus: SearchEvaluationCorpus,
    run: SearchEvaluationRun,
    k: Int,
    queries: List[SearchEvaluationQueryResult],
    summary: SearchEvaluationSummary,
    groups: List[SearchEvaluationGroup],
    adoption: SearchEvaluationAdoption
)
