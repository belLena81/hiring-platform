package com.example.graphQL.cats.service.search

import cats.data.NonEmptyChain

enum SearchEvaluationPolicyAgreement { case Proposed, Agreed }
enum SearchEvaluationDecision { case Recommend, Defer, Reject }
enum SearchEvaluationEvidenceOutcome { case Passed, Failed }
final case class SearchEvaluationUseCasePolicy(maximumCost: Option[BigDecimal] = None)
final case class SearchEvaluationAssessmentPolicy(
    identity: String,
    agreement: SearchEvaluationPolicyAgreement = SearchEvaluationPolicyAgreement.Proposed,
    useCases: Map[SearchEvaluationUseCase, SearchEvaluationUseCasePolicy]
)
final case class SearchEvaluationLabelAttestation(corpusDigest: String, judgmentIdentity: String, reviewer: String)

/** Evidence is supplied by an observing adapter, never inferred from returned entity identifiers. */
final case class SearchEvaluationObservedEvidence(
    corpus: SearchEvaluationCorpus,
    run: SearchEvaluationRun,
    source: String,
    privacy: SearchEvaluationEvidenceOutcome,
    eligibility: SearchEvaluationEvidenceOutcome,
    billingIdentity: Option[String],
    currency: Option[String],
    useCaseCosts: Map[SearchEvaluationUseCase, BigDecimal]
)
enum SearchEvaluationAssessmentReason {
  case PolicyProposed, LabelsUnreviewed, MissingLabelAttestation, FixtureRankings, FixtureEmbeddings
  case MissingObservedEvidence, PrivacyFailure, EligibilityFailure, IncompleteCoverage, RetrievalFailure
  case MissingRelevance, RelevanceRegression, MissingLatency, LatencyRegression
  case MissingProviderMeasurements, ProviderRequestRegression, MissingComparableBilling, CostLimitExceeded
}
final case class SearchEvaluationAssessmentResult(
    decision: SearchEvaluationDecision,
    policyIdentity: String,
    reasons: List[SearchEvaluationAssessmentReason],
    evaluatedUseCases: List[SearchEvaluationUseCase]
)

/** Conservative recommendation policy: zero regressions and complete observed held-out evidence. */
object SearchEvaluationAssessment {
  import SearchEvaluationAssessmentReason.*

  def assess(
      baseline: SearchEvaluationReport,
      candidate: SearchEvaluationReport,
      policy: SearchEvaluationAssessmentPolicy,
      labels: Option[SearchEvaluationLabelAttestation],
      baselineEvidence: Option[SearchEvaluationObservedEvidence],
      candidateEvidence: Option[SearchEvaluationObservedEvidence]
  ): Either[NonEmptyChain[SearchEvaluationError], SearchEvaluationAssessmentResult] =
    for {
      left <- SearchEvaluationHarness.report(baseline.corpus, baseline.run, baseline.k)
      right <- SearchEvaluationHarness.report(candidate.corpus, candidate.run, candidate.k)
      result <-
        if (
          left.copy(assessment = baseline.assessment) != baseline ||
          right.copy(assessment = candidate.assessment) != candidate
        )
          Left(NonEmptyChain.one(SearchEvaluationError.InconsistentCoordinates("reportMeasurements")))
        else assessValidated(left, right, policy, labels, baselineEvidence, candidateEvidence)
    } yield result

  private def assessValidated(
      baseline: SearchEvaluationReport,
      candidate: SearchEvaluationReport,
      policy: SearchEvaluationAssessmentPolicy,
      labels: Option[SearchEvaluationLabelAttestation],
      baselineEvidence: Option[SearchEvaluationObservedEvidence],
      candidateEvidence: Option[SearchEvaluationObservedEvidence]
  ): Either[NonEmptyChain[SearchEvaluationError], SearchEvaluationAssessmentResult] = {
    val corpusUseCases = baseline.corpus.queries.map(_.useCase).toSet
    val supported = policy.useCases.keySet
    val invalidPolicy = policy.identity.trim.isEmpty || supported.isEmpty || !supported.subsetOf(corpusUseCases) ||
      policy.useCases.values.exists(_.maximumCost.exists(_ < 0))
    val evidenceMatches = List(baseline -> baselineEvidence, candidate -> candidateEvidence).forall {
      case (report, evidence) =>
        evidence.forall(value =>
          value.source.trim.nonEmpty && value.corpus == report.corpus && value.run == report.run && value.useCaseCosts.values
            .forall(_ >= 0)
        )
    }
    if (invalidPolicy) Left(NonEmptyChain.one(SearchEvaluationError.InvalidField("assessmentPolicy")))
    else if (!evidenceMatches)
      Left(NonEmptyChain.one(SearchEvaluationError.InconsistentCoordinates("observedEvidence")))
    else
      SearchEvaluationHarness.compare(baseline, candidate).map { _ =>
        val reports = List(baseline, candidate)
        def supportedQuery(query: SearchEvaluationFixtureQuery, strategy: SearchEvaluationStrategy): Boolean =
          query.useCase != SearchEvaluationUseCase.Recommendations ||
            (query.filterGroup == SearchEvaluationFilterGroup.Broad &&
              strategy == SearchEvaluationStrategy.Vector)
        val allSupported = baseline.corpus.queries.filter(q =>
          supported.contains(q.useCase) &&
            supportedQuery(q, baseline.run.strategy) && supportedQuery(q, candidate.run.strategy)
        )
        val supportedIds = allSupported.map(_.queryId).toSet
        val heldOut = baseline.corpus.queries.filter(q =>
          q.split == SearchEvaluationSplit.HeldOut && supported.contains(q.useCase) &&
            supportedQuery(q, baseline.run.strategy) && supportedQuery(q, candidate.run.strategy)
        )
        val keys = heldOut.map(q => (q.useCase, q.filterGroup)).toSet
        def groups(report: SearchEvaluationReport) = report.groups
          .filter(g => g.split == SearchEvaluationSplit.HeldOut && keys.contains((g.useCase, g.filterGroup)))
          .map(g => (g.useCase, g.filterGroup) -> g.summary)
          .toMap
        val left = groups(baseline)
        val right = groups(candidate)
        val evidence = List(baselineEvidence, candidateEvidence)
        val reviewed = baseline.corpus.rubric.origin == SearchEvaluationJudgmentOrigin.HumanReviewedFabricated &&
          baseline.corpus.rubric.approved && baseline.corpus.queries
            .filter(q => supported.contains(q.useCase))
            .forall(_.labelReview == SearchEvaluationLabelReview.Reviewed)
        val attested = labels.exists(a =>
          a.corpusDigest == baseline.corpus.digest &&
            a.judgmentIdentity == baseline.corpus.rubric.identity && a.reviewer.trim.nonEmpty
        )
        val paired = keys.toList.flatMap(key => left.get(key).flatMap(a => right.get(key).map(b => (key, a, b))))
        val completeAttempts = reports.forall(r =>
          r.queries
            .filter(q => supportedIds.contains(q.fixture.queryId))
            .forall(q =>
              q.observation.ranking match {
                case SearchEvaluationRanking.Unavailable(_) => false
                case _                                      => true
              }
            )
        )
        val coverage = completeAttempts && heldOut.nonEmpty && heldOut.map(_.useCase).toSet == supported &&
          left.keySet == keys && right.keySet == keys && reports.forall(report =>
            report.queries.map(_.fixture.queryId).toSet == report.corpus.queries.map(_.queryId).toSet
          ) &&
          paired.forall { case ((useCase, filter), a, b) =>
            val expected = heldOut.count(q => q.useCase == useCase && q.filterGroup == filter)
            a.attempted == expected && b.attempted == expected && a.unavailable == 0 && b.unavailable == 0
          }
        val comparableBilling = (baselineEvidence, candidateEvidence) match {
          case (Some(a), Some(b)) =>
            a.billingIdentity.exists(_.trim.nonEmpty) && a.billingIdentity == b.billingIdentity &&
            a.currency.exists(_.trim.nonEmpty) && a.currency == b.currency &&
            a.useCaseCosts.keySet == supported && b.useCaseCosts.keySet == supported
          case _ => false
        }
        val conditions = List(
          (policy.agreement == SearchEvaluationPolicyAgreement.Proposed, PolicyProposed),
          (!reviewed, LabelsUnreviewed),
          (!attested, MissingLabelAttestation),
          (
            reports.exists(_.run.coordinates.rankingOrigin == SearchEvaluationRankingOrigin.AuthoredFixture),
            FixtureRankings
          ),
          (
            reports
              .exists(_.run.coordinates.embeddingProvenance != SearchEvaluationEmbeddingProvenance.ProviderGenerated),
            FixtureEmbeddings
          ),
          (evidence.exists(_.isEmpty), MissingObservedEvidence),
          (evidence.flatten.exists(_.privacy == SearchEvaluationEvidenceOutcome.Failed), PrivacyFailure),
          (evidence.flatten.exists(_.eligibility == SearchEvaluationEvidenceOutcome.Failed), EligibilityFailure),
          (!coverage, IncompleteCoverage),
          (
            reports.exists(r =>
              r.queries.exists(q =>
                supportedIds.contains(q.fixture.queryId) && (q.observation.ranking match {
                  case SearchEvaluationRanking.Failed(_, _) => true
                  case _                                    => false
                })
              )
            ),
            RetrievalFailure
          ),
          (paired.exists { case (_, a, b) => a.meanRelevance.isEmpty || b.meanRelevance.isEmpty }, MissingRelevance),
          (
            paired.exists { case (_, a, b) =>
              (a.meanRelevance, b.meanRelevance) match {
                case (Some(x), Some(y)) => y.recallAtK < x.recallAtK || y.ndcgAtK < x.ndcgAtK
                case _                  => false
              }
            },
            RelevanceRegression
          ),
          (
            reports.exists(r =>
              r.queries
                .filter(q => supportedIds.contains(q.fixture.queryId))
                .exists(q =>
                  q.observation.ranking match {
                    case SearchEvaluationRanking.Succeeded(_, latency) => latency.isEmpty
                    case _                                             => false
                  }
                )
            ),
            MissingLatency
          ),
          (
            paired.exists { case (_, a, b) =>
              (a.successLatency, b.successLatency) match {
                case (Some(x), Some(y)) => y.p95 > x.p95
                case _                  => false
              }
            },
            LatencyRegression
          ),
          (reports.exists(_.run.environment.providerRequests.isEmpty), MissingProviderMeasurements),
          (
            (baseline.run.environment.providerRequests, candidate.run.environment.providerRequests) match {
              case (Some(a), Some(b)) => b > a
              case _                  => false
            },
            ProviderRequestRegression
          ),
          (!comparableBilling, MissingComparableBilling),
          (
            comparableBilling && candidateEvidence.exists(e =>
              e.useCaseCosts.exists { case (useCase, cost) =>
                policy.useCases.get(useCase).exists(_.maximumCost.exists(cost > _)) || baselineEvidence
                  .exists(_.useCaseCosts.get(useCase).exists(cost > _))
              }
            ),
            CostLimitExceeded
          )
        )
        val reasons = conditions.collect { case (true, reason) => reason }
        val failures = Set(
          PrivacyFailure,
          EligibilityFailure,
          RetrievalFailure,
          RelevanceRegression,
          LatencyRegression,
          ProviderRequestRegression,
          CostLimitExceeded
        )
        val decision =
          if (reasons.exists(failures.contains)) SearchEvaluationDecision.Reject
          else if (reasons.nonEmpty) SearchEvaluationDecision.Defer
          else SearchEvaluationDecision.Recommend
        SearchEvaluationAssessmentResult(decision, policy.identity, reasons, supported.toList.sortBy(_.ordinal))
      }
  }
}
