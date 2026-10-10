package com.example.graphQL.cats.service.search

import cats.data.{NonEmptyChain, ValidatedNec}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.pagination.PageSize

/** Pure calculations over validated observations; files, JSON and measurements belong to adapters. */
object SearchEvaluationHarness {
  import SearchEvaluationError.*
  import SearchEvaluationRanking.*

  def report(
      corpus: SearchEvaluationCorpus,
      run: SearchEvaluationRun,
      k: Int
  ): Either[NonEmptyChain[SearchEvaluationError], SearchEvaluationReport] =
    validate(corpus, run, k).toEither.map { _ =>
      val observations = run.queries.map(value => value.queryId -> value).toMap
      val results = corpus.queries.sortBy(_.queryId).flatMap { fixture =>
        observations.get(fixture.queryId).map { observation =>
          val relevance = observation.ranking match {
            case Succeeded(ids, _) =>
              Some(
                SearchEvaluationQuality(
                  SearchEvaluationMetrics.recallAtK(ids, fixture.relevantIds, k),
                  SearchEvaluationMetrics.ndcgAtK(ids, fixture.relevantIds, k)
                )
              )
            case _ => None
          }
          val fidelity = (observation.ranking, observation.exactReference) match {
            case (Succeeded(ids, _), Succeeded(reference, _)) =>
              Some(SearchEvaluationMetrics.recallAtK(ids, reference.take(k).toSet, k))
            case _ => None
          }
          SearchEvaluationQueryResult(fixture, observation, relevance, fidelity)
        }
      }
      val groups = results
        .groupBy(value => (value.fixture.useCase, value.fixture.filterGroup, value.fixture.split))
        .toList
        .sortBy { case ((useCase, filterGroup, split), _) => (useCase.ordinal, filterGroup.ordinal, split.ordinal) }
        .map { case ((useCase, filterGroup, split), values) =>
          SearchEvaluationGroup(useCase, filterGroup, split, summarize(values, None))
        }
      SearchEvaluationReport(
        corpus,
        run,
        k,
        results,
        summarize(results, run.durationMillis),
        groups,
        SearchEvaluationAdoption.DeferredPendingReviewedJudgmentsAndThresholds
      )
    }

  def compare(
      baseline: SearchEvaluationReport,
      alternative: SearchEvaluationReport
  ): Either[NonEmptyChain[SearchEvaluationError], Unit] = {
    val left = baseline.run.coordinates
    val right = alternative.run.coordinates
    List(
      check(baseline.k == alternative.k, InconsistentCoordinates("k")),
      check(baseline.corpus == alternative.corpus, InconsistentCoordinates("corpus")),
      check(
        left.copy(sourceRevision = right.sourceRevision, sourceFingerprint = right.sourceFingerprint) == right,
        InconsistentCoordinates("workload")
      ),
      check(
        baseline.run.concurrency == alternative.run.concurrency &&
          baseline.run.warmupQueries == alternative.run.warmupQueries && baseline.run.warm == alternative.run.warm,
        InconsistentCoordinates("execution")
      ),
      check(
        baseline.run.environment.identity == alternative.run.environment.identity &&
          baseline.run.environment.atlasVersion == alternative.run.environment.atlasVersion,
        InconsistentCoordinates("environment")
      )
    ).sequence_.toEither
  }

  private def check(condition: Boolean, error: SearchEvaluationError): ValidatedNec[SearchEvaluationError, Unit] =
    if (condition) ().validNec else error.invalidNec

  private def distinct(values: List[String], field: String): ValidatedNec[SearchEvaluationError, Unit] =
    check(values.distinct.size == values.size, DuplicateIdentity(field))

  private def validate(
      corpus: SearchEvaluationCorpus,
      run: SearchEvaluationRun,
      k: Int
  ): ValidatedNec[SearchEvaluationError, Unit] = {
    val coordinates = run.coordinates
    val entityIds = corpus.entityIds.toSet
    val fixtures = corpus.queries.map(value => value.queryId -> value).toMap
    val strings = List(
      "corpusIdentity" -> corpus.identity,
      "corpusDigest" -> corpus.digest,
      "rubricIdentity" -> corpus.rubric.identity,
      "rubricOwner" -> corpus.rubric.owner,
      "sourceRevision" -> coordinates.sourceRevision,
      "sourceFingerprint" -> coordinates.sourceFingerprint,
      "model" -> coordinates.embeddingModel,
      "indexIdentity" -> coordinates.indexIdentity,
      "tieRule" -> coordinates.tieRule,
      "environment" -> run.environment.identity
    )
    val numeric = List(
      "dimensions" -> coordinates.embeddingDimensions,
      "numCandidates" -> coordinates.numCandidates,
      "branchResultLimit" -> coordinates.branchResultLimit,
      "pageSize" -> coordinates.pageSize
    )
    val metadata = List(
      check(k > 0 && k <= coordinates.pageSize, InvalidField("k")),
      check(
        corpus.entityIds.nonEmpty && corpus.entityIds.size <= 10000 && corpus.entityIds.forall(_.nonEmpty),
        InvalidField("entities")
      ),
      check(corpus.queries.nonEmpty && corpus.queries.size <= 100, InvalidField("queries")),
      distinct(corpus.entityIds, "entities"),
      distinct(corpus.queries.map(_.queryId), "queries"),
      distinct(run.queries.map(_.queryId), "observations"),
      check(run.queries.map(_.queryId).toSet == fixtures.keySet, InconsistentCoordinates("queries")),
      check(
        coordinates.corpusIdentity == corpus.identity && coordinates.corpusDigest == corpus.digest &&
          coordinates.judgmentIdentity == corpus.rubric.identity,
        InconsistentCoordinates("corpusIdentity")
      ),
      check(Set(1, 8).contains(run.concurrency), InvalidField("concurrency")),
      check(run.warmupQueries >= 0, InvalidField("warmupQueries")),
      check(run.durationMillis.forall(_ > 0L), InvalidField("durationMillis")),
      check(coordinates.pageSize <= PageSize.Max, InvalidField("pageSize")),
      check(coordinates.embeddingDimensions <= 4096, InvalidField("dimensions")),
      check(
        coordinates.pageSize <= coordinates.branchResultLimit && coordinates.branchResultLimit <= coordinates.numCandidates &&
          coordinates.numCandidates <= 1000,
        InvalidField("budgets")
      ),
      check(
        List(
          run.environment.collectionIndexBytes,
          run.environment.vectorSearchIndexBytes,
          run.environment.cpuMillis,
          run.environment.peakMemoryBytes,
          run.environment.providerRequests
        ).flatten.forall(_ >= 0L),
        InvalidField("telemetry")
      )
    ) ++ strings.map { case (name, value) => check(value.trim.nonEmpty, InvalidField(name)) } ++
      numeric.map { case (name, value) => check(value > 0, InvalidField(name)) }
    val telemetryChecks = List(
      SearchEvaluationTelemetry.Cpu -> run.environment.cpuMillis.isDefined,
      SearchEvaluationTelemetry.Memory -> run.environment.peakMemoryBytes.isDefined,
      SearchEvaluationTelemetry.CollectionIndex -> run.environment.collectionIndexBytes.isDefined,
      SearchEvaluationTelemetry.VectorIndex -> run.environment.vectorSearchIndexBytes.isDefined,
      SearchEvaluationTelemetry.AtlasVersion -> run.environment.atlasVersion.isDefined
    ).map { case (field, present) =>
      check(
        present != run.environment.telemetryUnavailable.contains(field),
        InvalidField(s"telemetryAvailability:$field")
      )
    } ++ List(SearchEvaluationTelemetry.Billing, SearchEvaluationTelemetry.AtlasQueryMetrics).map(field =>
      check(run.environment.telemetryUnavailable.contains(field), InvalidField(s"telemetryAvailability:$field"))
    )
    val fixtureChecks = corpus.queries.flatMap { fixture =>
      List(
        check(fixture.queryId.nonEmpty && fixture.filterIdentity.nonEmpty, InvalidField("queryIdentity")),
        distinct(fixture.eligibleIds, s"eligible:${fixture.queryId}"),
        check(fixture.eligibleIds.toSet.subsetOf(entityIds), UnknownIdentity(s"eligible:${fixture.queryId}")),
        check(fixture.relevantIds.subsetOf(fixture.eligibleIds.toSet), UnknownIdentity(s"labels:${fixture.queryId}")),
        check(
          corpus.rubric.origin != SearchEvaluationJudgmentOrigin.HumanReviewedFabricated ||
            fixture.labelReview == SearchEvaluationLabelReview.Reviewed,
          UnreviewedJudgments(fixture.queryId)
        )
      )
    }
    val observationChecks = run.queries.flatMap { observation =>
      val fixture = fixtures.get(observation.queryId)
      val supported = fixture.forall(value =>
        value.useCase != SearchEvaluationUseCase.Recommendations ||
          run.strategy == SearchEvaluationStrategy.Vector || run.strategy == SearchEvaluationStrategy.AtlasAnn
      )
      val modeCheck = check(
        supported || (observation.ranking match {
          case Unavailable(SearchEvaluationUnavailable.UnsupportedUseCase) => true
          case _                                                           => false
        }),
        InconsistentCoordinates("unsupportedUseCase")
      )
      modeCheck :: List(observation.ranking, observation.exactReference).flatMap {
        case Succeeded(ids, latency) =>
          List(
            check(ids.distinct.size == ids.size, DuplicateRanking(observation.queryId)),
            check(
              fixture.exists(value => ids.toSet.subsetOf(value.eligibleIds.toSet)),
              IneligibleRanking(observation.queryId)
            ),
            check(ids.size <= coordinates.branchResultLimit, InvalidField("rankingBudget")),
            validLatency(latency)
          )
        case Failed(_, latency) => List(validLatency(latency))
        case Unavailable(_)     => Nil
      }
    }
    (metadata ++ telemetryChecks ++ fixtureChecks ++ observationChecks).sequence_
  }

  private def validLatency(value: Option[Double]): ValidatedNec[SearchEvaluationError, Unit] =
    check(value.forall(number => number.isFinite && number >= 0.0), InvalidField("latencyMillis"))

  private def mean(values: List[Double]): Option[Double] =
    Option.when(values.nonEmpty)(values.sum / values.size.toDouble)

  private def percentiles(values: List[Double]): Option[SearchEvaluationPercentiles] = {
    val sorted = values.sorted
    def percentile(p: Double): Option[Double] = sorted.lift(math.max(0, math.ceil(p * sorted.size).toInt - 1))
    (percentile(0.50), percentile(0.95), percentile(0.99)).mapN(SearchEvaluationPercentiles.apply)
  }

  private def summarize(results: List[SearchEvaluationQueryResult], duration: Option[Long]): SearchEvaluationSummary = {
    val successes = results.filter(_.relevance.nonEmpty)
    val failures = results.filter(_.observation.ranking match { case Failed(_, _) => true; case _ => false })
    val quality = successes.flatMap(_.relevance)
    val fidelity = results.flatMap(_.fidelityRecallAtK)
    val attempted = successes.size + failures.size
    def rate(count: Int): Option[Double] = duration.map(milliseconds => count.toDouble * 1000.0 / milliseconds.toDouble)
    SearchEvaluationSummary(
      attempted,
      successes.size,
      failures.size,
      results.size - attempted,
      successes.count(_.fixture.eligibleIds.isEmpty),
      successes.count(_.fixture.relevantIds.isEmpty),
      quality.size,
      fidelity.size,
      results.count(value => value.observation.exactReference match { case Failed(_, _) => true; case _ => false }),
      (mean(quality.map(_.recallAtK)), mean(quality.map(_.ndcgAtK))).mapN(SearchEvaluationQuality.apply),
      mean(fidelity),
      percentiles(successes.flatMap(_.observation.ranking match {
        case Succeeded(_, latency) => latency; case _ => None
      })),
      percentiles(failures.flatMap(_.observation.ranking match { case Failed(_, latency) => latency; case _ => None })),
      percentiles(results.flatMap(_.observation.exactReference match {
        case Succeeded(_, latency) => latency; case _ => None
      })),
      rate(attempted),
      rate(successes.size)
    )
  }
}
