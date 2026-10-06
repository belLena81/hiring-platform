package com.example.graphQL.cats.service.search

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import scala.concurrent.duration.*

/** Owns bounded captures. Adapters must authorize and validate current source before returning IDs. */
object SearchEvaluationCapture {
  trait Retrieval {
    def retrieve(
        query: SearchEvaluationFixtureQuery,
        strategy: SearchEvaluationStrategy
    ): IO[Either[SearchEvaluationFailure, List[String]]]
  }

  def supported(query: SearchEvaluationFixtureQuery, strategy: SearchEvaluationStrategy): Boolean =
    strategy != SearchEvaluationStrategy.AtlasAnn &&
      (query.useCase != SearchEvaluationUseCase.Recommendations ||
        (strategy == SearchEvaluationStrategy.Vector && query.filterGroup == SearchEvaluationFilterGroup.Broad))

  def capture(
      corpus: SearchEvaluationCorpus,
      strategy: SearchEvaluationStrategy,
      concurrency: Int,
      timeout: FiniteDuration,
      retrieval: Resource[IO, Retrieval],
      maximumResults: Int = 100
  ): IO[Either[SearchEvaluationError, List[SearchEvaluationQuery]]] =
    if (corpus.identity.trim.isEmpty || corpus.digest.trim.isEmpty || corpus.rubric.identity.trim.isEmpty)
      IO.pure(Left(SearchEvaluationError.InvalidField("corpus")))
    else if (
      corpus.queries.isEmpty || corpus.queries.size > 100 || corpus.entityIds.isEmpty || corpus.entityIds.size > 10000
    )
      IO.pure(Left(SearchEvaluationError.InvalidField("corpusBounds")))
    else if (corpus.entityIds.exists(_.trim.isEmpty) || corpus.queries.exists(_.queryId.trim.isEmpty))
      IO.pure(Left(SearchEvaluationError.InvalidField("identity")))
    else if (
      corpus.entityIds.distinct.size != corpus.entityIds.size || corpus.queries
        .map(_.queryId)
        .distinct
        .size != corpus.queries.size
    )
      IO.pure(Left(SearchEvaluationError.DuplicateIdentity("corpus")))
    else if (
      corpus.queries.exists(query =>
        query.filterIdentity.trim.isEmpty || query.eligibleIds.distinct.size != query.eligibleIds.size ||
          !query.eligibleIds.forall(corpus.entityIds.contains) || !query.relevantIds.subsetOf(query.eligibleIds.toSet)
      )
    )
      IO.pure(Left(SearchEvaluationError.InvalidField("judgments")))
    else if (maximumResults < 1 || maximumResults > 100)
      IO.pure(Left(SearchEvaluationError.InvalidField("maximumResults")))
    else if (!Set(1, 8).contains(concurrency)) IO.pure(Left(SearchEvaluationError.InvalidField("concurrency")))
    else if (timeout <= Duration.Zero || timeout > 5.minutes)
      IO.pure(Left(SearchEvaluationError.InvalidField("timeout")))
    else
      retrieval.use { adapter =>
        corpus.queries
          .grouped(concurrency)
          .toList
          .traverse(_.parTraverse { query =>
            val reference = SearchEvaluationRanking.Unavailable(SearchEvaluationUnavailable.NotCaptured)
            if (!supported(query, strategy))
              IO.pure(
                SearchEvaluationQuery(
                  query.queryId,
                  SearchEvaluationRanking.Unavailable(SearchEvaluationUnavailable.UnsupportedUseCase),
                  reference
                )
              )
            else
              for {
                start <- IO.monotonic
                result <- IO
                  .defer(adapter.retrieve(query, strategy))
                  .handleError(_ => Left(SearchEvaluationFailure.RetrievalFailed))
                  .timeoutTo(timeout, IO.pure(Left(SearchEvaluationFailure.TimedOut)))
                end <- IO.monotonic
                latency = Some((end - start).toNanos.toDouble / 1000000.0)
                ranking = result.fold(
                  failure => SearchEvaluationRanking.Failed(failure, latency),
                  ids =>
                    if (
                      ids.size > maximumResults || ids.distinct.size != ids.size || !ids
                        .forall(query.eligibleIds.contains)
                    )
                      SearchEvaluationRanking.Failed(SearchEvaluationFailure.RetrievalFailed, latency)
                    else SearchEvaluationRanking.Succeeded(ids, latency)
                )
              } yield SearchEvaluationQuery(query.queryId, ranking, reference)
          })
          .map(values => Right(values.flatten))
      }
}
