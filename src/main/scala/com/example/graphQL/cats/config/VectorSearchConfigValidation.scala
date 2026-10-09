package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.domain.search.SearchFusionStrategy

private[config] object VectorSearchConfigValidation {
  def read(vector: RawVectorSearchConfig): ValidatedNel[ConfigError, VectorSearchConfig] =
    (
      validVoyageApiKey(vector.enabled, vector.voyage.apiKey),
      validNumCandidates(vector.numCandidates),
      validBranchResultLimit(vector.branchResultLimit, vector.numCandidates),
      validRerankFusion(vector.rerank.enabled, vector.fusionStrategy),
      validDurableRetryWindow(vector.embedding.durableRetryBaseMillis, vector.embedding.durableRetryCapMillis)
    ).mapN { (apiKey, numCandidates, branchResultLimit, fusionStrategy, _) =>
      apiKey.filter(_ => vector.enabled).fold(VectorSearchConfig.Disabled: VectorSearchConfig) { key =>
        VectorSearchConfig.Enabled(
          VoyageConfig(key, vector.voyage.endpoint, vector.voyage.model, vector.voyage.dimension),
          vector.embedding,
          vector.indexes,
          numCandidates,
          branchResultLimit,
          fusionStrategy,
          vector.rerank
        )
      }
    }

  def validVoyageApiKey(enabled: Boolean, value: Option[String]): ValidatedNel[ConfigError, Option[String]] =
    val normalized = value.filter(_ != "disabled")
    Either
      .cond(!enabled || normalized.exists(_.trim.nonEmpty), normalized, ConfigError.InvalidVoyageApiKey)
      .toValidatedNel
  def validNumCandidates(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(PageSize.Max, 10000, ConfigError.InvalidVectorNumCandidates)(value)

  def validBranchResultLimit(value: Option[Int], numCandidates: Int): ValidatedNel[ConfigError, Int] =
    val resolved = value.getOrElse(numCandidates)
    Either
      .cond(resolved >= PageSize.Max && resolved <= numCandidates, resolved, ConfigError.InvalidVectorBranchResultLimit)
      .toValidatedNel

  /** Reranking needs a database-side fusion; application RRF cannot feed the reranker. */
  def validRerankFusion(
      rerankEnabled: Boolean,
      strategy: SearchFusionStrategy
  ): ValidatedNel[ConfigError, SearchFusionStrategy] =
    Either
      .cond(
        !rerankEnabled || strategy != SearchFusionStrategy.ApplicationRrf,
        strategy,
        ConfigError.InvalidVectorFusionStrategy
      )
      .toValidatedNel

  def validDurableRetryWindow(baseMillis: Int, capMillis: Int): ValidatedNel[ConfigError, Unit] =
    Either.cond(capMillis >= baseMillis, (), ConfigError.InvalidEmbeddingDurableRetryWindow).toValidatedNel
}
