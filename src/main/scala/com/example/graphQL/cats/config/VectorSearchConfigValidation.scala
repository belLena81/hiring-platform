package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.pagination.PageSize
import com.example.graphQL.cats.domain.search.SearchFusionStrategy

private[config] object VectorSearchConfigValidation {
  def read(vector: RawVectorSearchConfig): ValidatedNel[ConfigError, VectorSearchConfig] = {
    val voyage = vector.voyage
    val embedding = vector.embedding
    val indexes = vector.indexes
    val durableRetryBaseMillis =
      embedding.durableRetryBaseMillis.getOrElse(VectorSearchConfig.DefaultDurableRetryBaseMillis)
    val durableRetryCapMillis =
      embedding.durableRetryCapMillis.getOrElse(VectorSearchConfig.DefaultDurableRetryCapMillis)
    (
      validVoyageApiKey(vector.enabled, voyage.apiKey),
      validNumCandidates(vector.numCandidates),
      validBranchResultLimit(vector.branchResultLimit, vector.numCandidates),
      validRerankFusion(vector.rerank.enabled, vector.fusionStrategy),
      validDurableRetryWindow(durableRetryBaseMillis, durableRetryCapMillis)
    ).mapN { (apiKey, numCandidates, branchResultLimit, fusionStrategy, _) =>
      VectorSearchConfig(
        vector.enabled,
        apiKey,
        voyage.endpoint,
        voyage.model,
        voyage.dimension,
        embedding.queueSize,
        embedding.parallelism,
        embedding.timeoutMs,
        embedding.retryAttempts,
        embedding.retryDelayMs,
        indexes.jobs,
        indexes.candidates,
        indexes.lexical,
        indexes.candidateLexical,
        fusionStrategy,
        vector.rerank.enabled,
        vector.rerank.model,
        indexes.readyTimeoutMs,
        indexes.pollIntervalMs,
        numCandidates,
        branchResultLimit,
        embedding.durableRetryAttempts.getOrElse(VectorSearchConfig.DefaultDurableRetryAttempts),
        durableRetryBaseMillis,
        durableRetryCapMillis,
        embedding.workerRestartDelayMillis.getOrElse(VectorSearchConfig.DefaultWorkerRestartDelayMillis)
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
