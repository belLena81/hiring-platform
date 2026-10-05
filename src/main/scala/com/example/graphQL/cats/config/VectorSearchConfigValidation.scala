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
    (
      validVoyageApiKey(vector.enabled, voyage.apiKey),
      validNumCandidates(vector.numCandidates),
      validBranchResultLimit(vector.branchResultLimit, vector.numCandidates),
      validFusionStrategy(vector.fusionStrategy),
      validRerankModel(vector.rerank.model)
    ).mapN { (apiKey, numCandidates, branchResultLimit, fusionStrategy, rerankModel) =>
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
        rerankModel,
        indexes.readyTimeoutMs,
        indexes.pollIntervalMs,
        numCandidates,
        branchResultLimit
      )
    }.andThen(config =>
      Either
        .cond(
          !config.rerankEnabled || config.fusionStrategy != SearchFusionStrategy.ApplicationRrf,
          config,
          ConfigError.InvalidVectorFusionStrategy
        )
        .toValidatedNel
    )
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

  def validFusionStrategy(value: String): ValidatedNel[ConfigError, SearchFusionStrategy] =
    value match {
      case "applicationRrf"   => SearchFusionStrategy.ApplicationRrf.validNel
      case "mongoRankFusion"  => SearchFusionStrategy.MongoRankFusion.validNel
      case "mongoScoreFusion" => SearchFusionStrategy.MongoScoreFusion.validNel
      case _                  => ConfigError.InvalidVectorFusionStrategy.invalidNel
    }

  def validRerankModel(value: String): ValidatedNel[ConfigError, String] =
    Either
      .cond(
        Set("rerank-2.5", "rerank-2.5-lite", "rerank-2", "rerank-2-lite").contains(value),
        value,
        ConfigError.InvalidRerankModel
      )
      .toValidatedNel
  def validIndexReadyTimeout(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(1000, 600000, ConfigError.InvalidSearchIndexReadyTimeout)(value)
  def validIndexPollInterval(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(100, 10000, ConfigError.InvalidSearchIndexPollInterval)(value)
  def validEmbeddingRetryAttempts(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(1, 10, ConfigError.InvalidEmbeddingRetryAttempts)(value)
  def validEmbeddingRetryDelay(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(100, 60000, ConfigError.InvalidEmbeddingRetryDelay)(value)
}
