package com.example.graphQL.cats.config

import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.*
import io.github.iltotore.iron.constraint.numeric.*
import io.github.iltotore.iron.constraint.string.*
import io.github.iltotore.iron.pureconfig.given
import _root_.pureconfig.*

private[config] final case class RawVectorSearchConfig(
    enabled: Boolean,
    voyage: RawVoyageConfig,
    embedding: RawEmbeddingConfig,
    indexes: RawVectorIndexesConfig,
    numCandidates: Int,
    branchResultLimit: Option[Int],
    fusionStrategy: String,
    rerank: RawRerankConfig
) derives ConfigReader
private[config] final case class RawRerankConfig(enabled: Boolean, model: NonBlankStr) derives ConfigReader
private[config] final case class RawVoyageConfig(
    apiKey: Option[String],
    endpoint: HttpsUrl,
    model: NonBlankStr,
    dimension: VoyageDim
) derives ConfigReader
private[config] final case class RawEmbeddingConfig(
    queueSize: QueueSize,
    parallelism: Parallelism,
    timeoutMs: TimeoutMs,
    retryAttempts: EmbeddingRetryAttempts,
    retryDelayMs: EmbeddingRetryDelayMs
) derives ConfigReader
private[config] final case class RawVectorIndexesConfig(
    jobs: NonBlankStr,
    candidates: NonBlankStr,
    lexical: NonBlankStr,
    candidateLexical: NonBlankStr,
    readyTimeoutMs: SearchIndexReadyTimeoutMs,
    pollIntervalMs: SearchIndexPollIntervalMs
) derives ConfigReader
