package com.example.graphQL.cats.config

import com.example.graphQL.cats.domain.search.SearchFusionStrategy
import io.github.iltotore.iron.pureconfig.given
import _root_.pureconfig.*
import _root_.pureconfig.generic.derivation.EnumConfigReaderDerivation

/** Enum settings are written in lower camel case (`applicationRrf`), matching the domain case names. */
private object CamelCaseEnumReaders extends EnumConfigReaderDerivation(ConfigFieldMapping(PascalCase, CamelCase))
private[config] given ConfigReader[SearchFusionStrategy] = CamelCaseEnumReaders.EnumConfigReader.derived

private[config] final case class RawVectorSearchConfig(
    enabled: Boolean,
    voyage: RawVoyageConfig,
    embedding: RawEmbeddingConfig,
    indexes: RawVectorIndexesConfig,
    numCandidates: Int,
    branchResultLimit: Option[Int],
    fusionStrategy: SearchFusionStrategy,
    rerank: RawRerankConfig
) derives ConfigReader
private[config] final case class RawRerankConfig(enabled: Boolean, model: RerankModel) derives ConfigReader
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
    retryDelayMs: EmbeddingRetryDelayMs,
    durableRetryAttempts: Option[EmbeddingDurableRetryAttempts] = None,
    durableRetryBaseMillis: Option[EmbeddingDurableRetryMs] = None,
    durableRetryCapMillis: Option[EmbeddingDurableRetryMs] = None,
    workerRestartDelayMillis: Option[EmbeddingWorkerRestartDelayMs] = None
) derives ConfigReader
private[config] final case class RawVectorIndexesConfig(
    jobs: NonBlankStr,
    candidates: NonBlankStr,
    lexical: NonBlankStr,
    candidateLexical: NonBlankStr,
    readyTimeoutMs: SearchIndexReadyTimeoutMs,
    pollIntervalMs: SearchIndexPollIntervalMs
) derives ConfigReader
