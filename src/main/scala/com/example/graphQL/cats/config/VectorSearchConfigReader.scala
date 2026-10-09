package com.example.graphQL.cats.config

import cats.syntax.all.*
import com.example.graphQL.cats.domain.search.SearchFusionStrategy
import io.github.iltotore.iron.pureconfig.given
import org.http4s.Uri
import _root_.pureconfig.*
import _root_.pureconfig.error.CannotConvert
import _root_.pureconfig.generic.derivation.EnumConfigReaderDerivation

/** Enum settings are written in lower camel case (`applicationRrf`), matching the domain case names. */
private object CamelCaseEnumReaders extends EnumConfigReaderDerivation(ConfigFieldMapping(PascalCase, CamelCase))
private[config] given ConfigReader[SearchFusionStrategy] = CamelCaseEnumReaders.EnumConfigReader.derived

private given ConfigReader[Uri] =
  ConfigReader[HttpsUrl].emap(url => Uri.fromString(url).leftMap(_ => CannotConvert(url, "Uri", "invalid")))

private given ConfigReader[EmbeddingConfig] =
  ConfigReader.forProduct9(
    "queue-size",
    "parallelism",
    "timeout-ms",
    "retry-attempts",
    "retry-delay-ms",
    "durable-retry-attempts",
    "durable-retry-base-millis",
    "durable-retry-cap-millis",
    "worker-restart-delay-millis"
  )(
    (
        queueSize: QueueSize,
        parallelism: Parallelism,
        timeoutMs: TimeoutMs,
        retryAttempts: EmbeddingRetryAttempts,
        retryDelayMs: EmbeddingRetryDelayMs,
        durableAttempts: Option[EmbeddingDurableRetryAttempts],
        durableBaseMillis: Option[EmbeddingDurableRetryMs],
        durableCapMillis: Option[EmbeddingDurableRetryMs],
        restartDelayMillis: Option[EmbeddingWorkerRestartDelayMs]
    ) =>
      EmbeddingConfig(
        queueSize,
        parallelism,
        timeoutMs,
        retryAttempts,
        retryDelayMs,
        durableAttempts.getOrElse(EmbeddingConfig.DefaultDurableRetryAttempts),
        durableBaseMillis.getOrElse(EmbeddingConfig.DefaultDurableRetryBaseMillis),
        durableCapMillis.getOrElse(EmbeddingConfig.DefaultDurableRetryCapMillis),
        restartDelayMillis.getOrElse(EmbeddingConfig.DefaultWorkerRestartDelayMillis)
      )
  )

private[config] final case class RawVectorSearchConfig(
    enabled: Boolean,
    voyage: RawVoyageConfig,
    embedding: EmbeddingConfig,
    indexes: VectorIndexesConfig,
    numCandidates: Int,
    branchResultLimit: Option[Int],
    fusionStrategy: SearchFusionStrategy,
    rerank: RerankConfig
) derives ConfigReader
private[config] final case class RawVoyageConfig(
    apiKey: Option[String],
    endpoint: Uri,
    model: NonBlankStr,
    dimension: VoyageDim
) derives ConfigReader
