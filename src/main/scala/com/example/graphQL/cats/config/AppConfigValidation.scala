package com.example.graphQL.cats.config

import cats.data.{NonEmptyList, ValidatedNel}
import cats.syntax.all.*
import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import _root_.pureconfig.*
import _root_.pureconfig.error.{ConfigReaderFailures, ConvertFailure, KeyNotFound}
import scala.jdk.CollectionConverters.*

private[config] object AppConfigValidation {
  def fromConfig(raw: String, env: Map[String, String]): Either[NonEmptyList[ConfigError], AppConfig] =
    resolve(raw, env).map(ConfigSource.fromConfig).flatMap(read)

  def resolve(raw: String, env: Map[String, String]): Either[NonEmptyList[ConfigError], com.typesafe.config.Config] =
    for {
      parsed <- Either.catchNonFatal(ConfigFactory.parseString(raw, parseOptions)).left.map(parseError)
      resolved <- Either
        .catchNonFatal(parsed.withFallback(ConfigFactory.parseMap(env.asJava)).resolve(ConfigResolveOptions.noSystem()))
        .left
        .map(parseError)
    } yield resolved

  def read(source: ConfigSource): Either[NonEmptyList[ConfigError], AppConfig] = {
    val decoded =
      (
        loadSection[RawHttpConfig](source, "http"),
        loadSection[RawMongoConfig](source, "mongo"),
        loadSection[RawLoggingConfig](source, "logging"),
        loadSection[RawAuthConfig](source, "auth"),
        loadSection[RawKafkaConfig](source, "kafka"),
        loadSection[RawVectorSearchConfig](source, "vector-search")
      ).mapN((http, mongo, logging, auth, kafka, vector) => (http, mongo, logging, auth, kafka, vector))

    decoded.andThen { case (http, mongo, logging, auth, kafka, vector) =>
      (
        HttpMongoConfigValidation.read(http, mongo),
        AuthConfigValidation.read(auth),
        KafkaConfigValidation.read(kafka),
        VectorSearchConfigValidation.read(vector)
      ).mapN { (transport, authSettings, kafkaSettings, vectorSettings) =>
        AppConfig(
          transport.host,
          transport.port,
          transport.admissionPermits,
          transport.requestTimeout,
          transport.trustedProxy,
          transport.mongoUri,
          transport.mongoDatabase,
          logging.maskSensitive,
          authSettings.jwt,
          authSettings.passwordHash,
          authSettings.rateLimit,
          vectorSettings,
          kafkaSettings,
          mongo.resetOnStart.getOrElse(false),
          transport.discovery,
          authSettings.adminSeed
        )
      }
    }.toEither
  }

  private def loadSection[A: ConfigReader](source: ConfigSource, section: String): ValidatedNel[ConfigError, A] =
    source.at(section).load[A].leftMap(readError(_, section)).toValidated

  private def readError(failures: ConfigReaderFailures, section: String): NonEmptyList[ConfigError] = {
    val errors = failures.toList.flatMap {
      case ConvertFailure(KeyNotFound(key, _), _, path) =>
        configErrorForPath(qualifiedPath(section, fullPath(path, key)))
      case ConvertFailure(_, _, path) => configErrorForPath(qualifiedPath(section, path))
      case _                          => None
    }.distinct
    NonEmptyList
      .fromList(errors)
      .getOrElse(NonEmptyList.one(ConfigError.InvalidConfigFile("configuration decoding failed")))
  }

  private def qualifiedPath(section: String, path: String): String =
    if (path == section || path.startsWith(s"$section.")) path else s"$section.$path"

  private def fullPath(path: String, key: String): String =
    Option(path).filter(_.nonEmpty).fold(key)(parent => s"$parent.$key")

  private def configErrorForPath(path: String): Option[ConfigError] = path match {
    case "http.host"               => Some(ConfigError.InvalidHost)
    case "http.port"               => Some(ConfigError.InvalidPort)
    case "http.admission-permits"  => Some(ConfigError.InvalidAdmissionPermits)
    case "http.request-timeout-ms" => Some(ConfigError.InvalidRequestTimeout)
    case "mongo.uri"               => Some(ConfigError.InvalidMongoUri)
    case "mongo.discovery.max-time-millis" | "mongo.discovery.permits" | "mongo.discovery.max-roots" =>
      Some(ConfigError.InvalidDiscoveryQueryLimits)
    case "kafka.consumer.partition-concurrency" => Some(ConfigError.InvalidKafkaPartitionConcurrency)
    case "vector-search.embedding.durable-retry-attempts" | "vector-search.embedding.durable-retry-base-millis" |
        "vector-search.embedding.durable-retry-cap-millis" | "vector-search.embedding.worker-restart-delay-millis" =>
      Some(ConfigError.InvalidEmbeddingRecovery)
    case "mongo.database"                           => Some(ConfigError.InvalidMongoDatabase)
    case "logging.mask-sensitive"                   => Some(ConfigError.InvalidMaskSensitive)
    case "kafka.restart-max-delay-seconds"          => Some(ConfigError.InvalidKafkaRestartMaxDelay)
    case path if path.startsWith("auth.admin-seed") => Some(ConfigError.InvalidAdminSeed)
    case "auth.jwt.hs256-secret"                    => Some(ConfigError.InvalidJwtSecret)
    case "auth.jwt.receipt-fingerprint-secret"      => Some(ConfigError.InvalidReceiptFingerprintSecret)
    case "auth.jwt.issuer"                          => Some(ConfigError.InvalidJwtIssuer)
    case "auth.jwt.audience"                        => Some(ConfigError.InvalidJwtAudience)
    case "auth.jwt.cursor-ttl-seconds"              => Some(ConfigError.InvalidCursorTtl)
    case "auth.password-hash.iterations"            => Some(ConfigError.InvalidPasswordHashIterations)
    case "auth.password-hash.memory-kib"            => Some(ConfigError.InvalidPasswordHashMemory)
    case "auth.password-hash.parallelism"           => Some(ConfigError.InvalidPasswordHashParallelism)
    case "auth.rate-limit.window-seconds"           => Some(ConfigError.InvalidAuthRateLimitWindow)
    case "auth.rate-limit.attempts"                 => Some(ConfigError.InvalidAuthRateLimitAttempts)
    case "auth.rate-limit.max-buckets"              => Some(ConfigError.InvalidAuthRateLimitBuckets)
    case "http.trusted-proxy-cidrs"                 => Some(ConfigError.InvalidTrustedProxyCidrs)
    case "vector-search.enabled"                    => Some(ConfigError.InvalidVectorSearchEnabled)
    case "vector-search.voyage.api-key"             => Some(ConfigError.InvalidVoyageApiKey)
    case "vector-search.voyage.endpoint"            => Some(ConfigError.InvalidVoyageEndpoint)
    case "vector-search.voyage.model"               => Some(ConfigError.InvalidVoyageModel)
    case "vector-search.voyage.dimension"           => Some(ConfigError.InvalidVoyageDimension)
    case "vector-search.embedding.queue-size"       => Some(ConfigError.InvalidEmbeddingQueueSize)
    case "vector-search.embedding.parallelism"      => Some(ConfigError.InvalidEmbeddingParallelism)
    case "vector-search.embedding.timeout-ms"       => Some(ConfigError.InvalidEmbeddingTimeout)
    case "vector-search.embedding.retry-attempts"   => Some(ConfigError.InvalidEmbeddingRetryAttempts)
    case "vector-search.embedding.retry-delay-ms"   => Some(ConfigError.InvalidEmbeddingRetryDelay)
    case "vector-search.indexes.jobs"               => Some(ConfigError.InvalidJobVectorIndex)
    case "vector-search.indexes.candidates"         => Some(ConfigError.InvalidCandidateVectorIndex)
    case "vector-search.indexes.lexical"            => Some(ConfigError.InvalidJobLexicalIndex)
    case "vector-search.indexes.ready-timeout-ms"   => Some(ConfigError.InvalidSearchIndexReadyTimeout)
    case "vector-search.indexes.poll-interval-ms"   => Some(ConfigError.InvalidSearchIndexPollInterval)
    case "vector-search.num-candidates"             => Some(ConfigError.InvalidVectorNumCandidates)
    case "vector-search.branch-result-limit"        => Some(ConfigError.InvalidVectorBranchResultLimit)
    case "vector-search.fusion-strategy"            => Some(ConfigError.InvalidVectorFusionStrategy)
    case "vector-search.rerank.model"               => Some(ConfigError.InvalidRerankModel)
    case "kafka.enabled"                            => Some(ConfigError.InvalidKafkaEnabled)
    case "kafka.bootstrap-servers"                  => Some(ConfigError.InvalidKafkaBootstrapServers)
    case "kafka.topic"                              => Some(ConfigError.InvalidKafkaTopic)
    case "kafka.consumer-group"                     => Some(ConfigError.InvalidKafkaConsumerGroup)
    case "kafka.publisher.batch-size"               => Some(ConfigError.InvalidKafkaBatchSize)
    case "kafka.publisher.lease-seconds"            => Some(ConfigError.InvalidKafkaLeaseSeconds)
    case "kafka.publisher.retry-delay-seconds"      => Some(ConfigError.InvalidKafkaRetryDelaySeconds)
    case "kafka.publisher.max-attempts"             => Some(ConfigError.InvalidKafkaMaxAttempts)
    case "kafka.publisher.poll-interval-ms"         => Some(ConfigError.InvalidKafkaPollInterval)
    case "kafka.consumer.receipt-ttl-days"          => Some(ConfigError.InvalidKafkaReceiptTtl)
    case "kafka.consumer.quarantine-ttl-days"       => Some(ConfigError.InvalidKafkaQuarantineTtl)
    case "kafka.sasl-security-protocol"             => Some(ConfigError.InvalidKafkaSaslSecurityProtocol)
    case _                                          => None
  }

  private def parseError(error: Throwable): NonEmptyList[ConfigError] =
    NonEmptyList.one(
      ConfigError.InvalidConfigFile(Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getSimpleName))
    )

  private val parseOptions = ConfigParseOptions.defaults().setAllowMissing(false)
}
