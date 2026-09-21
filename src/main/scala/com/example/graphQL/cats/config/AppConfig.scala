package com.example.graphQL.cats.config

import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.IO
import cats.syntax.all.*
import com.comcast.ip4s.{Cidr, Host, IpAddress, Port as Ip4sPort}
import com.example.graphQL.cats.shared.pagination.PageSize
import com.mongodb.ConnectionString
import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigResolveOptions}
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.*
import io.github.iltotore.iron.constraint.collection.MaxLength
import io.github.iltotore.iron.constraint.numeric.*
import io.github.iltotore.iron.constraint.string.*
import io.github.iltotore.iron.pureconfig.given
import _root_.pureconfig.*
import _root_.pureconfig.error.{ConfigReaderFailures, ConvertFailure, KeyNotFound}
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters.*
import scala.concurrent.duration.*

enum ConfigError(val key: String) {
  case InvalidConfigFile(message: String) extends ConfigError("CONFIG_FILE")
  case InvalidHost extends ConfigError("HTTP_HOST")
  case InvalidPort extends ConfigError("HTTP_PORT")
  case InvalidAdmissionPermits extends ConfigError("HTTP_ADMISSION_PERMITS")
  case InvalidRequestTimeout extends ConfigError("HTTP_REQUEST_TIMEOUT_MS")
  case InvalidResolverTimeout extends ConfigError("HTTP_RESOLVER_TIMEOUT_MS")
  case InvalidMongoUri extends ConfigError("MONGODB_URI")
  case InvalidMongoDatabase extends ConfigError("MONGODB_DATABASE")
  case InvalidMaskSensitive extends ConfigError("LOG_MASK_SENSITIVE")
  case InvalidJwtSecret extends ConfigError("AUTH_JWT_HS256_SECRET")
  case InvalidJwtIssuer extends ConfigError("AUTH_JWT_ISSUER")
  case InvalidJwtAudience extends ConfigError("AUTH_JWT_AUDIENCE")
  case InvalidPasswordHashIterations extends ConfigError("AUTH_PASSWORD_HASH_ITERATIONS")
  case InvalidPasswordHashMemory extends ConfigError("AUTH_PASSWORD_HASH_MEMORY_KIB")
  case InvalidPasswordHashParallelism extends ConfigError("AUTH_PASSWORD_HASH_PARALLELISM")
  case InvalidAuthRateLimitWindow extends ConfigError("AUTH_RATE_LIMIT_WINDOW_SECONDS")
  case InvalidAuthRateLimitAttempts extends ConfigError("AUTH_RATE_LIMIT_ATTEMPTS")
  case InvalidAuthRateLimitBuckets extends ConfigError("AUTH_RATE_LIMIT_BUCKETS")
  case InvalidTrustedProxyCidrs extends ConfigError("HTTP_TRUSTED_PROXY_CIDRS")
  case InvalidVectorSearchEnabled extends ConfigError("VECTOR_SEARCH_ENABLED")
  case InvalidVoyageApiKey extends ConfigError("VOYAGE_API_KEY")
  case InvalidVoyageEndpoint extends ConfigError("VOYAGE_ENDPOINT")
  case InvalidVoyageModel extends ConfigError("VOYAGE_MODEL")
  case InvalidVoyageDimension extends ConfigError("VOYAGE_DIMENSION")
  case InvalidEmbeddingVersion extends ConfigError("EMBEDDING_VERSION")
  case InvalidEmbeddingQueueSize extends ConfigError("EMBEDDING_QUEUE_SIZE")
  case InvalidEmbeddingParallelism extends ConfigError("EMBEDDING_PARALLELISM")
  case InvalidEmbeddingTimeout extends ConfigError("EMBEDDING_TIMEOUT_MS")
  case InvalidEmbeddingRetryAttempts extends ConfigError("EMBEDDING_RETRY_ATTEMPTS")
  case InvalidEmbeddingRetryDelay extends ConfigError("EMBEDDING_RETRY_DELAY_MS")
  case InvalidJobVectorIndex extends ConfigError("JOB_VECTOR_INDEX")
  case InvalidCandidateVectorIndex extends ConfigError("CANDIDATE_VECTOR_INDEX")
  case InvalidJobLexicalIndex extends ConfigError("JOB_LEXICAL_INDEX")
  case InvalidSearchIndexReadyTimeout extends ConfigError("SEARCH_INDEX_READY_TIMEOUT_MS")
  case InvalidSearchIndexPollInterval extends ConfigError("SEARCH_INDEX_POLL_INTERVAL_MS")
  case InvalidVectorNumCandidates extends ConfigError("VECTOR_NUM_CANDIDATES")
  case InvalidKafkaEnabled extends ConfigError("KAFKA_ENABLED")
  case InvalidKafkaBootstrapServers extends ConfigError("KAFKA_BOOTSTRAP_SERVERS")
  case InvalidKafkaTopic extends ConfigError("KAFKA_TOPIC")
  case InvalidKafkaConsumerGroup extends ConfigError("KAFKA_CONSUMER_GROUP")
  case InvalidKafkaBatchSize extends ConfigError("KAFKA_BATCH_SIZE")
  case InvalidKafkaLeaseSeconds extends ConfigError("KAFKA_LEASE_SECONDS")
  case InvalidKafkaRetryDelaySeconds extends ConfigError("KAFKA_RETRY_DELAY_SECONDS")
  case InvalidKafkaMaxAttempts extends ConfigError("KAFKA_MAX_ATTEMPTS")
  case InvalidKafkaPollInterval extends ConfigError("KAFKA_POLL_INTERVAL_MS")
  case InvalidKafkaReceiptTtl extends ConfigError("KAFKA_RECEIPT_TTL_DAYS")
  case InvalidKafkaQuarantineTtl extends ConfigError("KAFKA_QUARANTINE_TTL_DAYS")
}

object ConfigError {
  val publicKeys: Set[String] = List[ConfigError](
    InvalidConfigFile(""), InvalidHost, InvalidPort, InvalidAdmissionPermits, InvalidRequestTimeout,
    InvalidResolverTimeout, InvalidMongoUri, InvalidMongoDatabase, InvalidMaskSensitive, InvalidJwtSecret,
    InvalidJwtIssuer, InvalidJwtAudience, InvalidPasswordHashIterations, InvalidPasswordHashMemory,
    InvalidPasswordHashParallelism, InvalidAuthRateLimitWindow, InvalidAuthRateLimitAttempts,
    InvalidAuthRateLimitBuckets, InvalidTrustedProxyCidrs, InvalidVectorSearchEnabled, InvalidVoyageApiKey,
    InvalidVoyageEndpoint, InvalidVoyageModel, InvalidVoyageDimension, InvalidEmbeddingVersion,
    InvalidEmbeddingQueueSize, InvalidEmbeddingParallelism, InvalidEmbeddingTimeout, InvalidEmbeddingRetryAttempts,
    InvalidEmbeddingRetryDelay, InvalidJobVectorIndex, InvalidCandidateVectorIndex, InvalidJobLexicalIndex,
    InvalidSearchIndexReadyTimeout, InvalidSearchIndexPollInterval, InvalidVectorNumCandidates, InvalidKafkaEnabled,
    InvalidKafkaBootstrapServers, InvalidKafkaTopic, InvalidKafkaConsumerGroup, InvalidKafkaBatchSize,
    InvalidKafkaLeaseSeconds, InvalidKafkaRetryDelaySeconds, InvalidKafkaMaxAttempts, InvalidKafkaPollInterval,
    InvalidKafkaReceiptTtl, InvalidKafkaQuarantineTtl
  ).map(_.key).toSet
}

final case class VectorSearchConfig(enabled: Boolean, voyageApiKey: Option[String], voyageEndpoint: String,
    voyageModel: String, voyageDimension: Int, embeddingVersion: Int, queueSize: Int, parallelism: Int,
    timeoutMillis: Int, retryAttempts: Int, retryDelayMillis: Int, jobVectorIndex: String, candidateVectorIndex: String, jobLexicalIndex: String,
    indexReadyTimeoutMillis: Int, indexPollIntervalMillis: Int, numCandidates: Int)

final case class JwtAuthConfig(hmacSecret: String, issuer: String, audience: String, accessTokenSeconds: Long = 900L)
final case class PasswordHashConfig(iterations: Int, memoryKilobytes: Int, parallelism: Int)
final case class AuthRateLimitConfig(windowSeconds: Int, attempts: Int, maxBuckets: Int)
final case class TrustedProxyConfig(cidrs: List[Cidr[IpAddress]])
final case class KafkaPublisherConfig(workerId: String, batchSize: Int, leaseSeconds: Int,
    retryDelaySeconds: Int, maxAttempts: Int, pollIntervalMillis: Int)
final case class KafkaConsumerConfig(enabled: Boolean, receiptTtlDays: Int, quarantineTtlDays: Int)
final case class KafkaConfig(enabled: Boolean, bootstrapServers: String, topic: String, consumerGroup: String,
    publisher: KafkaPublisherConfig, consumer: KafkaConsumerConfig)

type Port = Int :| Interval.Closed[1, 65535]
type AdmissionPermits = Int :| Interval.Closed[1, 1024]
type NonBlank128 = String :| (Not[Blank] & MaxLength[128])
type NonBlankStr = String :| Not[Blank]
type VoyageDim = Int :| StrictEqual[1024]
type Positive = Int :| Greater[0]
type QueueSize = Int :| Interval.Closed[1, 10000]
type Parallelism = Int :| Interval.Closed[1, 64]
type TimeoutMs = Int :| Interval.Closed[100, 60000]
type HttpsUrl = String :| StartWith["https://"]

final case class AppConfig(host: Host, port: Ip4sPort, admissionPermits: Int, requestTimeout: FiniteDuration,
    resolverTimeout: FiniteDuration, trustedProxy: TrustedProxyConfig,
    mongoUri: String, mongoDatabase: String,
    maskSensitive: Boolean, jwtAuth: JwtAuthConfig, passwordHash: PasswordHashConfig, authRateLimit: AuthRateLimitConfig,
    vectorSearch: VectorSearchConfig, kafka: KafkaConfig) {
  override def toString: String = "AppConfig([REDACTED])"
}

object AppConfig {
  def load: IO[Either[NonEmptyList[ConfigError], AppConfig]] =
    IO.blocking(ConfigSource.default.load[RawAppConfig].left.map(readError).flatMap(read))

  def loadMaskSensitive: IO[Boolean] =
    IO.blocking(ConfigSource.default.at("logging").load[RawLoggingConfig].toOption.forall(_.maskSensitive))

  def fromConfig(raw: String, env: Map[String, String]): Either[NonEmptyList[ConfigError], AppConfig] =
    for {
      parsed <- Either.catchNonFatal(ConfigFactory.parseString(raw, parseOptions)).left.map(parseError)
      resolved <- Either.catchNonFatal(parsed.withFallback(ConfigFactory.parseMap(env.asJava)).resolve(ConfigResolveOptions.noSystem())).left.map(parseError)
      config <- ConfigSource.fromConfig(resolved).load[RawAppConfig].left.map(readError).flatMap(read)
    } yield config

  private def read(raw: RawAppConfig): Either[NonEmptyList[ConfigError], AppConfig] =
    validate(raw).toEither

  private def validate(raw: RawAppConfig): ValidatedNel[ConfigError, AppConfig] = {
    val http = raw.http
    val mongo = raw.mongo
    val jwt = raw.auth.jwt
    val passwordHash = raw.auth.passwordHash.getOrElse(defaultPasswordHash)
    val kafka = raw.kafka
    val vector = raw.vectorSearch
    val voyage = vector.voyage
    val embedding = vector.embedding
    val indexes = vector.indexes

    val transport =
      (validHost(http.host), validPort(http.port), http.admissionPermits.validNel[ConfigError],
        validRequestTimeout(http.requestTimeoutMs), validResolverTimeout(http.resolverTimeoutMs, http.requestTimeoutMs),
        validTrustedProxyCidrs(http.trustedProxyCidrs),
        validMongoUri(mongo.uri), validMongoDatabase(mongo.database)).mapN {
        (host, port, permits, requestTimeout, resolverTimeout, trustedProxy, uri, database) =>
          (host, port, permits, requestTimeout.millis, resolverTimeout.millis, trustedProxy, uri, database)
      }

    val authConfig =
      (validJwtSecret(jwt.hs256Secret),
        validPasswordHashIterations(passwordHash.iterations), validPasswordHashMemory(passwordHash.memoryKib),
        validPasswordHashParallelism(passwordHash.parallelism), validAuthRateLimitWindow(raw.auth.rateLimit.windowSeconds),
        validAuthRateLimitAttempts(raw.auth.rateLimit.attempts), validAuthRateLimitBuckets(raw.auth.rateLimit.maxBuckets)).mapN {
        (secret, hashIterations, hashMemory, hashParallelism, windowSeconds, attempts, maxBuckets) =>
          (JwtAuthConfig(secret, jwt.issuer, jwt.audience), PasswordHashConfig(hashIterations, hashMemory, hashParallelism),
            AuthRateLimitConfig(windowSeconds, attempts, maxBuckets))
      }

    val kafkaConfig =
      (validKafkaBootstrapServers(kafka.bootstrapServers), validKafkaTopic(kafka.topic), validKafkaConsumerGroup(kafka.consumerGroup),
        validKafkaBatchSize(kafka.publisher.batchSize), validKafkaLeaseSeconds(kafka.publisher.leaseSeconds),
        validKafkaRetryDelaySeconds(kafka.publisher.retryDelaySeconds), validKafkaMaxAttempts(kafka.publisher.maxAttempts),
        validKafkaPollInterval(kafka.publisher.pollIntervalMs), validKafkaReceiptTtl(kafka.consumer.receiptTtlDays),
        validKafkaQuarantineTtl(kafka.consumer.quarantineTtlDays)).mapN {
        (bootstrapServers, topic, consumerGroup, batchSize, leaseSeconds, retryDelaySeconds, maxAttempts,
            kafkaPollInterval, receiptTtl, quarantineTtl) =>
          KafkaConfig(kafka.enabled, bootstrapServers, topic, consumerGroup,
            KafkaPublisherConfig(kafka.publisher.workerId, batchSize, leaseSeconds, retryDelaySeconds, maxAttempts, kafkaPollInterval),
            KafkaConsumerConfig(kafka.consumer.enabled, receiptTtl, quarantineTtl))
      }

    val vectorConfig =
      (validVoyageApiKey(vector.enabled, voyage.apiKey),
        validIndexReadyTimeout(vector.indexes.readyTimeoutMs), validIndexPollInterval(vector.indexes.pollIntervalMs),
        validNumCandidates(vector.numCandidates), validEmbeddingRetryAttempts(embedding.retryAttempts),
        validEmbeddingRetryDelay(embedding.retryDelayMs)).mapN {
        (apiKey, readyTimeout, searchIndexPollInterval, numCandidates, retryAttempts, retryDelay) =>
          VectorSearchConfig(vector.enabled, apiKey, voyage.endpoint, voyage.model, voyage.dimension,
            embedding.version, embedding.queueSize, embedding.parallelism, embedding.timeoutMs, retryAttempts, retryDelay,
            indexes.jobs, indexes.candidates, indexes.lexical, readyTimeout, searchIndexPollInterval, numCandidates)
      }

    (transport, authConfig, kafkaConfig, vectorConfig).mapN {
      case (
            (host, port, permits, requestTimeout, resolverTimeout, trustedProxy, uri, database),
            (jwtConfig, passwordHashConfig, rateLimitConfig),
            kafkaConfig,
            vectorSearchConfig
          ) =>
        AppConfig(host, port, permits, requestTimeout, resolverTimeout, trustedProxy, uri, database, raw.logging.maskSensitive,
          jwtConfig, passwordHashConfig, rateLimitConfig, vectorSearchConfig, kafkaConfig)
    }
  }

  // HTTP_HOST is a bind address, so hostnames such as localhost are intentionally rejected.
  private def bounded[A: Ordering](min: A, max: A, error: ConfigError)(value: A): ValidatedNel[ConfigError, A] =
    Either.cond(Ordering[A].lteq(min, value) && Ordering[A].lteq(value, max), value, error).toValidatedNel

  private def validHost(value: String): ValidatedNel[ConfigError, Host] =
    IpAddress.fromString(value).map(ip => ip: Host).toValidNel(ConfigError.InvalidHost)
  private def validPort(value: Port): ValidatedNel[ConfigError, Ip4sPort] =
    Ip4sPort.fromInt(value).toValidNel(ConfigError.InvalidPort)
  private def validMongoUri(value: String): ValidatedNel[ConfigError, String] =
    Either.catchNonFatal(new ConnectionString(value)).leftMap(_ => ConfigError.InvalidMongoUri).toValidatedNel.map(_ => value)
  private def validMongoDatabase(value: String): ValidatedNel[ConfigError, String] =
    Either.cond(value.nonEmpty && value.getBytes(StandardCharsets.UTF_8).length < 64 &&
      !value.exists(c => c.isWhitespace || c.isControl || "/\\.\"$*<>:|?".contains(c)), value,
      ConfigError.InvalidMongoDatabase).toValidatedNel
  private def validJwtSecret(value: Option[String]): ValidatedNel[ConfigError, String] =
    value.filter(_ != "disabled").filter(_.trim.nonEmpty).fold(ConfigError.InvalidJwtSecret.invalidNel[String]) { secret =>
      Either.cond(secret.getBytes(StandardCharsets.UTF_8).length >= 32, secret, ConfigError.InvalidJwtSecret).toValidatedNel
    }
  private def validAuthRateLimitWindow(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(1, 3600, ConfigError.InvalidAuthRateLimitWindow)(value)
  private def validAuthRateLimitAttempts(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(1, 1000, ConfigError.InvalidAuthRateLimitAttempts)(value)
  private def validAuthRateLimitBuckets(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(1, 100000, ConfigError.InvalidAuthRateLimitBuckets)(value)
  private def validPasswordHashIterations(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(1, 10, ConfigError.InvalidPasswordHashIterations)(value)
  private def validPasswordHashMemory(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(8192, 1048576, ConfigError.InvalidPasswordHashMemory)(value)
  private def validPasswordHashParallelism(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(1, 16, ConfigError.InvalidPasswordHashParallelism)(value)
  private def validRequestTimeout(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(100, 60000, ConfigError.InvalidRequestTimeout)(value)
  private def validResolverTimeout(value: Int, requestTimeout: Int): ValidatedNel[ConfigError, Int] =
    Either.cond(value >= 100 && value < requestTimeout, value, ConfigError.InvalidResolverTimeout).toValidatedNel
  private def validTrustedProxyCidrs(values: List[String]): ValidatedNel[ConfigError, TrustedProxyConfig] =
    values.traverse { value =>
      Cidr.fromString(value).filter(_.prefixBits > 0)
        .toRight(ConfigError.InvalidTrustedProxyCidrs).toValidatedNel
    }.map(TrustedProxyConfig.apply)
  private def validVoyageApiKey(enabled: Boolean, value: Option[String]): ValidatedNel[ConfigError, Option[String]] =
    val normalized = value.filter(_ != "disabled")
    Either.cond(!enabled || normalized.exists(_.trim.nonEmpty), normalized, ConfigError.InvalidVoyageApiKey).toValidatedNel
  private def validNumCandidates(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(PageSize.Max, 10000, ConfigError.InvalidVectorNumCandidates)(value)
  private def validIndexReadyTimeout(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(1000, 600000, ConfigError.InvalidSearchIndexReadyTimeout)(value)
  private def validIndexPollInterval(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(100, 10000, ConfigError.InvalidSearchIndexPollInterval)(value)
  private def validEmbeddingRetryAttempts(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(1, 10, ConfigError.InvalidEmbeddingRetryAttempts)(value)
  private def validEmbeddingRetryDelay(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(100, 60000, ConfigError.InvalidEmbeddingRetryDelay)(value)
  private def validKafkaBootstrapServers(value: String): ValidatedNel[ConfigError, String] =
    Either.cond(value.trim.nonEmpty && value.length <= 512, value, ConfigError.InvalidKafkaBootstrapServers).toValidatedNel
  private def validKafkaTopic(value: String): ValidatedNel[ConfigError, String] =
    Either.cond(value.trim.nonEmpty && value.length <= 249, value, ConfigError.InvalidKafkaTopic).toValidatedNel
  private def validKafkaConsumerGroup(value: String): ValidatedNel[ConfigError, String] =
    Either.cond(value.trim.nonEmpty && value.length <= 249, value, ConfigError.InvalidKafkaConsumerGroup).toValidatedNel
  private def validKafkaBatchSize(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(1, 500, ConfigError.InvalidKafkaBatchSize)(value)
  private def validKafkaLeaseSeconds(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(1, 3600, ConfigError.InvalidKafkaLeaseSeconds)(value)
  private def validKafkaRetryDelaySeconds(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(1, 3600, ConfigError.InvalidKafkaRetryDelaySeconds)(value)
  private def validKafkaMaxAttempts(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(1, 100, ConfigError.InvalidKafkaMaxAttempts)(value)
  private def validKafkaPollInterval(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(100, 60000, ConfigError.InvalidKafkaPollInterval)(value)
  private def validKafkaReceiptTtl(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(1, 365, ConfigError.InvalidKafkaReceiptTtl)(value)
  private def validKafkaQuarantineTtl(value: Int): ValidatedNel[ConfigError, Int] =
    bounded(1, 365, ConfigError.InvalidKafkaQuarantineTtl)(value)

  private def readError(failures: ConfigReaderFailures): NonEmptyList[ConfigError] = {
    val errors = failures.toList.flatMap {
      case ConvertFailure(KeyNotFound(key, _), _, path) => configErrorForPath(fullPath(path, key))
      case ConvertFailure(_, _, path) => configErrorForPath(path)
      case _ => None
    }.distinct
    NonEmptyList.fromList(errors).getOrElse(NonEmptyList.one(ConfigError.InvalidConfigFile("configuration decoding failed")))
  }

  private def parseError(error: Throwable): NonEmptyList[ConfigError] =
    NonEmptyList.one(ConfigError.InvalidConfigFile(Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getSimpleName)))

  private def fullPath(path: String, key: String): String = Option(path).filter(_.nonEmpty).fold(key)(parent => s"$parent.$key")
  private def configErrorForPath(path: String): Option[ConfigError] = path match {
    case "http.host" => Some(ConfigError.InvalidHost)
    case "http.port" => Some(ConfigError.InvalidPort)
    case "http.admission-permits" => Some(ConfigError.InvalidAdmissionPermits)
    case "http.request-timeout-ms" => Some(ConfigError.InvalidRequestTimeout)
    case "http.resolver-timeout-ms" => Some(ConfigError.InvalidResolverTimeout)
    case "mongo.uri" => Some(ConfigError.InvalidMongoUri)
    case "mongo.database" => Some(ConfigError.InvalidMongoDatabase)
    case "logging.mask-sensitive" => Some(ConfigError.InvalidMaskSensitive)
    case "auth.jwt.hs256-secret" => Some(ConfigError.InvalidJwtSecret)
    case "auth.jwt.issuer" => Some(ConfigError.InvalidJwtIssuer)
    case "auth.jwt.audience" => Some(ConfigError.InvalidJwtAudience)
    case "auth.password-hash.iterations" => Some(ConfigError.InvalidPasswordHashIterations)
    case "auth.password-hash.memory-kib" => Some(ConfigError.InvalidPasswordHashMemory)
    case "auth.password-hash.parallelism" => Some(ConfigError.InvalidPasswordHashParallelism)
    case "auth.rate-limit.window-seconds" => Some(ConfigError.InvalidAuthRateLimitWindow)
    case "auth.rate-limit.attempts" => Some(ConfigError.InvalidAuthRateLimitAttempts)
    case "auth.rate-limit.max-buckets" => Some(ConfigError.InvalidAuthRateLimitBuckets)
    case "http.trusted-proxy-cidrs" => Some(ConfigError.InvalidTrustedProxyCidrs)
    case "vector-search.enabled" => Some(ConfigError.InvalidVectorSearchEnabled)
    case "vector-search.voyage.api-key" => Some(ConfigError.InvalidVoyageApiKey)
    case "vector-search.voyage.endpoint" => Some(ConfigError.InvalidVoyageEndpoint)
    case "vector-search.voyage.model" => Some(ConfigError.InvalidVoyageModel)
    case "vector-search.voyage.dimension" => Some(ConfigError.InvalidVoyageDimension)
    case "vector-search.embedding.version" => Some(ConfigError.InvalidEmbeddingVersion)
    case "vector-search.embedding.queue-size" => Some(ConfigError.InvalidEmbeddingQueueSize)
    case "vector-search.embedding.parallelism" => Some(ConfigError.InvalidEmbeddingParallelism)
    case "vector-search.embedding.timeout-ms" => Some(ConfigError.InvalidEmbeddingTimeout)
    case "vector-search.embedding.retry-attempts" => Some(ConfigError.InvalidEmbeddingRetryAttempts)
    case "vector-search.embedding.retry-delay-ms" => Some(ConfigError.InvalidEmbeddingRetryDelay)
    case "vector-search.indexes.jobs" => Some(ConfigError.InvalidJobVectorIndex)
    case "vector-search.indexes.candidates" => Some(ConfigError.InvalidCandidateVectorIndex)
    case "vector-search.indexes.lexical" => Some(ConfigError.InvalidJobLexicalIndex)
    case "vector-search.indexes.ready-timeout-ms" => Some(ConfigError.InvalidSearchIndexReadyTimeout)
    case "vector-search.indexes.poll-interval-ms" => Some(ConfigError.InvalidSearchIndexPollInterval)
    case "vector-search.num-candidates" => Some(ConfigError.InvalidVectorNumCandidates)
    case "kafka.enabled" => Some(ConfigError.InvalidKafkaEnabled)
    case "kafka.bootstrap-servers" => Some(ConfigError.InvalidKafkaBootstrapServers)
    case "kafka.topic" => Some(ConfigError.InvalidKafkaTopic)
    case "kafka.consumer-group" => Some(ConfigError.InvalidKafkaConsumerGroup)
    case "kafka.publisher.batch-size" => Some(ConfigError.InvalidKafkaBatchSize)
    case "kafka.publisher.lease-seconds" => Some(ConfigError.InvalidKafkaLeaseSeconds)
    case "kafka.publisher.retry-delay-seconds" => Some(ConfigError.InvalidKafkaRetryDelaySeconds)
    case "kafka.publisher.max-attempts" => Some(ConfigError.InvalidKafkaMaxAttempts)
    case "kafka.publisher.poll-interval-ms" => Some(ConfigError.InvalidKafkaPollInterval)
    case "kafka.consumer.receipt-ttl-days" => Some(ConfigError.InvalidKafkaReceiptTtl)
    case "kafka.consumer.quarantine-ttl-days" => Some(ConfigError.InvalidKafkaQuarantineTtl)
    case _ => None
  }

  private val parseOptions = ConfigParseOptions.defaults().setAllowMissing(false)
  private final case class RawAppConfig(http: RawHttpConfig, mongo: RawMongoConfig, logging: RawLoggingConfig,
      auth: RawAuthConfig, kafka: RawKafkaConfig, vectorSearch: RawVectorSearchConfig) derives ConfigReader
  private final case class RawHttpConfig(host: String, port: Port, admissionPermits: AdmissionPermits,
      requestTimeoutMs: Int, resolverTimeoutMs: Int, trustedProxyCidrs: List[String]) derives ConfigReader
  private final case class RawMongoConfig(uri: String, database: String) derives ConfigReader
  private final case class RawLoggingConfig(maskSensitive: Boolean) derives ConfigReader
  private final case class RawAuthConfig(jwt: RawJwtAuthConfig, passwordHash: Option[RawPasswordHashConfig],
      rateLimit: RawAuthRateLimitConfig) derives ConfigReader
  private final case class RawJwtAuthConfig(hs256Secret: Option[String], issuer: NonBlank128, audience: NonBlank128)
  private final case class RawAuthRateLimitConfig(windowSeconds: Int, attempts: Int, maxBuckets: Int) derives ConfigReader
  private final case class RawPasswordHashConfig(iterations: Int, memoryKib: Int, parallelism: Int) derives ConfigReader
  private val defaultPasswordHash = RawPasswordHashConfig(iterations = 2, memoryKib = 19456, parallelism = 1)
  private final case class RawKafkaConfig(enabled: Boolean, bootstrapServers: String, topic: NonBlankStr,
      consumerGroup: NonBlankStr, publisher: RawKafkaPublisherConfig, consumer: RawKafkaConsumerConfig) derives ConfigReader
  private final case class RawKafkaPublisherConfig(workerId: NonBlankStr, batchSize: Int, leaseSeconds: Int,
      retryDelaySeconds: Int, maxAttempts: Int, pollIntervalMs: Int) derives ConfigReader
  private final case class RawKafkaConsumerConfig(enabled: Boolean, receiptTtlDays: Int, quarantineTtlDays: Int) derives ConfigReader
  private final case class RawVectorSearchConfig(enabled: Boolean, voyage: RawVoyageConfig, embedding: RawEmbeddingConfig,
      indexes: RawVectorIndexesConfig, numCandidates: Int) derives ConfigReader
  private final case class RawVoyageConfig(apiKey: Option[String], endpoint: HttpsUrl, model: NonBlankStr,
      dimension: VoyageDim) derives ConfigReader
  private final case class RawEmbeddingConfig(version: Positive, queueSize: QueueSize, parallelism: Parallelism,
      timeoutMs: TimeoutMs, retryAttempts: Int, retryDelayMs: Int) derives ConfigReader
  private final case class RawVectorIndexesConfig(jobs: NonBlankStr, candidates: NonBlankStr, lexical: NonBlankStr,
      readyTimeoutMs: Int, pollIntervalMs: Int) derives ConfigReader

  // Derived naming does not preserve the hs256-secret acronym, so keep this key explicit.
  private given ConfigReader[RawJwtAuthConfig] =
    ConfigReader.forProduct3("hs256-secret", "issuer", "audience")(RawJwtAuthConfig.apply)
}
