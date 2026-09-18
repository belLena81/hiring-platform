package com.example.graphQL.cats.config

import cats.data.{NonEmptyList, ValidatedNel}
import cats.effect.IO
import cats.syntax.all.*
import com.comcast.ip4s.IpAddress
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
import scala.util.Try

enum ConfigError(val key: String) {
  case InvalidConfigFile(message: String) extends ConfigError("CONFIG_FILE")
  case InvalidHost extends ConfigError("HTTP_HOST")
  case InvalidPort extends ConfigError("HTTP_PORT")
  case InvalidAdmissionPermits extends ConfigError("HTTP_ADMISSION_PERMITS")
  case InvalidMongoUri extends ConfigError("MONGODB_URI")
  case InvalidMongoDatabase extends ConfigError("MONGODB_DATABASE")
  case InvalidMaskSensitive extends ConfigError("LOG_MASK_SENSITIVE")
  case InvalidJwtSecret extends ConfigError("AUTH_JWT_HS256_SECRET")
  case InvalidJwtIssuer extends ConfigError("AUTH_JWT_ISSUER")
  case InvalidJwtAudience extends ConfigError("AUTH_JWT_AUDIENCE")
  case InvalidVectorSearchEnabled extends ConfigError("VECTOR_SEARCH_ENABLED")
  case InvalidVoyageApiKey extends ConfigError("VOYAGE_API_KEY")
  case InvalidVoyageEndpoint extends ConfigError("VOYAGE_ENDPOINT")
  case InvalidVoyageModel extends ConfigError("VOYAGE_MODEL")
  case InvalidVoyageDimension extends ConfigError("VOYAGE_DIMENSION")
  case InvalidEmbeddingVersion extends ConfigError("EMBEDDING_VERSION")
  case InvalidEmbeddingQueueSize extends ConfigError("EMBEDDING_QUEUE_SIZE")
  case InvalidEmbeddingParallelism extends ConfigError("EMBEDDING_PARALLELISM")
  case InvalidEmbeddingTimeout extends ConfigError("EMBEDDING_TIMEOUT_MS")
  case InvalidJobVectorIndex extends ConfigError("JOB_VECTOR_INDEX")
  case InvalidCandidateVectorIndex extends ConfigError("CANDIDATE_VECTOR_INDEX")
  case InvalidJobLexicalIndex extends ConfigError("JOB_LEXICAL_INDEX")
  case InvalidSearchIndexReadyTimeout extends ConfigError("SEARCH_INDEX_READY_TIMEOUT_MS")
  case InvalidSearchIndexPollInterval extends ConfigError("SEARCH_INDEX_POLL_INTERVAL_MS")
  case InvalidVectorNumCandidates extends ConfigError("VECTOR_NUM_CANDIDATES")
}

final case class VectorSearchConfig(enabled: Boolean, voyageApiKey: Option[String], voyageEndpoint: String,
    voyageModel: String, voyageDimension: Int, embeddingVersion: Int, queueSize: Int, parallelism: Int,
    timeoutMillis: Int, jobVectorIndex: String, candidateVectorIndex: String, jobLexicalIndex: String,
    indexReadyTimeoutMillis: Int, indexPollIntervalMillis: Int, numCandidates: Int)

final case class JwtAuthConfig(hmacSecret: Option[String], issuer: String, audience: String)

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

final case class AppConfig(host: String, port: Int, admissionPermits: Int, mongoUri: String, mongoDatabase: String,
    maskSensitive: Boolean, jwtAuth: JwtAuthConfig, vectorSearch: VectorSearchConfig) {
  override def toString: String = "AppConfig([REDACTED])"
}

object AppConfig {
  def load: IO[Either[NonEmptyList[ConfigError], AppConfig]] =
    IO.blocking(ConfigSource.default.load[RawAppConfig].left.map(readError).flatMap(read))

  def loadMaskSensitive: IO[Boolean] =
    IO.blocking(ConfigSource.default.at("logging").load[RawLoggingConfig].toOption.forall(_.maskSensitive))

  def fromConfig(raw: String, env: Map[String, String]): Either[NonEmptyList[ConfigError], AppConfig] =
    for {
      parsed <- Try(ConfigFactory.parseString(raw, parseOptions)).toEither.left.map(parseError)
      resolved <- Try(parsed.withFallback(ConfigFactory.parseMap(env.asJava)).resolve(ConfigResolveOptions.noSystem())).toEither
        .left.map(parseError)
      config <- ConfigSource.fromConfig(resolved).load[RawAppConfig].left.map(readError).flatMap(read)
    } yield config

  private def read(raw: RawAppConfig): Either[NonEmptyList[ConfigError], AppConfig] =
    validate(raw).toEither

  private def validate(raw: RawAppConfig): ValidatedNel[ConfigError, AppConfig] = {
    val http = raw.http
    val mongo = raw.mongo
    val jwt = raw.auth.jwt
    val vector = raw.vectorSearch
    val voyage = vector.voyage
    val embedding = vector.embedding
    val indexes = vector.indexes

    (validHost(http.host), http.port.validNel[ConfigError], http.admissionPermits.validNel[ConfigError],
      validMongoUri(mongo.uri), validMongoDatabase(mongo.database), validJwtSecret(jwt.hs256Secret),
      validVoyageApiKey(vector.enabled, voyage.apiKey), validIndexReadyTimeout(vector.indexes.readyTimeoutMs),
      validIndexPollInterval(vector.indexes.pollIntervalMs), validNumCandidates(vector.numCandidates)).mapN {
      (host, port, permits, uri, database, secret, apiKey, readyTimeout, pollInterval, numCandidates) =>
        new AppConfig(host, port, permits, uri, database, raw.logging.maskSensitive,
          JwtAuthConfig(secret, jwt.issuer, jwt.audience),
          VectorSearchConfig(vector.enabled, apiKey, voyage.endpoint, voyage.model, voyage.dimension,
            embedding.version, embedding.queueSize, embedding.parallelism, embedding.timeoutMs,
            indexes.jobs, indexes.candidates, indexes.lexical, readyTimeout, pollInterval, numCandidates))
    }
  }

  // HTTP_HOST is a bind address, so hostnames such as localhost are intentionally rejected.
  private def validHost(value: String): ValidatedNel[ConfigError, String] =
    Either.cond(IpAddress.fromString(value).isDefined, value, ConfigError.InvalidHost).toValidatedNel
  private def validMongoUri(value: String): ValidatedNel[ConfigError, String] =
    Try(new ConnectionString(value)).toEither.leftMap(_ => ConfigError.InvalidMongoUri).toValidatedNel.map(_ => value)
  private def validMongoDatabase(value: String): ValidatedNel[ConfigError, String] =
    Either.cond(value.nonEmpty && value.getBytes(StandardCharsets.UTF_8).length < 64 &&
      !value.exists(c => c.isWhitespace || c.isControl || "/\\.\"$*<>:|?".contains(c)), value,
      ConfigError.InvalidMongoDatabase).toValidatedNel
  private def validJwtSecret(value: Option[String]): ValidatedNel[ConfigError, Option[String]] =
    value.filter(_ != "disabled").fold(Option.empty[String].validNel[ConfigError]) { secret =>
      Either.cond(secret.getBytes(StandardCharsets.UTF_8).length >= 32, Some(secret), ConfigError.InvalidJwtSecret).toValidatedNel
    }
  private def validVoyageApiKey(enabled: Boolean, value: Option[String]): ValidatedNel[ConfigError, Option[String]] =
    val normalized = value.filter(_ != "disabled")
    Either.cond(!enabled || normalized.exists(_.trim.nonEmpty), normalized, ConfigError.InvalidVoyageApiKey).toValidatedNel
  private def validNumCandidates(value: Int): ValidatedNel[ConfigError, Int] =
    Either.cond(value >= PageSize.Max && value <= 10000, value, ConfigError.InvalidVectorNumCandidates).toValidatedNel
  private def validIndexReadyTimeout(value: Int): ValidatedNel[ConfigError, Int] =
    Either.cond(value >= 1000 && value <= 600000, value, ConfigError.InvalidSearchIndexReadyTimeout).toValidatedNel
  private def validIndexPollInterval(value: Int): ValidatedNel[ConfigError, Int] =
    Either.cond(value >= 100 && value <= 10000, value, ConfigError.InvalidSearchIndexPollInterval).toValidatedNel

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
    case "mongo.uri" => Some(ConfigError.InvalidMongoUri)
    case "mongo.database" => Some(ConfigError.InvalidMongoDatabase)
    case "logging.mask-sensitive" => Some(ConfigError.InvalidMaskSensitive)
    case "auth.jwt.hs256-secret" => Some(ConfigError.InvalidJwtSecret)
    case "auth.jwt.issuer" => Some(ConfigError.InvalidJwtIssuer)
    case "auth.jwt.audience" => Some(ConfigError.InvalidJwtAudience)
    case "vector-search.enabled" => Some(ConfigError.InvalidVectorSearchEnabled)
    case "vector-search.voyage.api-key" => Some(ConfigError.InvalidVoyageApiKey)
    case "vector-search.voyage.endpoint" => Some(ConfigError.InvalidVoyageEndpoint)
    case "vector-search.voyage.model" => Some(ConfigError.InvalidVoyageModel)
    case "vector-search.voyage.dimension" => Some(ConfigError.InvalidVoyageDimension)
    case "vector-search.embedding.version" => Some(ConfigError.InvalidEmbeddingVersion)
    case "vector-search.embedding.queue-size" => Some(ConfigError.InvalidEmbeddingQueueSize)
    case "vector-search.embedding.parallelism" => Some(ConfigError.InvalidEmbeddingParallelism)
    case "vector-search.embedding.timeout-ms" => Some(ConfigError.InvalidEmbeddingTimeout)
    case "vector-search.indexes.jobs" => Some(ConfigError.InvalidJobVectorIndex)
    case "vector-search.indexes.candidates" => Some(ConfigError.InvalidCandidateVectorIndex)
    case "vector-search.indexes.lexical" => Some(ConfigError.InvalidJobLexicalIndex)
    case "vector-search.indexes.ready-timeout-ms" => Some(ConfigError.InvalidSearchIndexReadyTimeout)
    case "vector-search.indexes.poll-interval-ms" => Some(ConfigError.InvalidSearchIndexPollInterval)
    case "vector-search.num-candidates" => Some(ConfigError.InvalidVectorNumCandidates)
    case _ => None
  }

  private val parseOptions = ConfigParseOptions.defaults().setAllowMissing(false)
  private final case class RawAppConfig(http: RawHttpConfig, mongo: RawMongoConfig, logging: RawLoggingConfig,
      auth: RawAuthConfig, vectorSearch: RawVectorSearchConfig) derives ConfigReader
  private final case class RawHttpConfig(host: String, port: Port, admissionPermits: AdmissionPermits) derives ConfigReader
  private final case class RawMongoConfig(uri: String, database: String) derives ConfigReader
  private final case class RawLoggingConfig(maskSensitive: Boolean) derives ConfigReader
  private final case class RawAuthConfig(jwt: RawJwtAuthConfig) derives ConfigReader
  private final case class RawJwtAuthConfig(hs256Secret: Option[String], issuer: NonBlank128, audience: NonBlank128)
  private final case class RawVectorSearchConfig(enabled: Boolean, voyage: RawVoyageConfig, embedding: RawEmbeddingConfig,
      indexes: RawVectorIndexesConfig, numCandidates: Int) derives ConfigReader
  private final case class RawVoyageConfig(apiKey: Option[String], endpoint: HttpsUrl, model: NonBlankStr,
      dimension: VoyageDim) derives ConfigReader
  private final case class RawEmbeddingConfig(version: Positive, queueSize: QueueSize, parallelism: Parallelism,
      timeoutMs: TimeoutMs) derives ConfigReader
  private final case class RawVectorIndexesConfig(jobs: NonBlankStr, candidates: NonBlankStr, lexical: NonBlankStr,
      readyTimeoutMs: Int, pollIntervalMs: Int) derives ConfigReader

  // Derived naming does not preserve the hs256-secret acronym, so keep this key explicit.
  private given ConfigReader[RawJwtAuthConfig] =
    ConfigReader.forProduct3("hs256-secret", "issuer", "audience")(RawJwtAuthConfig.apply)
}
