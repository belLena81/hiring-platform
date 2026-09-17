package com.example.graphQL.cats.config

import cats.effect.IO
import com.comcast.ip4s.IpAddress
import com.mongodb.ConnectionString
import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigResolveOptions, ConfigResolver, ConfigValue, ConfigValueFactory}
import pureconfig.*
import pureconfig.error.{ConfigReaderFailures, ConvertFailure, KeyNotFound}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.util.Try

enum ConfigError(val key: String) {
  case InvalidConfigFile extends ConfigError("CONFIG_FILE")
  case InvalidHost extends ConfigError("HTTP_HOST")
  case InvalidPort extends ConfigError("HTTP_PORT")
  case InvalidAdmissionPermits extends ConfigError("HTTP_ADMISSION_PERMITS")
  case InvalidMongoUri extends ConfigError("MONGODB_URI")
  case InvalidMongoDatabase extends ConfigError("MONGODB_DATABASE")
  case InvalidLogLevel extends ConfigError("LOG_LEVEL")
  case InvalidMaskSensitive extends ConfigError("LOG_MASK_SENSITIVE")
  case UnsafeMaskSensitive extends ConfigError("LOG_MASK_SENSITIVE")
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
  case InvalidVectorNumCandidates extends ConfigError("VECTOR_NUM_CANDIDATES")
}

final case class VectorSearchConfig(
    enabled: Boolean,
    voyageApiKey: Option[String],
    voyageEndpoint: String,
    voyageModel: String,
    voyageDimension: Int,
    embeddingVersion: Int,
    queueSize: Int,
    parallelism: Int,
    timeoutMillis: Int,
    jobVectorIndex: String,
    candidateVectorIndex: String,
    numCandidates: Int
)

final case class JwtAuthConfig(
    hmacSecret: Option[String],
    issuer: String,
    audience: String
)

final case class AppConfig(
    host: String,
    port: Int,
    admissionPermits: Int,
    mongoUri: String,
    mongoDatabase: String,
    logLevel: String,
    maskSensitive: Boolean,
    jwtAuth: JwtAuthConfig,
    vectorSearch: VectorSearchConfig
) {
  override def toString: String = "AppConfig([REDACTED])"
}

object AppConfig {
  private val DefaultConfigResource = "application.conf"

  def load: IO[Either[ConfigError, AppConfig]] =
    load(Path.of("local.conf"), name => Option(System.getenv(name)))

  def load(localConfig: Path, env: String => Option[String]): IO[Either[ConfigError, AppConfig]] =
    IO.blocking {
      fromSources(localConfig, env)
    }

  def fromConfig(raw: String, env: Map[String, String], base: Option[AppConfig] = None): Either[ConfigError, AppConfig] =
    fromConfig(raw, name => env.get(name), base)

  private[config] def fromConfig(
      raw: String,
      env: String => Option[String],
      base: Option[AppConfig]
  ): Either[ConfigError, AppConfig] =
    base.fold(fromRawConfig(raw, "", env)) { current =>
      for {
        overrideConfig <- rawToConfig(raw)
        baseConfig <- configToRaw(current)
        merged <- Try(overrideConfig.withFallback(baseConfig).resolve(resolveOptions(env))).toEither
          .left.map(_ => ConfigError.InvalidConfigFile)
        loaded <- readAppConfig(merged)
      } yield loaded
    }

  private[config] def fromRawConfig(
      defaults: String,
      local: String,
      env: String => Option[String]
  ): Either[ConfigError, AppConfig] =
    for {
      defaultConfig <- rawToConfig(defaults)
      localConfig <- rawToConfig(local)
      merged <- Try(localConfig.withFallback(defaultConfig).resolve(resolveOptions(env))).toEither
        .left.map(_ => ConfigError.InvalidConfigFile)
      config <- readAppConfig(merged)
    } yield config

  private def fromSources(localConfig: Path, env: String => Option[String]): Either[ConfigError, AppConfig] =
    for {
      defaults <- Try(ConfigFactory.parseResources(DefaultConfigResource, parseOptions)).toEither.left.map(_ => ConfigError.InvalidConfigFile)
      local <- Try {
        if (Files.isRegularFile(localConfig)) ConfigFactory.parseFile(localConfig.toFile, parseOptions)
        else ConfigFactory.empty()
      }.toEither.left.map(_ => ConfigError.InvalidConfigFile)
      merged <- Try(local.withFallback(defaults).resolve(resolveOptions(env))).toEither
        .left.map(_ => ConfigError.InvalidConfigFile)
      config <- readAppConfig(merged)
    } yield config

  private def readAppConfig(config: com.typesafe.config.Config): Either[ConfigError, AppConfig] =
    ConfigSource.fromConfig(config).load[RawAppConfig].left.map(readError).flatMap(fromRaw)

  private def fromRaw(raw: RawAppConfig): Either[ConfigError, AppConfig] = {
    for {
      host = raw.http.host
      port = raw.http.port
      admissionPermits = raw.http.admissionPermits
      uri = raw.mongo.uri
      database = raw.mongo.database
      level = raw.logging.level
      maskSensitive = raw.logging.maskSensitive
      jwtSecret = raw.auth.jwt.hs256Secret.getOrElse("disabled")
      jwtIssuer = raw.auth.jwt.issuer
      jwtAudience = raw.auth.jwt.audience
      vectorEnabled = raw.vectorSearch.enabled
      voyageKey = raw.vectorSearch.voyage.apiKey.getOrElse("disabled")
      voyageEndpoint = raw.vectorSearch.voyage.endpoint
      voyageModel = raw.vectorSearch.voyage.model
      voyageDimension = raw.vectorSearch.voyage.dimension
      embeddingVersion = raw.vectorSearch.embedding.version
      queueSize = raw.vectorSearch.embedding.queueSize
      parallelism = raw.vectorSearch.embedding.parallelism
      timeout = raw.vectorSearch.embedding.timeoutMs
      jobVectorIndex = raw.vectorSearch.indexes.jobs
      candidateVectorIndex = raw.vectorSearch.indexes.candidates
      numCandidates = raw.vectorSearch.numCandidates
      address <- IpAddress.fromString(host).filter(_ => host.matches("[0-9a-fA-F:.]+"))
        .toRight(ConfigError.InvalidHost)
      validPort <- port.toIntOption.filter(value => value >= 1 && value <= 65535)
        .filter(_ => port.matches("[0-9]+")).toRight(ConfigError.InvalidPort)
      validAdmissionPermits <- admissionPermits.toIntOption.filter(value => value >= 1 && value <= 1024)
        .filter(_ => admissionPermits.matches("[0-9]+")).toRight(ConfigError.InvalidAdmissionPermits)
      _ <- Try(new ConnectionString(uri)).toEither.left.map(_ => ConfigError.InvalidMongoUri)
      _ <- Either.cond(
        database.nonEmpty && database.getBytes(java.nio.charset.StandardCharsets.UTF_8).length < 64 &&
          !database.exists(character => character.isWhitespace || character.isControl || "/\\.\"$*<>:|?".contains(character)),
        (),
        ConfigError.InvalidMongoDatabase
      )
      _ <- Either.cond(Set("TRACE", "DEBUG", "INFO", "WARN", "ERROR").contains(level), (), ConfigError.InvalidLogLevel)
      masking <- strictBoolean(maskSensitive, ConfigError.InvalidMaskSensitive)
      _ <- Either.cond(masking || address.isLoopback, (), ConfigError.UnsafeMaskSensitive)
      jwtSecretValue <- parseJwtSecret(jwtSecret)
      _ <- Either.cond(jwtIssuer.trim.nonEmpty && jwtIssuer.length <= 128, (), ConfigError.InvalidJwtIssuer)
      _ <- Either.cond(jwtAudience.trim.nonEmpty && jwtAudience.length <= 128, (), ConfigError.InvalidJwtAudience)
      enabled <- strictBoolean(vectorEnabled, ConfigError.InvalidVectorSearchEnabled)
      dimension <- voyageDimension.toIntOption.filter(_ == 1024)
        .toRight(ConfigError.InvalidVoyageDimension)
      version <- embeddingVersion.toIntOption.filter(_ > 0).toRight(ConfigError.InvalidEmbeddingVersion)
      queue <- queueSize.toIntOption.filter(value => value >= 1 && value <= 10000).toRight(ConfigError.InvalidEmbeddingQueueSize)
      workers <- parallelism.toIntOption.filter(value => value >= 1 && value <= 64).toRight(ConfigError.InvalidEmbeddingParallelism)
      timeoutMillis <- timeout.toIntOption.filter(value => value >= 100 && value <= 60000).toRight(ConfigError.InvalidEmbeddingTimeout)
      candidates <- numCandidates.toIntOption.filter(value => value >= 1 && value <= 10000).toRight(ConfigError.InvalidVectorNumCandidates)
      _ <- Either.cond(!enabled || voyageKey.trim.nonEmpty && voyageKey != "disabled", (), ConfigError.InvalidVoyageApiKey)
      _ <- Either.cond(voyageEndpoint.startsWith("https://"), (), ConfigError.InvalidVoyageEndpoint)
      _ <- Either.cond(voyageModel.trim.nonEmpty, (), ConfigError.InvalidVoyageModel)
      _ <- Either.cond(jobVectorIndex.trim.nonEmpty, (), ConfigError.InvalidJobVectorIndex)
      _ <- Either.cond(candidateVectorIndex.trim.nonEmpty, (), ConfigError.InvalidCandidateVectorIndex)
      vector = VectorSearchConfig(
        enabled,
        Option.when(voyageKey != "disabled")(voyageKey),
        voyageEndpoint,
        voyageModel,
        dimension,
        version,
        queue,
        workers,
        timeoutMillis,
        jobVectorIndex,
        candidateVectorIndex,
        candidates
      )
      jwtAuth = JwtAuthConfig(jwtSecretValue, jwtIssuer, jwtAudience)
    } yield AppConfig(host, validPort, validAdmissionPermits, uri, database, level, masking, jwtAuth, vector)
  }

  private def rawToConfig(raw: String): Either[ConfigError, com.typesafe.config.Config] =
    Try(ConfigFactory.parseString(raw, parseOptions)).toEither
      .left.map(_ => ConfigError.InvalidConfigFile)

  private def readError(failures: ConfigReaderFailures): ConfigError =
    failures.toList.collectFirst {
      case ConvertFailure(KeyNotFound(key, _), _, path) => configErrorForPath(fullPath(path, key))
    }.flatten.getOrElse(ConfigError.InvalidConfigFile)

  private def fullPath(path: String, key: String): String =
    Option(path).filter(_.nonEmpty).fold(key)(parent => s"$parent.$key")

  private def configErrorForPath(path: String): Option[ConfigError] =
    path match {
      case "http.host" => Some(ConfigError.InvalidHost)
      case "http.port" => Some(ConfigError.InvalidPort)
      case "http.admission-permits" => Some(ConfigError.InvalidAdmissionPermits)
      case "mongo.uri" => Some(ConfigError.InvalidMongoUri)
      case "mongo.database" => Some(ConfigError.InvalidMongoDatabase)
      case "logging.level" => Some(ConfigError.InvalidLogLevel)
      case "logging.mask-sensitive" => Some(ConfigError.InvalidMaskSensitive)
      case "auth.jwt.issuer" => Some(ConfigError.InvalidJwtIssuer)
      case "auth.jwt.audience" => Some(ConfigError.InvalidJwtAudience)
      case "vector-search.enabled" => Some(ConfigError.InvalidVectorSearchEnabled)
      case "vector-search.voyage.endpoint" => Some(ConfigError.InvalidVoyageEndpoint)
      case "vector-search.voyage.model" => Some(ConfigError.InvalidVoyageModel)
      case "vector-search.voyage.dimension" => Some(ConfigError.InvalidVoyageDimension)
      case "vector-search.embedding.version" => Some(ConfigError.InvalidEmbeddingVersion)
      case "vector-search.embedding.queue-size" => Some(ConfigError.InvalidEmbeddingQueueSize)
      case "vector-search.embedding.parallelism" => Some(ConfigError.InvalidEmbeddingParallelism)
      case "vector-search.embedding.timeout-ms" => Some(ConfigError.InvalidEmbeddingTimeout)
      case "vector-search.indexes.jobs" => Some(ConfigError.InvalidJobVectorIndex)
      case "vector-search.indexes.candidates" => Some(ConfigError.InvalidCandidateVectorIndex)
      case "vector-search.num-candidates" => Some(ConfigError.InvalidVectorNumCandidates)
      case _ => None
    }

  private def configToRaw(config: AppConfig): Either[ConfigError, com.typesafe.config.Config] =
    rawToConfig(
      s"""http {
         |  host = "${config.host}"
         |  port = ${config.port}
         |  admission-permits = ${config.admissionPermits}
         |}
         |mongo {
         |  uri = "${config.mongoUri}"
         |  database = "${config.mongoDatabase}"
         |}
         |logging {
         |  level = "${config.logLevel}"
         |  mask-sensitive = ${config.maskSensitive}
         |}
         |auth.jwt {
         |  hs256-secret = "${config.jwtAuth.hmacSecret.getOrElse("disabled")}"
         |  issuer = "${config.jwtAuth.issuer}"
         |  audience = "${config.jwtAuth.audience}"
         |}
         |vector-search {
         |  enabled = ${config.vectorSearch.enabled}
         |  voyage {
         |    api-key = "${config.vectorSearch.voyageApiKey.getOrElse("disabled")}"
         |    endpoint = "${config.vectorSearch.voyageEndpoint}"
         |    model = "${config.vectorSearch.voyageModel}"
         |    dimension = ${config.vectorSearch.voyageDimension}
         |  }
         |  embedding {
         |    version = ${config.vectorSearch.embeddingVersion}
         |    queue-size = ${config.vectorSearch.queueSize}
         |    parallelism = ${config.vectorSearch.parallelism}
         |    timeout-ms = ${config.vectorSearch.timeoutMillis}
         |  }
         |  indexes {
         |    jobs = "${config.vectorSearch.jobVectorIndex}"
         |    candidates = "${config.vectorSearch.candidateVectorIndex}"
         |  }
         |  num-candidates = ${config.vectorSearch.numCandidates}
         |}
         |""".stripMargin,
    )

  private val parseOptions: ConfigParseOptions =
    ConfigParseOptions.defaults().setAllowMissing(false)

  private def resolveOptions(env: String => Option[String]): ConfigResolveOptions = {
    val resolver: ConfigResolver = new ConfigResolver { self =>
      override def lookup(name: String): ConfigValue =
        env(name).map(ConfigValueFactory.fromAnyRef).orNull

      override def withFallback(fallback: ConfigResolver): ConfigResolver = new ConfigResolver {
        override def lookup(name: String): ConfigValue =
          Option(self.lookup(name)).getOrElse(fallback.lookup(name))

        override def withFallback(next: ConfigResolver): ConfigResolver =
          this.withFallback(fallback.withFallback(next))
      }
    }
    ConfigResolveOptions.noSystem().appendResolver(resolver)
  }

  private final case class RawAppConfig(
      http: RawHttpConfig,
      mongo: RawMongoConfig,
      logging: RawLoggingConfig,
      auth: RawAuthConfig,
      vectorSearch: RawVectorSearchConfig
  )

  private final case class RawHttpConfig(host: String, port: String, admissionPermits: String)
  private final case class RawMongoConfig(uri: String, database: String)
  private final case class RawLoggingConfig(level: String, maskSensitive: String)
  private final case class RawAuthConfig(jwt: RawJwtAuthConfig)
  private final case class RawJwtAuthConfig(hs256Secret: Option[String], issuer: String, audience: String)
  private final case class RawVectorSearchConfig(
      enabled: String,
      voyage: RawVoyageConfig,
      embedding: RawEmbeddingConfig,
      indexes: RawVectorIndexesConfig,
      numCandidates: String
  )
  private final case class RawVoyageConfig(apiKey: Option[String], endpoint: String, model: String, dimension: String)
  private final case class RawEmbeddingConfig(
      version: String,
      queueSize: String,
      parallelism: String,
      timeoutMs: String
  )
  private final case class RawVectorIndexesConfig(jobs: String, candidates: String)

  private given ConfigReader[String] =
    ConfigReader.fromCursor(_.asConfigValue.map(_.unwrapped.toString))

  private given ConfigReader[RawAppConfig] =
    ConfigReader.forProduct5("http", "mongo", "logging", "auth", "vector-search")(RawAppConfig.apply)
  private given ConfigReader[RawHttpConfig] =
    ConfigReader.forProduct3("host", "port", "admission-permits")(RawHttpConfig.apply)
  private given ConfigReader[RawMongoConfig] =
    ConfigReader.forProduct2("uri", "database")(RawMongoConfig.apply)
  private given ConfigReader[RawLoggingConfig] =
    ConfigReader.forProduct2("level", "mask-sensitive")(RawLoggingConfig.apply)
  private given ConfigReader[RawAuthConfig] =
    ConfigReader.forProduct1("jwt")(RawAuthConfig.apply)
  private given ConfigReader[RawJwtAuthConfig] =
    ConfigReader.forProduct3("hs256-secret", "issuer", "audience")(RawJwtAuthConfig.apply)
  private given ConfigReader[RawVectorSearchConfig] =
    ConfigReader.forProduct5("enabled", "voyage", "embedding", "indexes", "num-candidates")(RawVectorSearchConfig.apply)
  private given ConfigReader[RawVoyageConfig] =
    ConfigReader.forProduct4("api-key", "endpoint", "model", "dimension")(RawVoyageConfig.apply)
  private given ConfigReader[RawEmbeddingConfig] =
    ConfigReader.forProduct4("version", "queue-size", "parallelism", "timeout-ms")(RawEmbeddingConfig.apply)
  private given ConfigReader[RawVectorIndexesConfig] =
    ConfigReader.forProduct2("jobs", "candidates")(RawVectorIndexesConfig.apply)

  private def strictBoolean(value: String, error: ConfigError): Either[ConfigError, Boolean] = value match {
    case "true" => Right(true)
    case "false" => Right(false)
    case _ => Left(error)
  }

  private def parseJwtSecret(value: String): Either[ConfigError, Option[String]] =
    if (value == "disabled") Right(None)
    else if (value.getBytes(StandardCharsets.UTF_8).length >= 32) Right(Some(value))
    else Left(ConfigError.InvalidJwtSecret)
}
