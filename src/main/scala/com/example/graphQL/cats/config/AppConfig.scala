package com.example.graphQL.cats.config

import cats.effect.IO
import cats.syntax.all.*
import com.comcast.ip4s.IpAddress
import com.mongodb.ConnectionString
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.util.{Try, Using}

enum ConfigError(val key: String) {
  case InvalidConfigFile extends ConfigError("CONFIG_FILE")
  case InvalidHost extends ConfigError("HTTP_HOST")
  case InvalidPort extends ConfigError("HTTP_PORT")
  case InvalidMongoUri extends ConfigError("MONGODB_URI")
  case InvalidMongoDatabase extends ConfigError("MONGODB_DATABASE")
  case InvalidLogLevel extends ConfigError("LOG_LEVEL")
  case InvalidMaskSensitive extends ConfigError("LOG_MASK_SENSITIVE")
  case InvalidRequestPayloads extends ConfigError("LOG_REQUEST_PAYLOADS")
  case UnsafeMaskSensitive extends ConfigError("LOG_MASK_SENSITIVE")
  case UnsafeRequestPayloads extends ConfigError("LOG_REQUEST_PAYLOADS")
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
    mongoUri: String,
    mongoDatabase: String,
    logLevel: String,
    maskSensitive: Boolean,
    requestPayloads: Boolean,
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
      loadDefaultConfig().flatMap { defaults =>
        val local = if (Files.isRegularFile(localConfig)) Files.readString(localConfig, StandardCharsets.UTF_8) else ""
        fromConfig(defaults, env, None).flatMap(defaultConfig => fromConfig(local, env, defaultConfig.some))
      }
    }

  def fromConfig(raw: String, env: Map[String, String], base: Option[AppConfig] = None): Either[ConfigError, AppConfig] =
    fromConfig(raw, name => env.get(name), base)

  private[config] def fromConfig(
      raw: String,
      env: String => Option[String],
      base: Option[AppConfig]
  ): Either[ConfigError, AppConfig] =
    for {
      parsed <- parse(raw)
      resolved <- resolve(parsed, env)
      merged = base.fold(resolved)(configToEntries(_) ++ resolved)
      config <- fromEntries(merged)
    } yield config

  private def fromEntries(entries: Map[String, String]): Either[ConfigError, AppConfig] = {
    for {
      host <- entries.get("HTTP_HOST").toRight(ConfigError.InvalidHost)
      port <- entries.get("HTTP_PORT").toRight(ConfigError.InvalidPort)
      uri <- entries.get("MONGODB_URI").toRight(ConfigError.InvalidMongoUri)
      database <- entries.get("MONGODB_DATABASE").toRight(ConfigError.InvalidMongoDatabase)
      level <- entries.get("LOG_LEVEL").toRight(ConfigError.InvalidLogLevel)
      maskSensitive <- entries.get("LOG_MASK_SENSITIVE").toRight(ConfigError.InvalidMaskSensitive)
      requestPayloads <- entries.get("LOG_REQUEST_PAYLOADS").toRight(ConfigError.InvalidRequestPayloads)
      jwtSecret <- entries.get("AUTH_JWT_HS256_SECRET").toRight(ConfigError.InvalidJwtSecret)
      jwtIssuer <- entries.get("AUTH_JWT_ISSUER").toRight(ConfigError.InvalidJwtIssuer)
      jwtAudience <- entries.get("AUTH_JWT_AUDIENCE").toRight(ConfigError.InvalidJwtAudience)
      vectorEnabled <- entries.get("VECTOR_SEARCH_ENABLED").toRight(ConfigError.InvalidVectorSearchEnabled)
      voyageKey <- entries.get("VOYAGE_API_KEY").toRight(ConfigError.InvalidVoyageApiKey)
      voyageEndpoint <- entries.get("VOYAGE_ENDPOINT").toRight(ConfigError.InvalidVoyageEndpoint)
      voyageModel <- entries.get("VOYAGE_MODEL").toRight(ConfigError.InvalidVoyageModel)
      voyageDimension <- entries.get("VOYAGE_DIMENSION").toRight(ConfigError.InvalidVoyageDimension)
      embeddingVersion <- entries.get("EMBEDDING_VERSION").toRight(ConfigError.InvalidEmbeddingVersion)
      queueSize <- entries.get("EMBEDDING_QUEUE_SIZE").toRight(ConfigError.InvalidEmbeddingQueueSize)
      parallelism <- entries.get("EMBEDDING_PARALLELISM").toRight(ConfigError.InvalidEmbeddingParallelism)
      timeout <- entries.get("EMBEDDING_TIMEOUT_MS").toRight(ConfigError.InvalidEmbeddingTimeout)
      jobVectorIndex <- entries.get("JOB_VECTOR_INDEX").toRight(ConfigError.InvalidJobVectorIndex)
      candidateVectorIndex <- entries.get("CANDIDATE_VECTOR_INDEX").toRight(ConfigError.InvalidCandidateVectorIndex)
      numCandidates <- entries.get("VECTOR_NUM_CANDIDATES").toRight(ConfigError.InvalidVectorNumCandidates)
      address <- IpAddress.fromString(host).filter(_ => host.matches("[0-9a-fA-F:.]+"))
        .toRight(ConfigError.InvalidHost)
      validPort <- port.toIntOption.filter(value => value >= 1 && value <= 65535)
        .filter(_ => port.matches("[0-9]+")).toRight(ConfigError.InvalidPort)
      _ <- Try(new ConnectionString(uri)).toEither.left.map(_ => ConfigError.InvalidMongoUri)
      _ <- Either.cond(
        database.nonEmpty && database.getBytes(java.nio.charset.StandardCharsets.UTF_8).length < 64 &&
          !database.exists(character => character.isWhitespace || character.isControl || "/\\.\"$*<>:|?".contains(character)),
        (),
        ConfigError.InvalidMongoDatabase
      )
      _ <- Either.cond(Set("INFO", "WARN", "ERROR").contains(level), (), ConfigError.InvalidLogLevel)
      masking <- strictBoolean(maskSensitive, ConfigError.InvalidMaskSensitive)
      payloads <- strictBoolean(requestPayloads, ConfigError.InvalidRequestPayloads)
      _ <- Either.cond(masking || address.isLoopback, (), ConfigError.UnsafeMaskSensitive)
      _ <- Either.cond(!payloads || (!masking && address.isLoopback), (), ConfigError.UnsafeRequestPayloads)
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
    } yield AppConfig(host, validPort, uri, database, level, masking, payloads, jwtAuth, vector)
  }

  private def parse(raw: String): Either[ConfigError, Map[String, String]] =
    raw.linesIterator.zipWithIndex.foldLeft[Either[ConfigError, Map[String, String]]](Right(Map.empty)) {
      case (Left(error), _) => Left(error)
      case (Right(entries), (line, _)) =>
        val trimmed = line.trim
        if (trimmed.isEmpty || trimmed.startsWith("#") || trimmed.startsWith("//")) Right(entries)
        else {
          val separator = List(trimmed.indexOf('='), trimmed.indexOf(':')).filter(_ >= 0).minOption
          separator match {
            case None => Left(ConfigError.InvalidConfigFile)
            case Some(index) =>
              val key = trimmed.take(index).trim
              val value = unquote(trimmed.drop(index + 1).trim)
              if (key.isEmpty || value.isEmpty) Left(ConfigError.InvalidConfigFile)
              else Right(entries + (key -> value))
          }
        }
    }

  private def resolve(entries: Map[String, String], env: String => Option[String]): Either[ConfigError, Map[String, String]] =
    entries.toList.foldLeft[Either[ConfigError, Map[String, String]]](Right(Map.empty)) {
      case (Left(error), _) => Left(error)
      case (Right(resolved), (key, value)) =>
        resolveValue(key, value, env).map(resolvedValue => resolved + (key -> resolvedValue))
    }

  private def resolveValue(key: String, value: String, env: String => Option[String]): Either[ConfigError, String] =
    value match {
      case EnvReference(name) => env(name).toRight(errorFor(key))
      case _ => Right(value)
    }

  private def unquote(value: String): String =
    if (value.length >= 2 && ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'"))))
      value.substring(1, value.length - 1)
    else value

  private def configToEntries(config: AppConfig): Map[String, String] = Map(
    "HTTP_HOST" -> config.host,
    "HTTP_PORT" -> config.port.toString,
    "MONGODB_URI" -> config.mongoUri,
    "MONGODB_DATABASE" -> config.mongoDatabase,
    "LOG_LEVEL" -> config.logLevel,
    "LOG_MASK_SENSITIVE" -> config.maskSensitive.toString,
    "LOG_REQUEST_PAYLOADS" -> config.requestPayloads.toString,
    "AUTH_JWT_HS256_SECRET" -> config.jwtAuth.hmacSecret.getOrElse("disabled"),
    "AUTH_JWT_ISSUER" -> config.jwtAuth.issuer,
    "AUTH_JWT_AUDIENCE" -> config.jwtAuth.audience,
    "VECTOR_SEARCH_ENABLED" -> config.vectorSearch.enabled.toString,
    "VOYAGE_API_KEY" -> config.vectorSearch.voyageApiKey.getOrElse("disabled"),
    "VOYAGE_ENDPOINT" -> config.vectorSearch.voyageEndpoint,
    "VOYAGE_MODEL" -> config.vectorSearch.voyageModel,
    "VOYAGE_DIMENSION" -> config.vectorSearch.voyageDimension.toString,
    "EMBEDDING_VERSION" -> config.vectorSearch.embeddingVersion.toString,
    "EMBEDDING_QUEUE_SIZE" -> config.vectorSearch.queueSize.toString,
    "EMBEDDING_PARALLELISM" -> config.vectorSearch.parallelism.toString,
    "EMBEDDING_TIMEOUT_MS" -> config.vectorSearch.timeoutMillis.toString,
    "JOB_VECTOR_INDEX" -> config.vectorSearch.jobVectorIndex,
    "CANDIDATE_VECTOR_INDEX" -> config.vectorSearch.candidateVectorIndex,
    "VECTOR_NUM_CANDIDATES" -> config.vectorSearch.numCandidates.toString
  )

  private val EnvReference = """\{\$([A-Z0-9_]+)\}""".r

  private def errorFor(key: String): ConfigError =
    key match {
      case "HTTP_HOST" => ConfigError.InvalidHost
      case "HTTP_PORT" => ConfigError.InvalidPort
      case "MONGODB_URI" => ConfigError.InvalidMongoUri
      case "MONGODB_DATABASE" => ConfigError.InvalidMongoDatabase
      case "LOG_LEVEL" => ConfigError.InvalidLogLevel
      case "LOG_MASK_SENSITIVE" => ConfigError.InvalidMaskSensitive
      case "LOG_REQUEST_PAYLOADS" => ConfigError.InvalidRequestPayloads
      case "AUTH_JWT_HS256_SECRET" => ConfigError.InvalidJwtSecret
      case "AUTH_JWT_ISSUER" => ConfigError.InvalidJwtIssuer
      case "AUTH_JWT_AUDIENCE" => ConfigError.InvalidJwtAudience
      case "VECTOR_SEARCH_ENABLED" => ConfigError.InvalidVectorSearchEnabled
      case "VOYAGE_API_KEY" => ConfigError.InvalidVoyageApiKey
      case "VOYAGE_ENDPOINT" => ConfigError.InvalidVoyageEndpoint
      case "VOYAGE_MODEL" => ConfigError.InvalidVoyageModel
      case "VOYAGE_DIMENSION" => ConfigError.InvalidVoyageDimension
      case "EMBEDDING_VERSION" => ConfigError.InvalidEmbeddingVersion
      case "EMBEDDING_QUEUE_SIZE" => ConfigError.InvalidEmbeddingQueueSize
      case "EMBEDDING_PARALLELISM" => ConfigError.InvalidEmbeddingParallelism
      case "EMBEDDING_TIMEOUT_MS" => ConfigError.InvalidEmbeddingTimeout
      case "JOB_VECTOR_INDEX" => ConfigError.InvalidJobVectorIndex
      case "CANDIDATE_VECTOR_INDEX" => ConfigError.InvalidCandidateVectorIndex
      case "VECTOR_NUM_CANDIDATES" => ConfigError.InvalidVectorNumCandidates
      case _ => ConfigError.InvalidConfigFile
    }

  private def loadDefaultConfig(): Either[ConfigError, String] =
    Option(Thread.currentThread().getContextClassLoader.getResourceAsStream(DefaultConfigResource)).flatMap { stream =>
      Using(stream)(input => String(input.readAllBytes(), StandardCharsets.UTF_8)).toOption
    }.toRight(ConfigError.InvalidConfigFile)

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
