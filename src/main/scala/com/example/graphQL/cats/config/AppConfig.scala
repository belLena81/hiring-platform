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
}

final case class AppConfig(
    host: String,
    port: Int,
    mongoUri: String,
    mongoDatabase: String,
    logLevel: String,
    maskSensitive: Boolean,
    requestPayloads: Boolean
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
    } yield AppConfig(host, validPort, uri, database, level, masking, payloads)
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
    "LOG_REQUEST_PAYLOADS" -> config.requestPayloads.toString
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
}
