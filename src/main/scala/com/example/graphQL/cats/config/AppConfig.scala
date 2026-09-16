package com.example.graphQL.cats.config

import cats.effect.IO
import com.comcast.ip4s.IpAddress
import com.mongodb.ConnectionString
import scala.util.Try

enum ConfigError(val key: String) {
  case InvalidHost extends ConfigError("HTTP_HOST")
  case InvalidPort extends ConfigError("HTTP_PORT")
  case InvalidMongoUri extends ConfigError("MONGODB_URI")
  case InvalidMongoDatabase extends ConfigError("MONGODB_DATABASE")
  case InvalidLogLevel extends ConfigError("LOG_LEVEL")
  case InvalidAppEnv extends ConfigError("APP_ENV")
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
    appEnv: String = "production",
    maskSensitive: Boolean = true,
    requestPayloads: Boolean = false
) {
  override def toString: String = "AppConfig([REDACTED])"
}

object AppConfig {
  def load: IO[Either[ConfigError, AppConfig]] = IO.delay(fromEnvironment(sys.env))

  def fromEnvironment(env: Map[String, String]): Either[ConfigError, AppConfig] = {
    val host = env.getOrElse("HTTP_HOST", "127.0.0.1")
    val port = env.getOrElse("HTTP_PORT", "8080")
    val uri = env.getOrElse("MONGODB_URI", "mongodb://127.0.0.1:27017")
    val database = env.getOrElse("MONGODB_DATABASE", "hiring")
    val level = env.getOrElse("LOG_LEVEL", "INFO")
    val appEnv = env.getOrElse("APP_ENV", "production")
    val maskSensitive = env.getOrElse("LOG_MASK_SENSITIVE", "true")
    val requestPayloads = env.getOrElse("LOG_REQUEST_PAYLOADS", "false")

    for {
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
      _ <- Either.cond(Set("production", "local").contains(appEnv), (), ConfigError.InvalidAppEnv)
      masking <- strictBoolean(maskSensitive, ConfigError.InvalidMaskSensitive)
      payloads <- strictBoolean(requestPayloads, ConfigError.InvalidRequestPayloads)
      _ <- Either.cond(masking || (appEnv == "local" && address.isLoopback), (), ConfigError.UnsafeMaskSensitive)
      _ <- Either.cond(!payloads || (!masking && appEnv == "local" && address.isLoopback), (), ConfigError.UnsafeRequestPayloads)
    } yield AppConfig(host, validPort, uri, database, level, appEnv, masking, payloads)
  }

  private def strictBoolean(value: String, error: ConfigError): Either[ConfigError, Boolean] = value match {
    case "true" => Right(true)
    case "false" => Right(false)
    case _ => Left(error)
  }
}
