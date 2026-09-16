package com.example.graphQL.cats.config

import cats.effect.IO
import com.comcast.ip4s.IpAddress
import com.mongodb.ConnectionString
import scala.util.Try

enum ConfigError {
  case InvalidHost, InvalidPort, InvalidMongoUri, InvalidMongoDatabase, InvalidLogLevel
}

final case class AppConfig(
    host: String,
    port: Int,
    mongoUri: String,
    mongoDatabase: String,
    logLevel: String
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

    for {
      _ <- Either.cond(
        host.matches("[0-9a-fA-F:.]+") && IpAddress.fromString(host).isDefined,
        (),
        ConfigError.InvalidHost
      )
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
    } yield AppConfig(host, validPort, uri, database, level)
  }
}
