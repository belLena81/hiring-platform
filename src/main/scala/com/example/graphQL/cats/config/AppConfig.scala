package com.example.graphQL.cats.config

import cats.data.NonEmptyList
import cats.effect.IO
import com.comcast.ip4s.{Host, Port as Ip4sPort}
import _root_.pureconfig.*
import scala.concurrent.duration.FiniteDuration

final case class AppConfig(
    host: Host,
    port: Ip4sPort,
    admissionPermits: Int,
    requestTimeout: FiniteDuration,
    trustedProxy: TrustedProxyConfig,
    mongoUri: String,
    mongoDatabase: String,
    maskSensitive: Boolean,
    jwtAuth: JwtAuthConfig,
    passwordHash: PasswordHashConfig,
    authRateLimit: AuthRateLimitConfig,
    vectorSearch: VectorSearchConfig,
    kafka: KafkaConfig,
    resetOnStart: Boolean = false
) {
  override def toString: String = "AppConfig([REDACTED])"
}

object AppConfig {
  def load: IO[Either[NonEmptyList[ConfigError], AppConfig]] =
    IO.blocking(AppConfigValidation.read(ConfigSource.default))

  def loadMaskSensitive: IO[Boolean] =
    IO.blocking(ConfigSource.default.at("logging").load[RawLoggingConfig].toOption.forall(_.maskSensitive))

  def fromConfig(raw: String, env: Map[String, String]): Either[NonEmptyList[ConfigError], AppConfig] =
    AppConfigValidation.fromConfig(raw, env)
}
