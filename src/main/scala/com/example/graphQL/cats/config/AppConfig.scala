package com.example.graphQL.cats.config

import cats.data.NonEmptyList
import cats.effect.IO
import com.comcast.ip4s.{Host, Port as Ip4sPort}
import com.typesafe.config.Config
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
    resetOnStart: Boolean = false,
    discovery: DiscoveryConfig = DiscoveryConfig(),
    adminSeed: AdminSeedConfig = AdminSeedConfig(),
    interviewActionRateLimit: InterviewActionRateLimitConfig
) {
  override def toString: String = "AppConfig([REDACTED])"
}

object AppConfig {
  def load: IO[Either[NonEmptyList[ConfigError], AppConfig]] =
    IO.blocking(AppConfigValidation.read(ConfigSource.default))

  def loadMaskSensitive: IO[Boolean] =
    IO.blocking(ConfigSource.default.at("logging").load[RawLoggingConfig].toOption.forall(_.maskSensitive))

  /** Parses HOCON text without resolving substitutions; malformed text is a typed failure. */
  private[config] def parse(raw: String): Either[NonEmptyList[ConfigError], Config] = AppConfigValidation.parse(raw)

  /** Resolves substitutions against the given config only (no system properties or environment) and validates. */
  private[config] def fromParsed(config: Config): Either[NonEmptyList[ConfigError], AppConfig] =
    AppConfigValidation.fromParsed(config)
}
