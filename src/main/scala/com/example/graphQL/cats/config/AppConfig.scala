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
    authRateLimit: RateLimitConfig,
    vectorSearch: VectorSearchConfig,
    kafka: KafkaConfig,
    resetOnStart: Boolean = false,
    discovery: DiscoveryConfig = DiscoveryConfig(),
    adminSeed: AdminSeedConfig = AdminSeedConfig(),
    interviewActionRateLimit: RateLimitConfig,
    resilience: ResilienceConfig = ResilienceConfig(),
    graphql: GraphQLConfig = GraphQLConfig(),
    recruiterJobClosure: RecruiterJobClosureConfig = RecruiterJobClosureConfig(),
    readiness: ReadinessConfig = ReadinessConfig()
) {
  override def toString: String = "AppConfig([REDACTED])"
}

object AppConfig {
  def load: IO[Either[NonEmptyList[ConfigError], AppConfig]] =
    IO.blocking(AppConfigValidation.read(ConfigSource.default))

  /** Resolves the default configuration once: the leniently read masking flag, so invalid files still log safely, and
    * the validated configuration.
    */
  def loadWithMask: IO[(Boolean, Either[NonEmptyList[ConfigError], AppConfig])] =
    IO.blocking(
      ConfigSource.default
        .config()
        .fold(
          _ => (true, Left(NonEmptyList.one(ConfigError.InvalidConfigFile))),
          resolved => AppConfigValidation.readWithMask(ConfigSource.fromConfig(resolved))
        )
    )
}
