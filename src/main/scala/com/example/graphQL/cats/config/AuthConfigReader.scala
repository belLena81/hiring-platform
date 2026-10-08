package com.example.graphQL.cats.config

import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.*
import io.github.iltotore.iron.constraint.numeric.*
import io.github.iltotore.iron.pureconfig.given
import _root_.pureconfig.*

private[config] final case class RawAuthConfig(
    jwt: RawJwtAuthConfig,
    passwordHash: Option[RawPasswordHashConfig],
    rateLimit: RawAuthRateLimitConfig,
    adminSeed: Option[RawAdminSeedConfig]
) derives ConfigReader
private[config] final case class RawJwtAuthConfig(
    hs256Secret: Option[String],
    issuer: NonBlank128,
    audience: NonBlank128,
    cursorTtlSeconds: CursorTtlSeconds
)
private[config] object RawJwtAuthConfig {
  given ConfigReader[RawJwtAuthConfig] =
    ConfigReader.forProduct4("hs256-secret", "issuer", "audience", "cursor-ttl-seconds")(RawJwtAuthConfig.apply)
}
private[config] final case class RawAuthRateLimitConfig(
    windowSeconds: AuthRateWindowSeconds,
    attempts: AuthRateAttempts,
    maxBuckets: AuthRateBuckets
) derives ConfigReader
private[config] final case class RawPasswordHashConfig(
    iterations: PasswordHashIterations,
    memoryKib: PasswordHashMemoryKib,
    parallelism: PasswordHashParallelism
) derives ConfigReader
private[config] val defaultPasswordHash = RawPasswordHashConfig(iterations = 2, memoryKib = 19456, parallelism = 1)

private[config] final case class RawAdminSeedConfig(enabled: Boolean, name: Option[String], password: Option[String])
    derives ConfigReader {
  override def toString: String = "RawAdminSeedConfig([REDACTED])"
}
