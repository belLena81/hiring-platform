package com.example.graphQL.cats.config

import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.*
import io.github.iltotore.iron.constraint.numeric.*
import io.github.iltotore.iron.pureconfig.given
import _root_.pureconfig.*

private object AuthSettingReaders {
  given ConfigReader[PasswordHashConfig] =
    ConfigReader.forProduct3("iterations", "memory-kib", "parallelism")(
      (iterations: PasswordHashIterations, memoryKib: PasswordHashMemoryKib, parallelism: PasswordHashParallelism) =>
        PasswordHashConfig(iterations, memoryKib, parallelism)
    )
  given ConfigReader[AuthRateLimitConfig] =
    ConfigReader.forProduct3("window-seconds", "attempts", "max-buckets")(
      (window: AuthRateWindowSeconds, attempts: AuthRateAttempts, buckets: AuthRateBuckets) =>
        AuthRateLimitConfig(window, attempts, buckets)
    )
  given ConfigReader[InterviewActionRateLimitConfig] =
    ConfigReader.forProduct3("window-seconds", "attempts", "max-buckets")(
      (window: AuthRateWindowSeconds, attempts: AuthRateAttempts, buckets: AuthRateBuckets) =>
        InterviewActionRateLimitConfig(window, attempts, buckets)
    )
}
import AuthSettingReaders.given

private[config] final case class RawAuthConfig(
    jwt: RawJwtAuthConfig,
    passwordHash: Option[PasswordHashConfig],
    rateLimit: AuthRateLimitConfig,
    interviewActionRateLimit: InterviewActionRateLimitConfig,
    adminSeed: Option[AdminSeedConfig]
) derives ConfigReader
private[config] final case class RawJwtAuthConfig(
    hs256Secret: Option[String],
    receiptFingerprintSecret: Option[String],
    issuer: NonBlank128,
    audience: NonBlank128,
    cursorTtlSeconds: CursorTtlSeconds
) {
  override def toString: String = "RawJwtAuthConfig([REDACTED])"
}
private[config] object RawJwtAuthConfig {
  given ConfigReader[RawJwtAuthConfig] =
    ConfigReader.forProduct5(
      "hs256-secret",
      "receipt-fingerprint-secret",
      "issuer",
      "audience",
      "cursor-ttl-seconds"
    )(RawJwtAuthConfig.apply)
}
