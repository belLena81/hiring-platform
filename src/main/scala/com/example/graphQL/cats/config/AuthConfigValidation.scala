package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*
import java.nio.charset.StandardCharsets

private[config] object AuthConfigValidation {
  def read(auth: RawAuthConfig): ValidatedNel[ConfigError, AuthSettings] =
    validJwtSecret(auth.jwt.hs256Secret).map { secret =>
      val passwordHash = auth.passwordHash.getOrElse(defaultPasswordHash)
      AuthSettings(
        JwtAuthConfig(secret, auth.jwt.issuer, auth.jwt.audience, cursorTtlSeconds = auth.jwt.cursorTtlSeconds.toLong),
        PasswordHashConfig(passwordHash.iterations, passwordHash.memoryKib, passwordHash.parallelism),
        AuthRateLimitConfig(auth.rateLimit.windowSeconds, auth.rateLimit.attempts, auth.rateLimit.maxBuckets)
      )
    }

  def validJwtSecret(value: Option[String]): ValidatedNel[ConfigError, String] =
    value.filter(_ != "disabled").filter(_.trim.nonEmpty).fold(ConfigError.InvalidJwtSecret.invalidNel[String]) {
      secret =>
        Either
          .cond(secret.getBytes(StandardCharsets.UTF_8).length >= 32, secret, ConfigError.InvalidJwtSecret)
          .toValidatedNel
    }
  def validCursorTtl(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(60, 86400, ConfigError.InvalidCursorTtl)(value)
  def validAuthRateLimitWindow(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(1, 3600, ConfigError.InvalidAuthRateLimitWindow)(value)
  def validAuthRateLimitAttempts(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(1, 1000, ConfigError.InvalidAuthRateLimitAttempts)(value)
  def validAuthRateLimitBuckets(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(1, 100000, ConfigError.InvalidAuthRateLimitBuckets)(value)
  def validPasswordHashIterations(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(1, 10, ConfigError.InvalidPasswordHashIterations)(value)
  def validPasswordHashMemory(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(8192, 1048576, ConfigError.InvalidPasswordHashMemory)(value)
  def validPasswordHashParallelism(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(1, 16, ConfigError.InvalidPasswordHashParallelism)(value)
}
