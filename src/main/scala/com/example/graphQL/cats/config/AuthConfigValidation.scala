package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*
import java.nio.charset.StandardCharsets

private[config] object AuthConfigValidation {
  def read(auth: RawAuthConfig): ValidatedNel[ConfigError, AuthSettings] =
    (
      validJwtSecret(auth.jwt.hs256Secret),
      validReceiptSecret(auth.jwt.receiptFingerprintSecret),
      validAdminSeed(auth.adminSeed.getOrElse(AdminSeedConfig()))
    ).mapN { (secret, receiptSecret, seed) =>
      AuthSettings(
        JwtAuthConfig(
          secret,
          auth.jwt.issuer,
          auth.jwt.audience,
          cursorTtlSeconds = auth.jwt.cursorTtlSeconds.toLong,
          receiptFingerprintSecret = receiptSecret
        ),
        auth.passwordHash.getOrElse(PasswordHashConfig()),
        auth.rateLimit,
        auth.interviewActionRateLimit,
        seed
      )
    }

  private def validAdminSeed(seed: AdminSeedConfig): ValidatedNel[ConfigError, AdminSeedConfig] = {
    val validName = seed.name.exists { value =>
      val normalized = java.text.Normalizer.normalize(value.trim, java.text.Normalizer.Form.NFKC)
      normalized.nonEmpty && normalized.length <= com.example.graphQL.cats.domain.model.FieldLimits.ShortTextMaxChars
    }
    val validPassword = seed.password.exists { value =>
      val bytes = value.getBytes(StandardCharsets.UTF_8).length
      bytes >= 12 && bytes <= com.example.graphQL.cats.domain.model.FieldLimits.PasswordMaxBytes
    }
    val valid = !seed.enabled || (validName && validPassword)
    Either.cond(valid, seed, ConfigError.InvalidAdminSeed).toValidatedNel
  }

  def validJwtSecret(value: Option[String]): ValidatedNel[ConfigError, String] =
    value.filter(_ != "disabled").filter(_.trim.nonEmpty).fold(ConfigError.InvalidJwtSecret.invalidNel[String]) {
      secret =>
        Either
          .cond(secret.getBytes(StandardCharsets.UTF_8).length >= 32, secret, ConfigError.InvalidJwtSecret)
          .toValidatedNel
    }

  /** Optional; when set it must meet the same strength floor as the JWT secret. */
  private def validReceiptSecret(value: Option[String]): ValidatedNel[ConfigError, Option[String]] =
    value.filter(_.trim.nonEmpty).fold(Option.empty[String].validNel[ConfigError]) { secret =>
      Either
        .cond(
          secret.getBytes(StandardCharsets.UTF_8).length >= 32,
          Some(secret),
          ConfigError.InvalidReceiptFingerprintSecret
        )
        .toValidatedNel
    }
}
