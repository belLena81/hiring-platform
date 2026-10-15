package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*

private[config] object ResilienceConfigValidation {
  private val Thresholds = 1 to 1000
  private val Durations = 100 to 3600000
  private val Probes = 1 to 16

  def read(raw: RawResilienceConfig): ValidatedNel[ConfigError, ResilienceConfig] = {
    val breakers = raw.breakers
    (
      validBreaker(breakers.voyage),
      validBreaker(breakers.atlasSearch),
      validBreaker(breakers.kafkaPublisher),
      validBreaker(breakers.mongoWrites)
    ).mapN(ResilienceConfig.apply)
  }

  /** Every field range and the `open-ms <= max-open-ms` rule are checked, and all violations accumulate. */
  def validBreaker(raw: RawCircuitBreakerConfig): ValidatedNel[ConfigError, CircuitBreakerConfig] =
    (
      within(raw.failureThreshold, Thresholds, ConfigError.InvalidBreakerFailureThreshold),
      within(raw.windowMs, Durations, ConfigError.InvalidBreakerWindow),
      within(raw.openMs, Durations, ConfigError.InvalidBreakerOpen),
      within(raw.maxOpenMs, Durations, ConfigError.InvalidBreakerMaxOpen),
      within(raw.halfOpenProbes, Probes, ConfigError.InvalidBreakerHalfOpenProbes),
      Either.cond(raw.openMs <= raw.maxOpenMs, (), ConfigError.InvalidBreakerOpenWindow).toValidatedNel
    ).mapN((threshold, window, open, maxOpen, probes, _) =>
      CircuitBreakerConfig(raw.enabled, threshold, window, open, maxOpen, probes)
    )

  private def within(value: Int, range: Range, error: ConfigError): ValidatedNel[ConfigError, Int] =
    Either.cond(range.contains(value), value, error).toValidatedNel
}
