package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*

private[config] object HttpMongoConfigValidation {
  def read(http: RawHttpConfig, mongo: RawMongoConfig): ValidatedNel[ConfigError, TransportSettings] =
    validDiscovery(mongo.discovery.getOrElse(DiscoveryConfig()), http.requestTimeoutMs).map { discovery =>
      TransportSettings(
        http.host,
        http.port,
        http.admissionPermits,
        http.requestTimeoutMs.millis,
        http.trustedProxyCidrs,
        mongo.uri.getConnectionString,
        mongo.database,
        discovery,
        mongo.resetOnStart.contains(true)
      )
    }

  /** Per-field bounds are decoded by their refined types; only the HTTP-deadline relation is checked here. */
  def validDiscovery(discovery: DiscoveryConfig, requestTimeoutMs: Int): ValidatedNel[ConfigError, DiscoveryConfig] =
    Either
      .cond(discovery.maxTimeMillis < requestTimeoutMs, discovery, ConfigError.InvalidDiscoveryDeadline)
      .toValidatedNel

  def validMongoDatabase(value: String): ValidatedNel[ConfigError, String] =
    Either
      .cond(
        value.nonEmpty && value.getBytes(StandardCharsets.UTF_8).length < 64 &&
          !value.exists(c => c.isWhitespace || c.isControl || "/\\.\"$*<>:|?".contains(c)),
        value,
        ConfigError.InvalidMongoDatabase
      )
      .toValidatedNel
}
