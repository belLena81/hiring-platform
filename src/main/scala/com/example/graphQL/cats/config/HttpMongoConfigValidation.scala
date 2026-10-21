package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*
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
      .cond(discovery.maxTime < requestTimeoutMs.millis, discovery, ConfigError.InvalidDiscoveryDeadline)
      .toValidatedNel
}
