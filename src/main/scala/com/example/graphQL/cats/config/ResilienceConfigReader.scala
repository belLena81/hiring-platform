package com.example.graphQL.cats.config

import _root_.pureconfig.*

/** Plain numbers: ranges and cross-field rules are validated together so one startup error lists every violation. */
private[config] final case class RawCircuitBreakerConfig(
    enabled: Boolean,
    failureThreshold: Int,
    windowMs: Int,
    openMs: Int,
    maxOpenMs: Int,
    halfOpenProbes: Int
) derives ConfigReader

private[config] final case class RawCircuitBreakers(
    voyage: RawCircuitBreakerConfig,
    atlasSearch: RawCircuitBreakerConfig,
    kafkaPublisher: RawCircuitBreakerConfig,
    mongoWrites: RawCircuitBreakerConfig
) derives ConfigReader

private[config] final case class RawResilienceConfig(breakers: RawCircuitBreakers) derives ConfigReader
