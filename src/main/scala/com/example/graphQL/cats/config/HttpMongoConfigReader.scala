package com.example.graphQL.cats.config

import io.github.iltotore.iron.pureconfig.given
import _root_.pureconfig.*

private[config] final case class RawHttpConfig(
    host: String,
    port: Port,
    admissionPermits: AdmissionPermits,
    requestTimeoutMs: RequestTimeoutMs,
    trustedProxyCidrs: List[String]
) derives ConfigReader
private[config] final case class RawMongoConfig(
    uri: String,
    database: String,
    resetOnStart: Option[Boolean],
    discovery: Option[RawDiscoveryConfig] = None
) derives ConfigReader
private[config] final case class RawLoggingConfig(maskSensitive: Boolean) derives ConfigReader

private[config] final case class RawDiscoveryConfig(
    maxTimeMillis: Option[DiscoveryMaxTimeMs] = None,
    permits: Option[DiscoveryPermits] = None,
    maxRoots: Option[DiscoveryMaxRoots] = None
) derives ConfigReader
