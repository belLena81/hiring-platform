package com.example.graphQL.cats.config

import com.comcast.ip4s.{Host, Port as Ip4sPort}
import scala.concurrent.duration.FiniteDuration

private[config] final case class TransportSettings(
    host: Host,
    port: Ip4sPort,
    admissionPermits: Int,
    requestTimeout: FiniteDuration,
    trustedProxy: TrustedProxyConfig,
    mongoUri: String,
    mongoDatabase: String,
    discovery: DiscoveryConfig
)

private[config] final case class AuthSettings(
    jwt: JwtAuthConfig,
    passwordHash: PasswordHashConfig,
    rateLimit: AuthRateLimitConfig,
    adminSeed: AdminSeedConfig
)
