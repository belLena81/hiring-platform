package com.example.graphQL.cats.config

import cats.syntax.all.*
import com.comcast.ip4s.{Cidr, IpAddress, Port as Ip4sPort}
import com.mongodb.ConnectionString
import io.github.iltotore.iron.pureconfig.given
import _root_.pureconfig.*
import _root_.pureconfig.error.CannotConvert

/** Value-level readers: a failed conversion reports the setting's path, which `ConfigError.forPath` maps to its key. */
private object HttpMongoReaders {
  // HTTP_HOST is a bind address, so hostnames such as localhost are intentionally rejected.
  given ConfigReader[IpAddress] =
    ConfigReader[String].emap(value =>
      IpAddress.fromString(value).toRight(CannotConvert(value, "IpAddress", "invalid"))
    )
  given ConfigReader[Ip4sPort] =
    ConfigReader[Int].emap(value =>
      Ip4sPort.fromInt(value).filter(_.value > 0).toRight(CannotConvert(value.toString, "Port", "invalid"))
    )
  // The URI may embed credentials, so it never appears in the failure. Repository results are trusted only when the
  // write concern acknowledges them, so an unacknowledged (w=0) concern is rejected at the configuration boundary.
  given ConfigReader[ConnectionString] =
    ConfigReader[String].emap(value =>
      Either
        .catchNonFatal(new ConnectionString(value))
        .filterOrElse(uri => Option(uri.getWriteConcern).forall(_.isAcknowledged), new IllegalArgumentException)
        .leftMap(_ => CannotConvert("[redacted]", "MongoUri", "invalid"))
    )
  given ConfigReader[DiscoveryConfig] =
    ConfigReader.forProduct3("max-time-millis", "permits", "max-roots")(
      (maxTime: Option[DiscoveryMaxTimeMs], permits: Option[DiscoveryPermits], roots: Option[DiscoveryMaxRoots]) =>
        val defaults = DiscoveryConfig()
        DiscoveryConfig(
          maxTime.getOrElse(defaults.maxTimeMillis),
          permits.getOrElse(defaults.permits),
          roots.getOrElse(defaults.maxRoots)
        )
    )
  given ConfigReader[TrustedProxyConfig] =
    ConfigReader[List[String]].emap(
      _.traverse(value =>
        Cidr
          .fromString(value)
          .filter(_.prefixBits > 0)
          .toRight(CannotConvert(value, "Cidr", "invalid or matches every address"))
      ).map(TrustedProxyConfig.apply)
    )
}
import HttpMongoReaders.given

private[config] final case class RawHttpConfig(
    host: IpAddress,
    port: Ip4sPort,
    admissionPermits: AdmissionPermits,
    requestTimeoutMs: RequestTimeoutMs,
    trustedProxyCidrs: TrustedProxyConfig
) derives ConfigReader
private[config] final case class RawMongoConfig(
    uri: ConnectionString,
    database: String,
    resetOnStart: Option[Boolean],
    discovery: Option[DiscoveryConfig]
) derives ConfigReader
private[config] final case class RawLoggingConfig(maskSensitive: Boolean) derives ConfigReader
