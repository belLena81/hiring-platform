package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.comcast.ip4s.{Cidr, Host, IpAddress, Port as Ip4sPort}
import com.mongodb.ConnectionString
import java.nio.charset.StandardCharsets
import scala.concurrent.duration.*

private[config] object HttpMongoConfigValidation {
  def read(http: RawHttpConfig, mongo: RawMongoConfig): ValidatedNel[ConfigError, TransportSettings] =
    (
      validHost(http.host),
      validPort(http.port),
      http.admissionPermits.validNel[ConfigError],
      validTrustedProxyCidrs(http.trustedProxyCidrs),
      validMongoUri(mongo.uri),
      validMongoDatabase(mongo.database)
    ).mapN { (host, port, admissionPermits, trustedProxy, mongoUri, mongoDatabase) =>
      TransportSettings(
        host,
        port,
        admissionPermits,
        http.requestTimeoutMs.millis,
        trustedProxy,
        mongoUri,
        mongoDatabase
      )
    }

  // HTTP_HOST is a bind address, so hostnames such as localhost are intentionally rejected.
  def validHost(value: String): ValidatedNel[ConfigError, Host] =
    IpAddress.fromString(value).map(ip => ip: Host).toValidNel(ConfigError.InvalidHost)
  def validPort(value: Port): ValidatedNel[ConfigError, Ip4sPort] =
    Ip4sPort.fromInt(value).toValidNel(ConfigError.InvalidPort)
  def validMongoUri(value: String): ValidatedNel[ConfigError, String] =
    Either
      .catchNonFatal(new ConnectionString(value))
      .leftMap(_ => ConfigError.InvalidMongoUri)
      .toValidatedNel
      .map(_ => value)
  def validMongoDatabase(value: String): ValidatedNel[ConfigError, String] =
    Either
      .cond(
        value.nonEmpty && value.getBytes(StandardCharsets.UTF_8).length < 64 &&
          !value.exists(c => c.isWhitespace || c.isControl || "/\\.\"$*<>:|?".contains(c)),
        value,
        ConfigError.InvalidMongoDatabase
      )
      .toValidatedNel
  def validTrustedProxyCidrs(values: List[String]): ValidatedNel[ConfigError, TrustedProxyConfig] =
    values
      .traverse { value =>
        Cidr
          .fromString(value)
          .filter(_.prefixBits > 0)
          .toRight(ConfigError.InvalidTrustedProxyCidrs)
          .toValidatedNel
      }
      .map(TrustedProxyConfig.apply)
}
