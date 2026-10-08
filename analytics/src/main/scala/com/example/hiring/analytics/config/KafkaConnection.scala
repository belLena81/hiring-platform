package com.example.hiring.analytics.config

import cats.effect.Async
import com.example.hiring.analytics.errors.AnalyticsError
import cats.data.ValidatedNec
import cats.syntax.all.*
import pureconfig.ConfigReader
import pureconfig.error.UserValidationFailed

import java.net.InetAddress
import scala.util.Try

enum KafkaSecurityProtocol(val kafkaValue: String) {
  case SaslSsl extends KafkaSecurityProtocol("SASL_SSL")
  case SaslPlaintext extends KafkaSecurityProtocol("SASL_PLAINTEXT")
}

object KafkaSecurityProtocol {
  given ConfigReader[KafkaSecurityProtocol] = ConfigReader[String].emap {
    case "SASL_SSL"       => Right(SaslSsl)
    case "SASL_PLAINTEXT" => Right(SaslPlaintext)
    case _                => Left(UserValidationFailed("must be SASL_SSL or SASL_PLAINTEXT"))
  }
}

/** Validated Kafka connection settings shared by analytics configuration and its adapters. */
final case class KafkaConnection(
    bootstrapServers: String,
    saslUsername: Option[String] = None,
    saslPassword: Option[String] = None,
    securityProtocol: KafkaSecurityProtocol = KafkaSecurityProtocol.SaslSsl,
    allowPlaintext: Boolean = false
) {
  override def toString: String = "KafkaConnection([REDACTED])"
}

object KafkaConnection {
  def validate(connection: KafkaConnection): ValidatedNec[String, KafkaConnection] =
    (
      if (connection.bootstrapServers.trim.nonEmpty) connection.validNec
      else "Kafka bootstrap servers must be non-empty".invalidNec,
      (connection.saslUsername, connection.saslPassword) match {
        case (None, None)                                                            => connection.validNec
        case (Some(user), Some(password)) if user.trim.nonEmpty && password.nonEmpty => connection.validNec
        case _ => "Kafka SASL username and password must both be non-empty".invalidNec
      },
      connection.securityProtocol match {
        case KafkaSecurityProtocol.SaslSsl => connection.validNec
        case KafkaSecurityProtocol.SaslPlaintext
            if connection.allowPlaintext && structurallyLocal(connection.bootstrapServers) =>
          connection.validNec
        case KafkaSecurityProtocol.SaslPlaintext if !connection.allowPlaintext =>
          "Kafka SASL_PLAINTEXT requires analytics.kafka.allow-plaintext=true".invalidNec
        case KafkaSecurityProtocol.SaslPlaintext =>
          "Kafka SASL_PLAINTEXT is restricted to validated loopback brokers and the local Compose kafka:9092 endpoint".invalidNec
      }
    ).mapN((_, _, _) => connection)

  private def structurallyLocal(servers: String): Boolean =
    servers.trim.nonEmpty && servers
      .split(",", -1)
      .forall(endpoint => BootstrapEndpoint.parse(endpoint).exists(_.allowedHost))

  def preflight[F[_]: Async](connection: KafkaConnection): F[Unit] =
    preflightUsing(connection, host => Try(InetAddress.getAllByName(host).toVector).toOption)

  private[analytics] def preflightUsing[F[_]: Async](
      connection: KafkaConnection,
      resolveAddresses: String => Option[Vector[InetAddress]]
  ): F[Unit] =
    Async[F].fromEither(validate(connection).toEither.leftMap(AnalyticsError.InvalidInput.apply)) *>
      (if (connection.securityProtocol == KafkaSecurityProtocol.SaslPlaintext)
         Async[F]
           .blocking(localPlaintextBootstrapUsing(connection.bootstrapServers, resolveAddresses))
           .flatMap(valid =>
             Async[F].raiseUnless(valid)(
               AnalyticsError
                 .InvalidConfiguration("Kafka plaintext endpoint did not resolve exclusively to loopback addresses")
             )
           )
       else Async[F].unit)

  private[analytics] def localPlaintextBootstrap(bootstrapServers: String): Boolean =
    structurallyLocal(bootstrapServers)

  private[analytics] def localPlaintextBootstrapUsing(
      bootstrapServers: String,
      resolveAddresses: String => Option[Vector[InetAddress]]
  ): Boolean =
    bootstrapServers.trim.nonEmpty && bootstrapServers.split(",", -1).forall { endpoint =>
      BootstrapEndpoint.parse(endpoint).exists(_.isLocal(resolveAddresses))
    }

  private final case class BootstrapEndpoint(host: String, port: Int) {
    def allowedHost: Boolean =
      (host.equalsIgnoreCase("kafka") && port == 9092) || host.equalsIgnoreCase("localhost") ||
        (host.nonEmpty && host.forall(char => char.isDigit || char == '.' || char == ':'))

    def isLocal(resolveAddresses: String => Option[Vector[InetAddress]]): Boolean =
      (host.equalsIgnoreCase("kafka") && port == 9092) ||
        ((host.equalsIgnoreCase("localhost") || (host.nonEmpty && host.forall(char =>
          char.isDigit || char == '.' || char == ':'
        ))) && resolveAddresses(host).exists(addresses => addresses.nonEmpty && addresses.forall(_.isLoopbackAddress)))
  }

  private object BootstrapEndpoint {
    def parse(endpoint: String): Option[BootstrapEndpoint] = {
      val broker = endpoint.trim
      val hostPort =
        if (broker.startsWith("[")) {
          val close = broker.indexOf(']')
          if (close > 1 && broker.drop(close + 1).startsWith(":"))
            Some(broker.substring(1, close) -> broker.drop(close + 2))
          else None
        } else {
          val separator = broker.lastIndexOf(':')
          if (separator > 0) Some(broker.substring(0, separator) -> broker.drop(separator + 1)) else None
        }
      hostPort.flatMap { case (host, rawPort) =>
        rawPort.toIntOption.filter(port => port > 0 && port <= 65535).map(BootstrapEndpoint(host, _))
      }
    }
  }
}
