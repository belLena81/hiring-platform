package com.example.hiring.analytics.config

import cats.data.ValidatedNec
import cats.syntax.all.*

import java.net.InetAddress
import scala.util.Try

/** Validated Kafka connection settings shared by analytics configuration and its adapters. */
final case class KafkaConnection(
    bootstrapServers: String,
    saslUsername: Option[String] = None,
    saslPassword: Option[String] = None,
    securityProtocol: String = "SASL_SSL",
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
        case "SASL_SSL" => connection.validNec
        case "SASL_PLAINTEXT" if connection.allowPlaintext && localPlaintextBootstrap(connection.bootstrapServers) =>
          connection.validNec
        case "SASL_PLAINTEXT" if !connection.allowPlaintext =>
          "Kafka SASL_PLAINTEXT requires analytics.kafka.allow-plaintext=true".invalidNec
        case "SASL_PLAINTEXT" =>
          "Kafka SASL_PLAINTEXT is restricted to validated loopback brokers and the local Compose kafka:9092 endpoint".invalidNec
        case _ => "Kafka security protocol must be SASL_SSL or SASL_PLAINTEXT".invalidNec
      }
    ).mapN((_, _, _) => connection)

  private[analytics] def localPlaintextBootstrap(bootstrapServers: String): Boolean =
    bootstrapServers.trim.nonEmpty && bootstrapServers.split(",", -1).forall { endpoint =>
      val broker = endpoint.trim
      val hostPort =
        if (broker.startsWith("[")) {
          val close = broker.indexOf(']')
          if (close > 0 && broker.drop(close + 1).startsWith(":"))
            Some(broker.substring(1, close) -> broker.drop(close + 2))
          else None
        } else {
          val separator = broker.lastIndexOf(':')
          if (separator > 0) Some(broker.substring(0, separator) -> broker.drop(separator + 1)) else None
        }
      hostPort.exists { case (host, rawPort) =>
        val validPort = rawPort.toIntOption.exists(port => port > 0 && port <= 65535)
        val localHost = host.equalsIgnoreCase("localhost") ||
          (host.equalsIgnoreCase("kafka") && rawPort == "9092") ||
          ((host.forall(char => char.isDigit || char == '.' || char == ':') && host.nonEmpty) &&
            Try(InetAddress.getByName(host).isLoopbackAddress).getOrElse(false))
        validPort && localHost
      }
    }
}
