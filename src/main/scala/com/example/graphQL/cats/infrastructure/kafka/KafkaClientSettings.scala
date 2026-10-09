package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.IO
import com.example.graphQL.cats.config.KafkaSaslSecurityProtocol
import fs2.kafka.*
import org.apache.kafka.clients.producer.ProducerConfig

/** The single builder of Kafka client transport, SASL and producer reliability settings for the application build. */
object KafkaClientSettings {
  val ProducerAcks: String = "all"
  val ProducerDeliveryTimeoutMillis: Int = 30000
  val ProducerRequestTimeoutMillis: Int = 10000

  private val PlainLoginModule = "org.apache.kafka.common.security.plain.PlainLoginModule"

  /** Credentials select SASL/PLAIN over the configured protocol; their absence is an explicit PLAINTEXT transport. */
  def security(
      username: Option[String],
      password: Option[String],
      protocol: KafkaSaslSecurityProtocol = KafkaSaslSecurityProtocol.Tls
  ): Map[String, String] =
    (username, password) match {
      case (Some(user), Some(secret)) =>
        Map(
          "security.protocol" -> protocol.kafkaValue,
          "sasl.mechanism" -> "PLAIN",
          "sasl.jaas.config" ->
            s"$PlainLoginModule required username=${jaasLiteral(user)} password=${jaasLiteral(secret)};"
        )
      case _ => Map("security.protocol" -> "PLAINTEXT")
    }

  /** A quoted JAAS string literal: escapes backslash, double quote and the control characters the parser unescapes. */
  def jaasLiteral(value: String): String = {
    val escaped = new StringBuilder(value.length + 2)
    value.foreach {
      case '\\'  => escaped.append("\\\\")
      case '"'   => escaped.append("\\\"")
      case '\n'  => escaped.append("\\n")
      case '\r'  => escaped.append("\\r")
      case '\t'  => escaped.append("\\t")
      case '\b'  => escaped.append("\\b")
      case '\f'  => escaped.append("\\f")
      case other => escaped.append(other)
    }
    "\"" + escaped + "\""
  }

  def producer(
      bootstrapServers: String,
      username: Option[String],
      password: Option[String],
      protocol: KafkaSaslSecurityProtocol,
      maxRequestBytes: Int
  ): ProducerSettings[IO, String, Array[Byte]] =
    ProducerSettings(Serializer[IO, String], Serializer[IO, Array[Byte]])
      .withBootstrapServers(bootstrapServers)
      .withProperty(ProducerConfig.ACKS_CONFIG, ProducerAcks)
      .withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
      .withProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestBytes.toString)
      .withProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, ProducerDeliveryTimeoutMillis.toString)
      .withProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, ProducerRequestTimeoutMillis.toString)
      .withProperties(security(username, password, protocol))

  def consumer(
      bootstrapServers: String,
      username: Option[String],
      password: Option[String],
      protocol: KafkaSaslSecurityProtocol
  ): ConsumerSettings[IO, String, Array[Byte]] =
    ConsumerSettings(Deserializer[IO, String], Deserializer[IO, Array[Byte]])
      .withBootstrapServers(bootstrapServers)
      .withProperties(security(username, password, protocol))
}
