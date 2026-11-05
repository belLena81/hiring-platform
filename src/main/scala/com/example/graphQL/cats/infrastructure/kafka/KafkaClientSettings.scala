package com.example.graphQL.cats.infrastructure.kafka

import cats.effect.IO
import com.example.graphQL.cats.config.{KafkaSaslMechanism, KafkaSaslSecurityProtocol}
import fs2.kafka.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

import java.util.Properties
import scala.concurrent.duration.*

/** The single builder of Kafka client transport, SASL and producer reliability settings for the application build. */
object KafkaClientSettings {
  val ProducerAcks: String = "all"
  val ProducerRequestTimeoutMillis: Int = 10000

  /** Head-room kept between a producer transaction and the lease or claim that guards it. */
  val LeaseMarginMillis: Int = 5000

  /** The shortest transaction the margin may shrink to; configuration validates a lease of at least 15 s. */
  val MinTransactionTimeoutMillis: Int = 10000

  /** The broker default `transaction.max.timeout.ms`; a larger producer transaction timeout fails `initTransactions`.
    * Configuration bounds the lease and claim at 905 s so the derived timeout stays at or below it.
    */
  val BrokerDefaultTransactionMaxTimeoutMillis: Int = 900000

  /** Invariant: one publication wave holds one transaction, and its sends plus commit must finish before the lease (or
    * interview claim) that guards it expires, so both `transaction.timeout.ms` and `delivery.timeout.ms` are the lease
    * less [[LeaseMarginMillis]], never below [[MinTransactionTimeoutMillis]]. The transaction timeout is capped at
    * [[BrokerDefaultTransactionMaxTimeoutMillis]]; the delivery timeout stays derived from the lease.
    */
  def transactionTimeoutMillis(leaseSeconds: Int): Int =
    math.min(BrokerDefaultTransactionMaxTimeoutMillis.toLong, deliveryTimeoutMillis(leaseSeconds).toLong).toInt

  def deliveryTimeoutMillis(leaseSeconds: Int): Int =
    math.max(MinTransactionTimeoutMillis.toLong, leaseSeconds.toLong * 1000L - LeaseMarginMillis).toInt

  /** A single request must time out strictly before the delivery it belongs to, so a retry fits inside the window. */
  def requestTimeoutMillis(deliveryTimeoutMillis: Int): Int =
    math.min(ProducerRequestTimeoutMillis, deliveryTimeoutMillis / 2)
  val ProducerCompressionType: String = "lz4"
  val ProducerLingerMillis: String = "5"

  /** Credentials select SASL with the configured mechanism over the configured protocol; their absence is an explicit
    * PLAINTEXT transport.
    */
  def security(
      username: Option[String],
      password: Option[String],
      protocol: KafkaSaslSecurityProtocol = KafkaSaslSecurityProtocol.Tls,
      mechanism: KafkaSaslMechanism
  ): Map[String, String] =
    (username, password) match {
      case (Some(user), Some(secret)) =>
        Map(
          "security.protocol" -> protocol.kafkaValue,
          "sasl.mechanism" -> mechanism.kafkaValue,
          "sasl.jaas.config" ->
            s"${mechanism.loginModule} required username=${jaasLiteral(user)} password=${jaasLiteral(secret)};"
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
      maxRequestBytes: Int,
      mechanism: KafkaSaslMechanism,
      leaseSeconds: Int
  ): ProducerSettings[IO, String, Array[Byte]] = {
    val transactionTimeout = transactionTimeoutMillis(leaseSeconds)
    val deliveryTimeout = deliveryTimeoutMillis(leaseSeconds)
    ProducerSettings(Serializer[IO, String], Serializer[IO, Array[Byte]])
      .withBootstrapServers(bootstrapServers)
      .withProperty(ProducerConfig.ACKS_CONFIG, ProducerAcks)
      .withProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
      .withProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestBytes.toString)
      .withProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeout.toString)
      .withProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMillis(deliveryTimeout).toString)
      .withProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeout.toString)
      .withProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, ProducerCompressionType)
      .withProperty(ProducerConfig.LINGER_MS_CONFIG, ProducerLingerMillis)
      .withProperties(security(username, password, protocol, mechanism))
  }

  /** Connection, bounded API timeouts and security for the blocking admin and plain consumer clients. */
  def clientProperties(
      bootstrapServers: String,
      username: Option[String],
      password: Option[String],
      protocol: KafkaSaslSecurityProtocol,
      mechanism: KafkaSaslMechanism
  ): java.util.Properties = {
    val properties = new Properties()
    (Map(
      "bootstrap.servers" -> bootstrapServers,
      "request.timeout.ms" -> ProducerRequestTimeoutMillis.toString,
      "default.api.timeout.ms" -> ProducerRequestTimeoutMillis.toString
    ) ++ security(username, password, protocol, mechanism)).foreach { case (key, value) =>
      properties.setProperty(key, value)
    }
    properties
  }

  /** An admin client with the same security and bounded timeouts as the other blocking clients. */
  def admin(
      bootstrapServers: String,
      username: Option[String],
      password: Option[String],
      protocol: KafkaSaslSecurityProtocol,
      mechanism: KafkaSaslMechanism
  ): AdminClientSettings =
    AdminClientSettings(bootstrapServers)
      .withProperties(
        Map(
          "request.timeout.ms" -> ProducerRequestTimeoutMillis.toString,
          "default.api.timeout.ms" -> ProducerRequestTimeoutMillis.toString
        ) ++ security(username, password, protocol, mechanism)
      )

  def consumer(
      bootstrapServers: String,
      username: Option[String],
      password: Option[String],
      protocol: KafkaSaslSecurityProtocol,
      mechanism: KafkaSaslMechanism
  ): ConsumerSettings[IO, String, Array[Byte]] =
    ConsumerSettings(Deserializer[IO, String], Deserializer[IO, Array[Byte]])
      .withBootstrapServers(bootstrapServers)
      .withProperties(security(username, password, protocol, mechanism))

  /** A group member that commits offsets only after its records are durable and never reads aborted transactions.
    */
  def committedConsumer(
      bootstrapServers: String,
      username: Option[String],
      password: Option[String],
      protocol: KafkaSaslSecurityProtocol,
      groupId: String,
      mechanism: KafkaSaslMechanism,
      maxPollRecords: Int,
      maxPollIntervalSeconds: Int
  ): ConsumerSettings[IO, String, Array[Byte]] =
    consumer(bootstrapServers, username, password, protocol, mechanism)
      .withMaxPollRecords(maxPollRecords)
      .withMaxPollInterval(maxPollIntervalSeconds.seconds)
      .withGroupId(groupId)
      .withEnableAutoCommit(false)
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      .withProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
}
