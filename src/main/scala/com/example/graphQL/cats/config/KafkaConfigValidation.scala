package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*

private[config] object KafkaConfigValidation {
  def read(kafka: RawKafkaConfig): ValidatedNel[ConfigError, KafkaConfig] =
    (
      validKafkaBootstrapServers(kafka.bootstrapServers),
      validKafkaTopic(kafka.topic),
      validKafkaConsumerGroup(kafka.consumerGroup),
      validKafkaSaslSecurityProtocol(kafka.saslSecurityProtocol, kafka.bootstrapServers),
      validKafkaCredentials(kafka.enabled, kafka.publisher.saslUsername, kafka.publisher.saslPassword),
      validKafkaCredentials(
        kafka.enabled && kafka.consumer.enabled,
        kafka.consumer.saslUsername,
        kafka.consumer.saslPassword
      )
    ).mapN { (bootstrapServers, topic, consumerGroup, saslSecurityProtocol, _, _) =>
      KafkaConfig(
        kafka.enabled,
        bootstrapServers,
        topic,
        consumerGroup,
        KafkaPublisherConfig(
          kafka.publisher.workerId,
          kafka.publisher.batchSize,
          kafka.publisher.leaseSeconds,
          kafka.publisher.retryDelaySeconds,
          kafka.publisher.maxAttempts,
          kafka.publisher.pollIntervalMs,
          kafka.publisher.saslUsername,
          kafka.publisher.saslPassword
        ),
        KafkaConsumerConfig(
          kafka.consumer.enabled,
          kafka.consumer.receiptTtlDays,
          kafka.consumer.quarantineTtlDays,
          kafka.consumer.saslUsername,
          kafka.consumer.saslPassword
        ),
        saslSecurityProtocol
      )
    }

  def validKafkaBootstrapServers(value: String): ValidatedNel[ConfigError, String] =
    Either
      .cond(value.trim.nonEmpty && value.length <= 512, value, ConfigError.InvalidKafkaBootstrapServers)
      .toValidatedNel
  def validKafkaTopic(value: String): ValidatedNel[ConfigError, String] =
    Either.cond(value.trim.nonEmpty && value.length <= 249, value, ConfigError.InvalidKafkaTopic).toValidatedNel
  def validKafkaConsumerGroup(value: String): ValidatedNel[ConfigError, String] =
    Either.cond(value.trim.nonEmpty && value.length <= 249, value, ConfigError.InvalidKafkaConsumerGroup).toValidatedNel
  def validKafkaBatchSize(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(1, 500, ConfigError.InvalidKafkaBatchSize)(value)
  def validKafkaLeaseSeconds(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(1, 3600, ConfigError.InvalidKafkaLeaseSeconds)(value)
  def validKafkaRetryDelaySeconds(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(1, 3600, ConfigError.InvalidKafkaRetryDelaySeconds)(value)
  def validKafkaMaxAttempts(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(1, 100, ConfigError.InvalidKafkaMaxAttempts)(value)
  def validKafkaPollInterval(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(100, 60000, ConfigError.InvalidKafkaPollInterval)(value)
  def validKafkaReceiptTtl(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(1, 365, ConfigError.InvalidKafkaReceiptTtl)(value)
  def validKafkaQuarantineTtl(value: Int): ValidatedNel[ConfigError, Int] =
    ConfigBounds.bounded(1, 365, ConfigError.InvalidKafkaQuarantineTtl)(value)

  def validKafkaSaslSecurityProtocol(
      value: Option[String],
      bootstrapServers: String
  ): ValidatedNel[ConfigError, KafkaSaslSecurityProtocol] =
    value.getOrElse("SASL_SSL") match {
      case "SASL_SSL" => KafkaSaslSecurityProtocol.Tls.validNel
      case "SASL_PLAINTEXT" if isLoopbackKafkaBootstrapServers(bootstrapServers) =>
        KafkaSaslSecurityProtocol.Plaintext.validNel
      case _ => ConfigError.InvalidKafkaSaslSecurityProtocol.invalidNel
    }

  def isLoopbackKafkaBootstrapServers(value: String): Boolean =
    value.split(",").toList.forall { server =>
      val address = server.trim
      val host =
        if (address.startsWith("[")) address.drop(1).takeWhile(_ != ']')
        else address.takeWhile(_ != ':')

      host.equalsIgnoreCase("localhost") || host == "::1" || isLoopbackIpv4(host)
    }

  def isLoopbackIpv4(host: String): Boolean =
    host.split("\\.").toList match {
      case first :: second :: third :: fourth :: Nil =>
        val octets = List(first, second, third, fourth).traverse(_.toIntOption)
        octets.exists {
          case firstOctet :: remaining =>
            firstOctet == 127 && remaining.forall(value => value >= 0 && value <= 255)
          case Nil => false
        }
      case _ => false
    }

  def validKafkaCredentials(
      required: Boolean,
      username: Option[String],
      password: Option[String]
  ): ValidatedNel[ConfigError, Unit] =
    Either
      .cond(
        (username, password) match {
          case (None, None)               => !required
          case (Some(user), Some(secret)) => user.trim.nonEmpty && secret.nonEmpty
          case _                          => false
        },
        (),
        ConfigError.InvalidKafkaCredentials
      )
      .toValidatedNel

}
