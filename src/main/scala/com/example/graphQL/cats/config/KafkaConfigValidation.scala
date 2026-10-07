package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*

private[config] object KafkaConfigValidation {
  def read(kafka: RawKafkaConfig): ValidatedNel[ConfigError, KafkaConfig] =
    (
      validKafkaSaslSecurityProtocol(kafka.saslSecurityProtocol, kafka.bootstrapServers),
      validKafkaCredentials(kafka.enabled, kafka.publisher.saslUsername, kafka.publisher.saslPassword),
      validKafkaCredentials(
        kafka.enabled && kafka.consumer.enabled,
        kafka.consumer.saslUsername,
        kafka.consumer.saslPassword
      ),
      ConfigBounds.bounded(1, 64, ConfigError.InvalidKafkaPartitionConcurrency)(
        kafka.consumer.partitionConcurrency.getOrElse(4)
      ),
      validInterview(
        kafka.interview.getOrElse(RawInterviewRuntimeConfig()),
        kafka.enabled,
        List(kafka.publisher.saslUsername, kafka.consumer.saslUsername).flatten
      )
    ).mapN { (saslSecurityProtocol, _, _, _, interview) =>
      KafkaConfig(
        kafka.enabled,
        kafka.bootstrapServers,
        kafka.topic,
        kafka.consumerGroup,
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
          kafka.consumer.saslPassword,
          kafka.consumer.partitionConcurrency.getOrElse(4)
        ),
        saslSecurityProtocol,
        interview
      )
    }

  private[config] def validInterview(
      raw: RawInterviewRuntimeConfig,
      kafkaEnabled: Boolean = true,
      operationalPrincipals: List[String] = Nil
  ): ValidatedNel[ConfigError, InterviewRuntimeConfig] =
    (
      validKafkaCredentials(raw.enabled, raw.orchestratorUsername, raw.orchestratorPassword),
      validKafkaCredentials(raw.enabled, raw.workerUsername, raw.workerPassword),
      validKafkaCredentials(raw.enabled, raw.fencerUsername, raw.fencerPassword),
      Either
        .cond(
          raw.publicationBatchSize.getOrElse(16) >= 1 && raw.publicationBatchSize.getOrElse(
            16
          ) <= 64 && raw.clockSkewToleranceMillis.getOrElse(5000) >= 0 && raw.clockSkewToleranceMillis.getOrElse(
            5000
          ) <= 60000 &&
            validInterviewTransportNames(raw) && raw.partitionConcurrency.getOrElse(4) >= 1 && raw.partitionConcurrency
              .getOrElse(
                4
              ) <= 64 && raw.maxAttempts >= 1 && raw.maxAttempts <= 100 && raw.retryBaseSeconds >= 1 &&
            raw.retryCapSeconds >= raw.retryBaseSeconds && raw.retryCapSeconds <= 300 &&
            raw.providerTimeoutSeconds >= 1 && raw.providerTimeoutSeconds.toLong + 30L < raw.claimSeconds.toLong &&
            raw.claimSeconds <= 3600 && raw.preCommitDeadlineSeconds >= 1 && raw.preCommitDeadlineSeconds <= 300 && raw.replayRetentionSeconds == 604800L &&
            raw.completedDedupRetentionSeconds >= 691200L && raw.completedDedupRetentionSeconds <= 31536000L &&
            (!raw.enabled || (kafkaEnabled && raw.orchestratorUsername != raw.workerUsername &&
              raw.fencerUsername != raw.orchestratorUsername && raw.fencerUsername != raw.workerUsername &&
              !operationalPrincipals.exists(principal =>
                raw.orchestratorUsername.contains(principal) || raw.workerUsername
                  .contains(principal) || raw.fencerUsername.contains(principal)
              ))),
          (),
          ConfigError.InvalidKafkaCredentials
        )
        .toValidatedNel
    ).mapN { (_, _, _, _) =>
      InterviewRuntimeConfig(
        raw.enabled,
        raw.orchestratorUsername,
        raw.orchestratorPassword,
        raw.workerUsername,
        raw.workerPassword,
        raw.maxAttempts,
        raw.retryBaseSeconds,
        raw.retryCapSeconds,
        raw.providerTimeoutSeconds,
        raw.claimSeconds,
        raw.preCommitDeadlineSeconds,
        raw.replayRetentionSeconds,
        raw.completedDedupRetentionSeconds,
        raw.fencerUsername,
        raw.fencerPassword,
        raw.publicationBatchSize.getOrElse(16),
        raw.clockSkewToleranceMillis.getOrElse(5000),
        raw.partitionConcurrency.getOrElse(4),
        com.example.graphQL.cats.domain.workflow.InterviewTopicPair(raw.commandsTopic, raw.resultsTopic),
        raw.workerGroup,
        raw.orchestratorGroup
      )
    }

  private def validInterviewTransportNames(raw: RawInterviewRuntimeConfig): Boolean = {
    def topic(value: String): Boolean = value.matches("[A-Za-z0-9._-]{1,249}") && value != "." && value != ".."
    def group(value: String): Boolean = value.matches("[A-Za-z0-9._-]{1,200}")
    topic(raw.commandsTopic) && topic(raw.resultsTopic) && raw.commandsTopic != raw.resultsTopic &&
    group(raw.workerGroup) && group(raw.orchestratorGroup) && raw.workerGroup != raw.orchestratorGroup
  }

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
