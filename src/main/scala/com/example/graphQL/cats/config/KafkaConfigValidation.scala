package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.example.graphQL.cats.domain.workflow.InterviewTopicPair

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
      validInterview(
        kafka.interview.getOrElse(RawInterviewRuntimeConfig()),
        kafka.enabled,
        List(kafka.publisher.saslUsername, kafka.consumer.saslUsername).flatten
      )
    ).mapN { (saslSecurityProtocol, _, _, interview) =>
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
          kafka.consumer.partitionConcurrency.getOrElse(KafkaConsumerConfig.DefaultPartitionConcurrency)
        ),
        saslSecurityProtocol,
        interview,
        kafka.restartMaxDelaySeconds.getOrElse(KafkaConfig.DefaultRestartMaxDelaySeconds)
      )
    }

  /** Per-field bounds are decoded by the refined raw types; this combines the cross-field interview rules. */
  private[config] def validInterview(
      raw: RawInterviewRuntimeConfig,
      kafkaEnabled: Boolean = true,
      operationalPrincipals: List[String] = Nil
  ): ValidatedNel[ConfigError, InterviewRuntimeConfig] = {
    val defaults = InterviewRuntimeConfig()
    (
      validKafkaCredentials(raw.enabled, raw.orchestratorUsername, raw.orchestratorPassword),
      validKafkaCredentials(raw.enabled, raw.workerUsername, raw.workerPassword),
      validKafkaCredentials(raw.enabled, raw.fencerUsername, raw.fencerPassword),
      validInterviewActivation(raw.enabled, kafkaEnabled),
      validInterviewPrincipals(raw, operationalPrincipals),
      validRetryWindow(raw.retryBaseSeconds, raw.retryCapSeconds),
      validClaimWindow(raw.providerTimeoutSeconds, raw.claimSeconds),
      validTopicPair(raw.commandsTopic, raw.resultsTopic),
      validGroupPair(raw.workerGroup, raw.orchestratorGroup)
    ).mapN { (_, _, _, _, _, _, _, topics, _) =>
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
        raw.publicationBatchSize.getOrElse(defaults.publicationBatchSize),
        raw.publicationPollIntervalMs.getOrElse(defaults.publicationPollIntervalMs),
        raw.clockSkewToleranceMillis.getOrElse(defaults.clockSkewToleranceMillis),
        raw.partitionConcurrency.getOrElse(defaults.partitionConcurrency),
        topics,
        raw.workerGroup,
        raw.orchestratorGroup,
        raw.proposalTtlSeconds
      )
    }
  }

  /** The interview workflow runs over Kafka, so it cannot be enabled while Kafka publication is disabled. */
  private def validInterviewActivation(enabled: Boolean, kafkaEnabled: Boolean): ValidatedNel[ConfigError, Unit] =
    Either.cond(!enabled || kafkaEnabled, (), ConfigError.InvalidInterviewEnabled).toValidatedNel

  /** Enabled workflows need three distinct principals that do not reuse operational publisher/reader principals. */
  private def validInterviewPrincipals(
      raw: RawInterviewRuntimeConfig,
      operationalPrincipals: List[String]
  ): ValidatedNel[ConfigError, Unit] = {
    val principals = List(raw.orchestratorUsername, raw.workerUsername, raw.fencerUsername).flatten
    val distinct = principals.distinct.length == principals.length
    val isolated = !principals.exists(operationalPrincipals.contains)
    Either.cond(!raw.enabled || (distinct && isolated), (), ConfigError.InvalidInterviewPrincipals).toValidatedNel
  }

  private def validRetryWindow(baseSeconds: Int, capSeconds: Int): ValidatedNel[ConfigError, Unit] =
    Either.cond(capSeconds >= baseSeconds, (), ConfigError.InvalidInterviewRetryWindow).toValidatedNel

  /** A claim must outlive the provider call by a fixed margin so a slow provider cannot outlast its lease. */
  private def validClaimWindow(providerTimeoutSeconds: Int, claimSeconds: Int): ValidatedNel[ConfigError, Unit] =
    Either
      .cond(
        providerTimeoutSeconds.toLong + ClaimMarginSeconds < claimSeconds.toLong,
        (),
        ConfigError.InvalidInterviewClaimWindow
      )
      .toValidatedNel

  private def validTopicPair(commands: String, results: String): ValidatedNel[ConfigError, InterviewTopicPair] =
    Either
      .cond(commands != results, InterviewTopicPair(commands, results), ConfigError.InvalidInterviewTopicPair)
      .toValidatedNel

  private def validGroupPair(workerGroup: String, orchestratorGroup: String): ValidatedNel[ConfigError, Unit] =
    Either.cond(workerGroup != orchestratorGroup, (), ConfigError.InvalidInterviewGroupPair).toValidatedNel

  private val ClaimMarginSeconds = 30L

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
