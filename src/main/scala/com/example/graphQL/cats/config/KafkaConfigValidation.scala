package com.example.graphQL.cats.config

import cats.data.ValidatedNel
import cats.syntax.all.*
import com.comcast.ip4s.{IpAddress, SocketAddress}
import com.example.graphQL.cats.domain.workflow.InterviewTopicPair

private[config] object KafkaConfigValidation {
  def read(kafka: RawKafkaConfig): ValidatedNel[ConfigError, KafkaConfig] =
    (
      validKafkaSaslSecurityProtocol(
        kafka.saslSecurityProtocol.getOrElse(KafkaSaslSecurityProtocol.Tls),
        kafka.bootstrapServers
      ),
      validKafkaSaslMechanism(kafka.saslMechanism.getOrElse(KafkaSaslMechanism.ScramSha512), kafka.bootstrapServers),
      validKafkaCredentials(kafka.enabled, kafka.publisher.saslUsername, kafka.publisher.saslPassword),
      validCommitBatch(
        kafka.consumer.maxPollRecords,
        kafka.consumer.maxPollIntervalSeconds,
        kafka.consumer.commitBatchSize,
        kafka.consumer.commitBatchIntervalMillis,
        ConfigError.InvalidKafkaCommitBatchSize,
        ConfigError.InvalidKafkaCommitBatchWindow
      ),
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
    ).mapN { (saslSecurityProtocol, saslMechanism, _, _, _, interview) =>
      KafkaConfig(
        kafka.enabled,
        kafka.bootstrapServers,
        kafka.topic,
        kafka.consumerGroup,
        kafka.publisher,
        kafka.consumer,
        saslSecurityProtocol,
        interview,
        kafka.restartMaxDelaySeconds.getOrElse(KafkaConfig.DefaultRestartMaxDelaySeconds),
        saslMechanism
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
      validGroupPair(raw.workerGroup, raw.orchestratorGroup),
      validCommitBatch(
        raw.maxPollRecords.getOrElse(defaults.maxPollRecords),
        raw.maxPollIntervalSeconds.getOrElse(defaults.maxPollIntervalSeconds),
        raw.commitBatchSize.getOrElse(defaults.commitBatchSize),
        raw.commitBatchIntervalMillis.getOrElse(defaults.commitBatchIntervalMillis),
        ConfigError.InvalidInterviewCommitBatchSize,
        ConfigError.InvalidInterviewCommitBatchWindow
      ),
      validNotifications(raw.notifications)
    ).mapN { (_, _, _, _, _, _, _, topics, _, _, notifications) =>
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
        raw.proposalTtlSeconds,
        raw.workerId,
        raw.maxPollRecords.getOrElse(defaults.maxPollRecords),
        raw.maxPollIntervalSeconds.getOrElse(defaults.maxPollIntervalSeconds),
        raw.commitBatchSize.getOrElse(defaults.commitBatchSize),
        raw.commitBatchIntervalMillis.getOrElse(defaults.commitBatchIntervalMillis),
        notifications
      )
    }
  }

  /** SES is selected only when `provider = ses` and `ses.enabled = true` agree. A disagreeing pair is an error, never a
    * silent fallback to the ledger. Missing required SES values and the pair error accumulate together.
    */
  private[config] def validNotifications(
      raw: RawInterviewNotificationConfig
  ): ValidatedNel[ConfigError, InterviewNotificationConfig] = {
    val wantsSes = raw.provider == InterviewNotificationProviderKind.Ses
    val pair = Either.cond(wantsSes == raw.ses.enabled, (), ConfigError.InvalidInterviewSesSelection).toValidatedNel
    if (!(wantsSes && raw.ses.enabled)) pair.map(_ => InterviewNotificationConfig())
    else
      (
        pair,
        raw.ses.region.toValidNel(ConfigError.InvalidInterviewSesRegion),
        raw.ses.fromAddress.toValidNel(ConfigError.InvalidInterviewSesFromAddress)
      ).mapN { (_, region, from) =>
        InterviewNotificationConfig(
          InterviewNotificationSelection.Ses(SesNotificationSettings(region, from, raw.ses.configurationSet))
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

  /** A claim must outlive the provider call by a fixed margin so a slow provider cannot outlast its lease. The margin
    * also keeps every valid claim above the 15 s floor that the producer transaction timeout (claim less 5 s) needs.
    */
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

  /** A commit batch must fit in one poll, and its flush interval must fit inside the poll-interval liveness window. */
  private def validCommitBatch(
      maxPollRecords: Int,
      maxPollIntervalSeconds: Int,
      commitBatchSize: Int,
      commitBatchIntervalMillis: Int,
      sizeError: ConfigError,
      windowError: ConfigError
  ): ValidatedNel[ConfigError, Unit] =
    (
      Either.cond(commitBatchSize <= maxPollRecords, (), sizeError).toValidatedNel,
      Either
        .cond(commitBatchIntervalMillis.toLong < maxPollIntervalSeconds.toLong * 1000L, (), windowError)
        .toValidatedNel
    ).mapN((_, _) => ())

  /** PLAIN sends the secret in clear, so it follows the same plaintext/loopback rule as SASL_PLAINTEXT. */
  def validKafkaSaslMechanism(
      mechanism: KafkaSaslMechanism,
      bootstrapServers: String
  ): ValidatedNel[ConfigError, KafkaSaslMechanism] =
    mechanism match {
      case KafkaSaslMechanism.ScramSha512 => mechanism.validNel
      case KafkaSaslMechanism.Plain       =>
        Either
          .cond(
            validKafkaSaslSecurityProtocol(KafkaSaslSecurityProtocol.Plaintext, bootstrapServers).isValid,
            mechanism,
            ConfigError.InvalidKafkaSaslMechanism
          )
          .toValidatedNel
    }

  def validKafkaSaslSecurityProtocol(
      protocol: KafkaSaslSecurityProtocol,
      bootstrapServers: String
  ): ValidatedNel[ConfigError, KafkaSaslSecurityProtocol] =
    Either
      .cond(
        protocol == KafkaSaslSecurityProtocol.Tls || isLoopbackKafkaBootstrapServers(bootstrapServers),
        protocol,
        ConfigError.InvalidKafkaSaslSecurityProtocol
      )
      .toValidatedNel

  /** Every `host:port` entry must be `localhost` or a loopback address (IPv6 literals in brackets). */
  def isLoopbackKafkaBootstrapServers(value: String): Boolean =
    value.split(",").toList.forall { server =>
      SocketAddress
        .fromString(server.trim)
        .exists(_.host match {
          case ip: IpAddress => ip.isLoopback
          case host          => host.toString.equalsIgnoreCase("localhost")
        })
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
