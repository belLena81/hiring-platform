package com.example.graphQL.cats.config

import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.*
import io.github.iltotore.iron.constraint.numeric.*
import io.github.iltotore.iron.pureconfig.given
import _root_.pureconfig.*

private[config] final case class RawKafkaConfig(
    enabled: Boolean,
    bootstrapServers: KafkaBootstrapServers,
    topic: KafkaTopic,
    consumerGroup: KafkaConsumerGroup,
    publisher: RawKafkaPublisherConfig,
    consumer: RawKafkaConsumerConfig,
    saslSecurityProtocol: Option[String] = None,
    interview: Option[RawInterviewRuntimeConfig] = None,
    restartMaxDelaySeconds: Option[Int] = None
) derives ConfigReader
private[config] final case class RawKafkaPublisherConfig(
    workerId: NonBlankStr,
    batchSize: KafkaBatchSize,
    leaseSeconds: KafkaLeaseSeconds,
    retryDelaySeconds: KafkaRetryDelaySeconds,
    maxAttempts: KafkaMaxAttempts,
    pollIntervalMs: KafkaPollIntervalMs,
    saslUsername: Option[String],
    saslPassword: Option[String]
) derives ConfigReader
private[config] final case class RawKafkaConsumerConfig(
    enabled: Boolean,
    receiptTtlDays: KafkaRetentionDays,
    quarantineTtlDays: KafkaRetentionDays,
    saslUsername: Option[String],
    saslPassword: Option[String],
    partitionConcurrency: Option[Int] = None
) derives ConfigReader

private[config] final case class RawInterviewRuntimeConfig(
    enabled: Boolean = false,
    orchestratorUsername: Option[String] = None,
    orchestratorPassword: Option[String] = None,
    workerUsername: Option[String] = None,
    workerPassword: Option[String] = None,
    maxAttempts: Int = 5,
    retryBaseSeconds: Int = 1,
    retryCapSeconds: Int = 30,
    providerTimeoutSeconds: Int = 10,
    claimSeconds: Int = 60,
    preCommitDeadlineSeconds: Int = 300,
    replayRetentionSeconds: Long = 604800L,
    completedDedupRetentionSeconds: Long = 691200L,
    fencerUsername: Option[String] = None,
    fencerPassword: Option[String] = None,
    publicationBatchSize: Option[Int] = None,
    publicationPollIntervalMs: Option[Int] = None,
    clockSkewToleranceMillis: Option[Int] = None,
    partitionConcurrency: Option[Int] = None,
    commandsTopic: String = "hiring.interview-commands",
    resultsTopic: String = "hiring.interview-results",
    workerGroup: String = "hiring-interview-workers",
    orchestratorGroup: String = "hiring-interview-orchestrator"
) derives ConfigReader
