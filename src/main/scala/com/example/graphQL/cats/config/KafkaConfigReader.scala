package com.example.graphQL.cats.config

import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.*
import io.github.iltotore.iron.constraint.numeric.*
import io.github.iltotore.iron.pureconfig.given
import _root_.pureconfig.*

private[config] final case class RawKafkaConfig(
    enabled: Boolean,
    bootstrapServers: String,
    topic: NonBlankStr,
    consumerGroup: NonBlankStr,
    publisher: RawKafkaPublisherConfig,
    consumer: RawKafkaConsumerConfig,
    saslSecurityProtocol: Option[String] = None
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
    saslPassword: Option[String]
) derives ConfigReader
