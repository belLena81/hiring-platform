package com.example.graphQL.cats.config

import io.github.iltotore.iron.*
import io.github.iltotore.iron.pureconfig.{RefinedConfigError, given}
import _root_.pureconfig.*

/** Pureconfig's tuple derivation nests one inline per field, so the 23-field interview section exceeds the compiler's
  * successive-inline limit when iron's inline readers are resolved inside it. These resolve them once, outside.
  */
private object InterviewSettingReaders {
  private def refined[A, C](using reader: ConfigReader[A], constraint: RuntimeConstraint[A, C]): ConfigReader[A :| C] =
    reader.emap(_.refineEither[C].left.map(RefinedConfigError(_)))
  // One reader per distinct constraint shape: aliases sharing a shape are the same type.
  given ConfigReader[InterviewMaxAttempts] = refined
  given ConfigReader[InterviewRetrySeconds] = refined
  given ConfigReader[InterviewClaimSeconds] = refined
  given ConfigReader[InterviewReplayRetentionSeconds] = refined
  given ConfigReader[InterviewCompletedDedupRetentionSeconds] = refined
  given ConfigReader[InterviewPublicationBatchSize] = refined
  given ConfigReader[InterviewPublicationPollIntervalMs] = refined
  given ConfigReader[InterviewClockSkewToleranceMs] = refined
  given ConfigReader[InterviewProposalTtlSeconds] = refined
  given ConfigReader[InterviewTopicName] = refined
  given ConfigReader[InterviewGroupName] = refined
}
import InterviewSettingReaders.given

private[config] final case class RawKafkaConfig(
    enabled: Boolean,
    bootstrapServers: KafkaBootstrapServers,
    topic: KafkaTopic,
    consumerGroup: KafkaConsumerGroup,
    publisher: RawKafkaPublisherConfig,
    consumer: RawKafkaConsumerConfig,
    saslSecurityProtocol: Option[String] = None,
    interview: Option[RawInterviewRuntimeConfig] = None,
    restartMaxDelaySeconds: Option[KafkaRestartMaxDelaySeconds] = None
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
    partitionConcurrency: Option[PartitionConcurrency] = None
) derives ConfigReader

/** Scala defaults apply when the whole `kafka.interview` section is absent; present sections list every required key.
  */
private[config] final case class RawInterviewRuntimeConfig(
    enabled: Boolean = false,
    orchestratorUsername: Option[String] = None,
    orchestratorPassword: Option[String] = None,
    workerUsername: Option[String] = None,
    workerPassword: Option[String] = None,
    maxAttempts: InterviewMaxAttempts = 5,
    retryBaseSeconds: InterviewRetrySeconds = 1,
    retryCapSeconds: InterviewRetrySeconds = 30,
    providerTimeoutSeconds: InterviewProviderTimeoutSeconds = 10,
    claimSeconds: InterviewClaimSeconds = 60,
    preCommitDeadlineSeconds: InterviewPreCommitDeadlineSeconds = 300,
    replayRetentionSeconds: InterviewReplayRetentionSeconds = 604800L,
    completedDedupRetentionSeconds: InterviewCompletedDedupRetentionSeconds = 691200L,
    fencerUsername: Option[String] = None,
    fencerPassword: Option[String] = None,
    publicationBatchSize: Option[InterviewPublicationBatchSize] = None,
    publicationPollIntervalMs: Option[InterviewPublicationPollIntervalMs] = None,
    clockSkewToleranceMillis: Option[InterviewClockSkewToleranceMs] = None,
    partitionConcurrency: Option[PartitionConcurrency] = None,
    commandsTopic: InterviewTopicName = "hiring.interview-commands",
    resultsTopic: InterviewTopicName = "hiring.interview-results",
    workerGroup: InterviewGroupName = "hiring-interview-workers",
    orchestratorGroup: InterviewGroupName = "hiring-interview-orchestrator",
    proposalTtlSeconds: InterviewProposalTtlSeconds = 259200
) derives ConfigReader
