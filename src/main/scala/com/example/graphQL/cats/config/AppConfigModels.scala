package com.example.graphQL.cats.config

import com.comcast.ip4s.{Cidr, IpAddress}
import com.example.graphQL.cats.domain.search.SearchFusionStrategy
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.collection.MaxLength
import io.github.iltotore.iron.constraint.numeric.*
import io.github.iltotore.iron.constraint.string.*

enum ConfigError(val key: String) {
  case InvalidConfigFile(message: String) extends ConfigError("CONFIG_FILE")
  case InvalidHost extends ConfigError("HTTP_HOST")
  case InvalidPort extends ConfigError("HTTP_PORT")
  case InvalidAdmissionPermits extends ConfigError("HTTP_ADMISSION_PERMITS")
  case InvalidRequestTimeout extends ConfigError("HTTP_REQUEST_TIMEOUT_MS")
  case InvalidMongoUri extends ConfigError("MONGODB_URI")
  case InvalidMongoDatabase extends ConfigError("MONGODB_DATABASE")
  case InvalidMaskSensitive extends ConfigError("LOG_MASK_SENSITIVE")
  case InvalidAdminSeed extends ConfigError("AUTH_ADMIN_SEED")
  case InvalidKafkaRestartMaxDelay extends ConfigError("KAFKA_RESTART_MAX_DELAY_SECONDS")
  case InvalidJwtSecret extends ConfigError("AUTH_JWT_HS256_SECRET")
  case InvalidReceiptFingerprintSecret extends ConfigError("AUTH_RECEIPT_FP_SECRET")
  case InvalidJwtIssuer extends ConfigError("AUTH_JWT_ISSUER")
  case InvalidJwtAudience extends ConfigError("AUTH_JWT_AUDIENCE")
  case InvalidCursorTtl extends ConfigError("AUTH_JWT_CURSOR_TTL_SECONDS")
  case InvalidPasswordHashIterations extends ConfigError("AUTH_PASSWORD_HASH_ITERATIONS")
  case InvalidPasswordHashMemory extends ConfigError("AUTH_PASSWORD_HASH_MEMORY_KIB")
  case InvalidPasswordHashParallelism extends ConfigError("AUTH_PASSWORD_HASH_PARALLELISM")
  case InvalidAuthRateLimitWindow extends ConfigError("AUTH_RATE_LIMIT_WINDOW_SECONDS")
  case InvalidAuthRateLimitAttempts extends ConfigError("AUTH_RATE_LIMIT_ATTEMPTS")
  case InvalidAuthRateLimitBuckets extends ConfigError("AUTH_RATE_LIMIT_BUCKETS")
  case InvalidTrustedProxyCidrs extends ConfigError("HTTP_TRUSTED_PROXY_CIDRS")
  case InvalidVectorSearchEnabled extends ConfigError("VECTOR_SEARCH_ENABLED")
  case InvalidVoyageApiKey extends ConfigError("VOYAGE_API_KEY")
  case InvalidVoyageEndpoint extends ConfigError("VOYAGE_ENDPOINT")
  case InvalidVoyageModel extends ConfigError("VOYAGE_MODEL")
  case InvalidVoyageDimension extends ConfigError("VOYAGE_DIMENSION")
  case InvalidEmbeddingQueueSize extends ConfigError("EMBEDDING_QUEUE_SIZE")
  case InvalidEmbeddingParallelism extends ConfigError("EMBEDDING_PARALLELISM")
  case InvalidEmbeddingTimeout extends ConfigError("EMBEDDING_TIMEOUT_MS")
  case InvalidEmbeddingRetryAttempts extends ConfigError("EMBEDDING_RETRY_ATTEMPTS")
  case InvalidEmbeddingRetryDelay extends ConfigError("EMBEDDING_RETRY_DELAY_MS")
  case InvalidJobVectorIndex extends ConfigError("JOB_VECTOR_INDEX")
  case InvalidCandidateVectorIndex extends ConfigError("CANDIDATE_VECTOR_INDEX")
  case InvalidJobLexicalIndex extends ConfigError("JOB_LEXICAL_INDEX")
  case InvalidSearchIndexReadyTimeout extends ConfigError("SEARCH_INDEX_READY_TIMEOUT_MS")
  case InvalidSearchIndexPollInterval extends ConfigError("SEARCH_INDEX_POLL_INTERVAL_MS")
  case InvalidVectorNumCandidates extends ConfigError("VECTOR_NUM_CANDIDATES")
  case InvalidVectorBranchResultLimit extends ConfigError("VECTOR_BRANCH_RESULT_LIMIT")
  case InvalidVectorFusionStrategy extends ConfigError("VECTOR_FUSION_STRATEGY")
  case InvalidRerankModel extends ConfigError("VECTOR_RERANK_MODEL")
  case InvalidKafkaEnabled extends ConfigError("KAFKA_ENABLED")
  case InvalidKafkaBootstrapServers extends ConfigError("KAFKA_BOOTSTRAP_SERVERS")
  case InvalidKafkaTopic extends ConfigError("KAFKA_TOPIC")
  case InvalidKafkaConsumerGroup extends ConfigError("KAFKA_CONSUMER_GROUP")
  case InvalidKafkaBatchSize extends ConfigError("KAFKA_BATCH_SIZE")
  case InvalidKafkaLeaseSeconds extends ConfigError("KAFKA_LEASE_SECONDS")
  case InvalidKafkaRetryDelaySeconds extends ConfigError("KAFKA_RETRY_DELAY_SECONDS")
  case InvalidKafkaMaxAttempts extends ConfigError("KAFKA_MAX_ATTEMPTS")
  case InvalidKafkaPollInterval extends ConfigError("KAFKA_POLL_INTERVAL_MS")
  case InvalidKafkaReceiptTtl extends ConfigError("KAFKA_RECEIPT_TTL_DAYS")
  case InvalidKafkaQuarantineTtl extends ConfigError("KAFKA_QUARANTINE_TTL_DAYS")
  case InvalidKafkaPartitionConcurrency extends ConfigError("KAFKA_PARTITION_CONCURRENCY")
  case InvalidDiscoveryQueryLimits extends ConfigError("DISCOVERY_QUERY_LIMITS")
  case InvalidEmbeddingRecovery extends ConfigError("EMBEDDING_RECOVERY")
  case InvalidKafkaCredentials extends ConfigError("KAFKA_CREDENTIALS")
  case InvalidKafkaSaslSecurityProtocol extends ConfigError("KAFKA_SASL_SECURITY_PROTOCOL")
}

object ConfigError {
  val publicKeys: Set[String] = List[ConfigError](
    InvalidConfigFile(""),
    InvalidHost,
    InvalidPort,
    InvalidAdmissionPermits,
    InvalidRequestTimeout,
    InvalidMongoUri,
    InvalidMongoDatabase,
    InvalidMaskSensitive,
    InvalidAdminSeed,
    InvalidKafkaRestartMaxDelay,
    InvalidJwtSecret,
    InvalidJwtIssuer,
    InvalidJwtAudience,
    InvalidCursorTtl,
    InvalidPasswordHashIterations,
    InvalidPasswordHashMemory,
    InvalidPasswordHashParallelism,
    InvalidAuthRateLimitWindow,
    InvalidAuthRateLimitAttempts,
    InvalidAuthRateLimitBuckets,
    InvalidTrustedProxyCidrs,
    InvalidVectorSearchEnabled,
    InvalidVoyageApiKey,
    InvalidVoyageEndpoint,
    InvalidVoyageModel,
    InvalidVoyageDimension,
    InvalidEmbeddingQueueSize,
    InvalidEmbeddingParallelism,
    InvalidEmbeddingTimeout,
    InvalidEmbeddingRetryAttempts,
    InvalidEmbeddingRetryDelay,
    InvalidJobVectorIndex,
    InvalidCandidateVectorIndex,
    InvalidJobLexicalIndex,
    InvalidSearchIndexReadyTimeout,
    InvalidSearchIndexPollInterval,
    InvalidVectorNumCandidates,
    InvalidVectorBranchResultLimit,
    InvalidVectorFusionStrategy,
    InvalidRerankModel,
    InvalidKafkaEnabled,
    InvalidKafkaBootstrapServers,
    InvalidKafkaTopic,
    InvalidKafkaConsumerGroup,
    InvalidKafkaBatchSize,
    InvalidKafkaLeaseSeconds,
    InvalidKafkaRetryDelaySeconds,
    InvalidKafkaMaxAttempts,
    InvalidKafkaPollInterval,
    InvalidKafkaReceiptTtl,
    InvalidKafkaQuarantineTtl,
    InvalidKafkaPartitionConcurrency,
    InvalidDiscoveryQueryLimits,
    InvalidEmbeddingRecovery,
    InvalidKafkaCredentials,
    InvalidKafkaSaslSecurityProtocol
  ).map(_.key).toSet
}

enum KafkaSaslSecurityProtocol(val kafkaValue: String) {
  case Tls extends KafkaSaslSecurityProtocol("SASL_SSL")
  case Plaintext extends KafkaSaslSecurityProtocol("SASL_PLAINTEXT")
}

final case class VectorSearchConfig(
    enabled: Boolean,
    voyageApiKey: Option[String],
    voyageEndpoint: String,
    voyageModel: String,
    voyageDimension: Int,
    queueSize: Int,
    parallelism: Int,
    timeoutMillis: Int,
    retryAttempts: Int,
    retryDelayMillis: Int,
    jobVectorIndex: String,
    candidateVectorIndex: String,
    jobLexicalIndex: String,
    candidateLexicalIndex: String,
    fusionStrategy: SearchFusionStrategy,
    rerankEnabled: Boolean,
    rerankModel: String,
    indexReadyTimeoutMillis: Int,
    indexPollIntervalMillis: Int,
    numCandidates: Int,
    branchResultLimit: Int,
    durableRetryAttempts: Int = 8,
    durableRetryBaseMillis: Int = 1000,
    durableRetryCapMillis: Int = 300000,
    workerRestartDelayMillis: Int = 1000
)

final case class JwtAuthConfig(
    hmacSecret: String,
    issuer: String,
    audience: String,
    accessTokenSeconds: Long = 900L,
    cursorTtlSeconds: Long = 900L,
    receiptFingerprintSecret: Option[String] = None
) {

  /** Idempotency-receipt key; independent of JWT signing so rotating the signing secret keeps receipts valid. */
  def receiptSecret: String = receiptFingerprintSecret.getOrElse(hmacSecret)
  override def toString: String = s"JwtAuthConfig($issuer, $audience, [REDACTED])"
}
final case class PasswordHashConfig(iterations: Int, memoryKilobytes: Int, parallelism: Int)
final case class AuthRateLimitConfig(windowSeconds: Int, attempts: Int, maxBuckets: Int)
final case class TrustedProxyConfig(cidrs: List[Cidr[IpAddress]])
final case class KafkaPublisherConfig(
    workerId: String,
    batchSize: Int,
    leaseSeconds: Int,
    retryDelaySeconds: Int,
    maxAttempts: Int,
    pollIntervalMillis: Int,
    saslUsername: Option[String] = None,
    saslPassword: Option[String] = None
)
final case class KafkaConsumerConfig(
    enabled: Boolean,
    receiptTtlDays: Int,
    quarantineTtlDays: Int,
    saslUsername: Option[String] = None,
    saslPassword: Option[String] = None,
    partitionConcurrency: Int = 4
)
final case class KafkaConfig(
    enabled: Boolean,
    bootstrapServers: String,
    topic: String,
    consumerGroup: String,
    publisher: KafkaPublisherConfig,
    consumer: KafkaConsumerConfig,
    saslSecurityProtocol: KafkaSaslSecurityProtocol = KafkaSaslSecurityProtocol.Tls,
    interview: InterviewRuntimeConfig = InterviewRuntimeConfig(),
    restartMaxDelaySeconds: Int = 30
)

type Port = Int :| Interval.Closed[1, 65535]
type AdmissionPermits = Int :| Interval.Closed[1, 1024]
type NonBlank128 = String :| (Not[Blank] & MaxLength[128])
type NonBlankStr = String :| Not[Blank]
type VoyageDim = Int :| StrictEqual[1024]
type QueueSize = Int :| Interval.Closed[1, 10000]
type Parallelism = Int :| Interval.Closed[1, 64]
type TimeoutMs = Int :| Interval.Closed[100, 60000]
type RequestTimeoutMs = Int :| Interval.Closed[100, 60000]
type CursorTtlSeconds = Int :| Interval.Closed[60, 86400]
type AuthRateWindowSeconds = Int :| Interval.Closed[1, 3600]
type AuthRateAttempts = Int :| Interval.Closed[1, 1000]
type AuthRateBuckets = Int :| Interval.Closed[1, 100000]
type PasswordHashIterations = Int :| Interval.Closed[1, 10]
type PasswordHashMemoryKib = Int :| Interval.Closed[8192, 1048576]
type PasswordHashParallelism = Int :| Interval.Closed[1, 16]
type EmbeddingRetryAttempts = Int :| Interval.Closed[1, 10]
type EmbeddingRetryDelayMs = Int :| Interval.Closed[100, 60000]
type SearchIndexReadyTimeoutMs = Int :| Interval.Closed[1000, 600000]
type SearchIndexPollIntervalMs = Int :| Interval.Closed[100, 10000]
type KafkaBootstrapServers = String :| (Not[Blank] & MaxLength[512])
type KafkaTopic = String :| (Not[Blank] & MaxLength[249])
type KafkaConsumerGroup = String :| (Not[Blank] & MaxLength[249])
type KafkaBatchSize = Int :| Interval.Closed[1, 500]
type KafkaLeaseSeconds = Int :| Interval.Closed[1, 3600]
type KafkaRetryDelaySeconds = Int :| Interval.Closed[1, 3600]
type KafkaMaxAttempts = Int :| Interval.Closed[1, 100]
type KafkaPollIntervalMs = Int :| Interval.Closed[100, 60000]
type KafkaRetentionDays = Int :| Interval.Closed[1, 365]
type HttpsUrl = String :| StartWith["https://"]

final case class InterviewRuntimeConfig(
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
    publicationBatchSize: Int = 16,
    publicationPollIntervalMs: Int = 1000,
    clockSkewToleranceMillis: Int = 5000,
    partitionConcurrency: Int = 4,
    topics: com.example.graphQL.cats.domain.workflow.InterviewTopicPair =
      com.example.graphQL.cats.domain.workflow.InterviewTopicPair.Default,
    workerGroup: String = "hiring-interview-workers",
    orchestratorGroup: String = "hiring-interview-orchestrator"
)

final case class DiscoveryConfig(maxTimeMillis: Int = 2000, permits: Int = 4, maxRoots: Int = 4)

final case class AdminSeedConfig(
    enabled: Boolean = false,
    name: Option[String] = None,
    password: Option[String] = None
) {
  override def toString: String = "AdminSeedConfig([REDACTED])"
}
