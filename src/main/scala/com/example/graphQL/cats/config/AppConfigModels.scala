package com.example.graphQL.cats.config

import com.comcast.ip4s.{Cidr, IpAddress}
import com.example.graphQL.cats.domain.search.SearchFusionStrategy
import io.github.iltotore.iron.*
import io.github.iltotore.iron.constraint.any.{In, Not, StrictEqual}
import io.github.iltotore.iron.constraint.collection.MaxLength
import io.github.iltotore.iron.constraint.numeric.*
import io.github.iltotore.iron.constraint.string.*

/** Startup configuration failures. `key` is the public, loggable identity; `paths` are the HOCON settings whose
  * decoding failure maps to this error, so pureconfig/iron field failures need no separate lookup table.
  */
enum ConfigError(val key: String, val paths: Set[String] = Set.empty) {
  case InvalidConfigFile extends ConfigError("CONFIG_FILE")
  case InvalidHost extends ConfigError("HTTP_HOST", Set("http.host"))
  case InvalidPort extends ConfigError("HTTP_PORT", Set("http.port"))
  case InvalidAdmissionPermits extends ConfigError("HTTP_ADMISSION_PERMITS", Set("http.admission-permits"))
  case InvalidRequestTimeout extends ConfigError("HTTP_REQUEST_TIMEOUT_MS", Set("http.request-timeout-ms"))
  case InvalidTrustedProxyCidrs extends ConfigError("HTTP_TRUSTED_PROXY_CIDRS", Set("http.trusted-proxy-cidrs"))
  case InvalidMongoUri extends ConfigError("MONGODB_URI", Set("mongo.uri"))
  case InvalidMongoDatabase extends ConfigError("MONGODB_DATABASE", Set("mongo.database"))
  case InvalidMongoResetOnStart extends ConfigError("MONGODB_RESET_ON_START", Set("mongo.reset-on-start"))
  case InvalidDiscoveryMaxTime extends ConfigError("DISCOVERY_MAX_TIME_MILLIS", Set("mongo.discovery.max-time-millis"))
  case InvalidDiscoveryPermits extends ConfigError("DISCOVERY_PERMITS", Set("mongo.discovery.permits"))
  case InvalidDiscoveryMaxRoots extends ConfigError("DISCOVERY_MAX_ROOTS", Set("mongo.discovery.max-roots"))
  case InvalidDiscoveryDeadline extends ConfigError("DISCOVERY_DEADLINE")
  case InvalidMaskSensitive extends ConfigError("LOG_MASK_SENSITIVE", Set("logging.mask-sensitive"))
  case InvalidAdminSeed
      extends ConfigError(
        "AUTH_ADMIN_SEED",
        Set("auth.admin-seed", "auth.admin-seed.enabled", "auth.admin-seed.name", "auth.admin-seed.password")
      )
  case InvalidJwtSecret extends ConfigError("AUTH_JWT_HS256_SECRET", Set("auth.jwt.hs256-secret"))
  case InvalidReceiptFingerprintSecret
      extends ConfigError("AUTH_RECEIPT_FP_SECRET", Set("auth.jwt.receipt-fingerprint-secret"))
  case InvalidJwtIssuer extends ConfigError("AUTH_JWT_ISSUER", Set("auth.jwt.issuer"))
  case InvalidJwtAudience extends ConfigError("AUTH_JWT_AUDIENCE", Set("auth.jwt.audience"))
  case InvalidCursorTtl extends ConfigError("AUTH_JWT_CURSOR_TTL_SECONDS", Set("auth.jwt.cursor-ttl-seconds"))
  case InvalidPasswordHashIterations
      extends ConfigError("AUTH_PASSWORD_HASH_ITERATIONS", Set("auth.password-hash.iterations"))
  case InvalidPasswordHashMemory
      extends ConfigError("AUTH_PASSWORD_HASH_MEMORY_KIB", Set("auth.password-hash.memory-kib"))
  case InvalidPasswordHashParallelism
      extends ConfigError("AUTH_PASSWORD_HASH_PARALLELISM", Set("auth.password-hash.parallelism"))
  case InvalidAuthRateLimitWindow
      extends ConfigError("AUTH_RATE_LIMIT_WINDOW_SECONDS", Set("auth.rate-limit.window-seconds"))
  case InvalidAuthRateLimitAttempts extends ConfigError("AUTH_RATE_LIMIT_ATTEMPTS", Set("auth.rate-limit.attempts"))
  case InvalidAuthRateLimitBuckets extends ConfigError("AUTH_RATE_LIMIT_BUCKETS", Set("auth.rate-limit.max-buckets"))
  case InvalidVectorSearchEnabled extends ConfigError("VECTOR_SEARCH_ENABLED", Set("vector-search.enabled"))
  case InvalidVoyageApiKey extends ConfigError("VOYAGE_API_KEY", Set("vector-search.voyage.api-key"))
  case InvalidVoyageEndpoint extends ConfigError("VOYAGE_ENDPOINT", Set("vector-search.voyage.endpoint"))
  case InvalidVoyageModel extends ConfigError("VOYAGE_MODEL", Set("vector-search.voyage.model"))
  case InvalidVoyageDimension extends ConfigError("VOYAGE_DIMENSION", Set("vector-search.voyage.dimension"))
  case InvalidEmbeddingQueueSize extends ConfigError("EMBEDDING_QUEUE_SIZE", Set("vector-search.embedding.queue-size"))
  case InvalidEmbeddingParallelism
      extends ConfigError("EMBEDDING_PARALLELISM", Set("vector-search.embedding.parallelism"))
  case InvalidEmbeddingTimeout extends ConfigError("EMBEDDING_TIMEOUT_MS", Set("vector-search.embedding.timeout-ms"))
  case InvalidEmbeddingRetryAttempts
      extends ConfigError("EMBEDDING_RETRY_ATTEMPTS", Set("vector-search.embedding.retry-attempts"))
  case InvalidEmbeddingRetryDelay
      extends ConfigError("EMBEDDING_RETRY_DELAY_MS", Set("vector-search.embedding.retry-delay-ms"))
  case InvalidEmbeddingDurableRetryAttempts
      extends ConfigError("EMBEDDING_DURABLE_RETRY_ATTEMPTS", Set("vector-search.embedding.durable-retry-attempts"))
  case InvalidEmbeddingDurableRetryBase
      extends ConfigError(
        "EMBEDDING_DURABLE_RETRY_BASE_MILLIS",
        Set("vector-search.embedding.durable-retry-base-millis")
      )
  case InvalidEmbeddingDurableRetryCap
      extends ConfigError("EMBEDDING_DURABLE_RETRY_CAP_MILLIS", Set("vector-search.embedding.durable-retry-cap-millis"))
  case InvalidEmbeddingDurableRetryWindow extends ConfigError("EMBEDDING_DURABLE_RETRY_WINDOW")
  case InvalidEmbeddingWorkerRestartDelay
      extends ConfigError(
        "EMBEDDING_WORKER_RESTART_DELAY_MILLIS",
        Set("vector-search.embedding.worker-restart-delay-millis")
      )
  case InvalidJobVectorIndex extends ConfigError("JOB_VECTOR_INDEX", Set("vector-search.indexes.jobs"))
  case InvalidCandidateVectorIndex
      extends ConfigError("CANDIDATE_VECTOR_INDEX", Set("vector-search.indexes.candidates"))
  case InvalidJobLexicalIndex extends ConfigError("JOB_LEXICAL_INDEX", Set("vector-search.indexes.lexical"))
  case InvalidCandidateLexicalIndex
      extends ConfigError("CANDIDATE_LEXICAL_INDEX", Set("vector-search.indexes.candidate-lexical"))
  case InvalidSearchIndexReadyTimeout
      extends ConfigError("SEARCH_INDEX_READY_TIMEOUT_MS", Set("vector-search.indexes.ready-timeout-ms"))
  case InvalidSearchIndexPollInterval
      extends ConfigError("SEARCH_INDEX_POLL_INTERVAL_MS", Set("vector-search.indexes.poll-interval-ms"))
  case InvalidVectorNumCandidates extends ConfigError("VECTOR_NUM_CANDIDATES", Set("vector-search.num-candidates"))
  case InvalidVectorBranchResultLimit
      extends ConfigError("VECTOR_BRANCH_RESULT_LIMIT", Set("vector-search.branch-result-limit"))
  case InvalidVectorFusionStrategy extends ConfigError("VECTOR_FUSION_STRATEGY", Set("vector-search.fusion-strategy"))
  case InvalidRerankEnabled extends ConfigError("VECTOR_RERANK_ENABLED", Set("vector-search.rerank.enabled"))
  case InvalidRerankModel extends ConfigError("VECTOR_RERANK_MODEL", Set("vector-search.rerank.model"))
  case InvalidKafkaEnabled extends ConfigError("KAFKA_ENABLED", Set("kafka.enabled"))
  case InvalidKafkaBootstrapServers extends ConfigError("KAFKA_BOOTSTRAP_SERVERS", Set("kafka.bootstrap-servers"))
  case InvalidKafkaTopic extends ConfigError("KAFKA_TOPIC", Set("kafka.topic"))
  case InvalidKafkaConsumerGroup extends ConfigError("KAFKA_CONSUMER_GROUP", Set("kafka.consumer-group"))
  case InvalidKafkaSaslSecurityProtocol
      extends ConfigError("KAFKA_SASL_SECURITY_PROTOCOL", Set("kafka.sasl-security-protocol"))
  case InvalidKafkaRestartMaxDelay
      extends ConfigError("HIRING_KAFKA_RESTART_MAX_DELAY_SECONDS", Set("kafka.restart-max-delay-seconds"))
  case InvalidKafkaWorkerId extends ConfigError("KAFKA_WORKER_ID", Set("kafka.publisher.worker-id"))
  case InvalidKafkaBatchSize extends ConfigError("KAFKA_BATCH_SIZE", Set("kafka.publisher.batch-size"))
  case InvalidKafkaLeaseSeconds extends ConfigError("KAFKA_LEASE_SECONDS", Set("kafka.publisher.lease-seconds"))
  case InvalidKafkaRetryDelaySeconds
      extends ConfigError("KAFKA_RETRY_DELAY_SECONDS", Set("kafka.publisher.retry-delay-seconds"))
  case InvalidKafkaMaxAttempts extends ConfigError("KAFKA_MAX_ATTEMPTS", Set("kafka.publisher.max-attempts"))
  case InvalidKafkaPollInterval extends ConfigError("KAFKA_POLL_INTERVAL_MS", Set("kafka.publisher.poll-interval-ms"))
  case InvalidKafkaConsumerEnabled extends ConfigError("KAFKA_CONSUMER_ENABLED", Set("kafka.consumer.enabled"))
  case InvalidKafkaReceiptTtl extends ConfigError("KAFKA_RECEIPT_TTL_DAYS", Set("kafka.consumer.receipt-ttl-days"))
  case InvalidKafkaQuarantineTtl
      extends ConfigError("KAFKA_QUARANTINE_TTL_DAYS", Set("kafka.consumer.quarantine-ttl-days"))
  case InvalidKafkaPartitionConcurrency
      extends ConfigError("KAFKA_PARTITION_CONCURRENCY", Set("kafka.consumer.partition-concurrency"))
  case InvalidKafkaCredentials
      extends ConfigError(
        "KAFKA_CREDENTIALS",
        Set(
          "kafka.publisher.sasl-username",
          "kafka.publisher.sasl-password",
          "kafka.consumer.sasl-username",
          "kafka.consumer.sasl-password",
          "kafka.interview.orchestrator-username",
          "kafka.interview.orchestrator-password",
          "kafka.interview.worker-username",
          "kafka.interview.worker-password",
          "kafka.interview.fencer-username",
          "kafka.interview.fencer-password"
        )
      )
  case InvalidInterviewEnabled extends ConfigError("INTERVIEW_ENABLED", Set("kafka.interview.enabled"))
  case InvalidInterviewPrincipals extends ConfigError("INTERVIEW_PRINCIPALS")
  case InvalidInterviewMaxAttempts extends ConfigError("INTERVIEW_MAX_ATTEMPTS", Set("kafka.interview.max-attempts"))
  case InvalidInterviewRetryBase
      extends ConfigError("INTERVIEW_RETRY_BASE_SECONDS", Set("kafka.interview.retry-base-seconds"))
  case InvalidInterviewRetryCap
      extends ConfigError("INTERVIEW_RETRY_CAP_SECONDS", Set("kafka.interview.retry-cap-seconds"))
  case InvalidInterviewRetryWindow extends ConfigError("INTERVIEW_RETRY_WINDOW")
  case InvalidInterviewProviderTimeout
      extends ConfigError("INTERVIEW_PROVIDER_TIMEOUT_SECONDS", Set("kafka.interview.provider-timeout-seconds"))
  case InvalidInterviewClaimSeconds extends ConfigError("INTERVIEW_CLAIM_SECONDS", Set("kafka.interview.claim-seconds"))
  case InvalidInterviewClaimWindow extends ConfigError("INTERVIEW_CLAIM_WINDOW")
  case InvalidInterviewPreCommitDeadline
      extends ConfigError("INTERVIEW_PRE_COMMIT_DEADLINE_SECONDS", Set("kafka.interview.pre-commit-deadline-seconds"))
  case InvalidInterviewReplayRetention
      extends ConfigError("INTERVIEW_REPLAY_RETENTION_SECONDS", Set("kafka.interview.replay-retention-seconds"))
  case InvalidInterviewCompletedDedupRetention
      extends ConfigError(
        "INTERVIEW_COMPLETED_DEDUP_RETENTION_SECONDS",
        Set("kafka.interview.completed-dedup-retention-seconds")
      )
  case InvalidInterviewPublicationBatchSize
      extends ConfigError("INTERVIEW_PUBLICATION_BATCH_SIZE", Set("kafka.interview.publication-batch-size"))
  case InvalidInterviewPublicationPollInterval
      extends ConfigError("INTERVIEW_PUBLICATION_POLL_INTERVAL_MS", Set("kafka.interview.publication-poll-interval-ms"))
  case InvalidInterviewClockSkewTolerance
      extends ConfigError("INTERVIEW_CLOCK_SKEW_TOLERANCE_MILLIS", Set("kafka.interview.clock-skew-tolerance-millis"))
  case InvalidInterviewPartitionConcurrency
      extends ConfigError("INTERVIEW_PARTITION_CONCURRENCY", Set("kafka.interview.partition-concurrency"))
  case InvalidInterviewCommandsTopic
      extends ConfigError("INTERVIEW_COMMANDS_TOPIC", Set("kafka.interview.commands-topic"))
  case InvalidInterviewResultsTopic extends ConfigError("INTERVIEW_RESULTS_TOPIC", Set("kafka.interview.results-topic"))
  case InvalidInterviewTopicPair extends ConfigError("INTERVIEW_TOPIC_PAIR")
  case InvalidInterviewWorkerGroup extends ConfigError("INTERVIEW_WORKER_GROUP", Set("kafka.interview.worker-group"))
  case InvalidInterviewOrchestratorGroup
      extends ConfigError("INTERVIEW_ORCHESTRATOR_GROUP", Set("kafka.interview.orchestrator-group"))
  case InvalidInterviewGroupPair extends ConfigError("INTERVIEW_GROUP_PAIR")
  case InvalidInterviewProposalTtl
      extends ConfigError("INTERVIEW_PROPOSAL_TTL_SECONDS", Set("kafka.interview.proposal-ttl-seconds"))
}

object ConfigError {
  val publicKeys: Set[String] = values.iterator.map(_.key).toSet

  /** The error owning a fully qualified HOCON path whose value failed to decode. */
  def forPath(path: String): Option[ConfigError] = values.find(_.paths.contains(path))
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
    durableRetryAttempts: Int = VectorSearchConfig.DefaultDurableRetryAttempts,
    durableRetryBaseMillis: Int = VectorSearchConfig.DefaultDurableRetryBaseMillis,
    durableRetryCapMillis: Int = VectorSearchConfig.DefaultDurableRetryCapMillis,
    workerRestartDelayMillis: Int = VectorSearchConfig.DefaultWorkerRestartDelayMillis
)
object VectorSearchConfig {
  val DefaultDurableRetryAttempts: Int = 8
  val DefaultDurableRetryBaseMillis: Int = 1000
  val DefaultDurableRetryCapMillis: Int = 300000
  val DefaultWorkerRestartDelayMillis: Int = 1000
}

final case class JwtAuthConfig(
    hmacSecret: String,
    issuer: String,
    audience: String,
    accessTokenSeconds: Long = 900L,
    cursorTtlSeconds: Long = 900L,
    receiptFingerprintSecret: Option[String] = None
) {

  /** Idempotency-receipt HMAC key. Independent of JWT signing only when `receiptFingerprintSecret` is set; otherwise it
    * is the signing secret, and changing either then invalidates stored authentication receipt matches. Key rotation is
    * manual and out of scope before MVP.
    */
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
    partitionConcurrency: Int = KafkaConsumerConfig.DefaultPartitionConcurrency
)
object KafkaConsumerConfig {
  val DefaultPartitionConcurrency: Int = 4
}
final case class KafkaConfig(
    enabled: Boolean,
    bootstrapServers: String,
    topic: String,
    consumerGroup: String,
    publisher: KafkaPublisherConfig,
    consumer: KafkaConsumerConfig,
    saslSecurityProtocol: KafkaSaslSecurityProtocol = KafkaSaslSecurityProtocol.Tls,
    interview: InterviewRuntimeConfig = InterviewRuntimeConfig(),
    restartMaxDelaySeconds: Int = KafkaConfig.DefaultRestartMaxDelaySeconds
)
object KafkaConfig {
  val DefaultRestartMaxDelaySeconds: Int = 30
}

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
type RerankModel = String :| In[("rerank-2.5", "rerank-2.5-lite", "rerank-2", "rerank-2-lite")]
type KafkaRestartMaxDelaySeconds = Int :| Interval.Closed[1, 300]
type PartitionConcurrency = Int :| Interval.Closed[1, 64]
type DiscoveryMaxTimeMs = Int :| GreaterEqual[100]
type DiscoveryPermits = Int :| Interval.Closed[1, 64]
type DiscoveryMaxRoots = Int :| Interval.Closed[1, 64]
type EmbeddingDurableRetryAttempts = Int :| Interval.Closed[1, 100]
type EmbeddingDurableRetryMs = Int :| Interval.Closed[100, 3600000]
type EmbeddingWorkerRestartDelayMs = Int :| Interval.Closed[100, 60000]
type InterviewMaxAttempts = Int :| Interval.Closed[1, 100]
type InterviewRetrySeconds = Int :| Interval.Closed[1, 300]
type InterviewProviderTimeoutSeconds = Int :| Interval.Closed[1, 3600]
type InterviewClaimSeconds = Int :| Interval.Closed[1, 3600]
type InterviewPreCommitDeadlineSeconds = Int :| Interval.Closed[1, 300]
type InterviewReplayRetentionSeconds = Long :| StrictEqual[604800L]
type InterviewCompletedDedupRetentionSeconds = Long :| Interval.Closed[691200L, 31536000L]
type InterviewPublicationBatchSize = Int :| Interval.Closed[1, 64]
type InterviewPublicationPollIntervalMs = Int :| Interval.Closed[100, 60000]
type InterviewClockSkewToleranceMs = Int :| Interval.Closed[0, 60000]
type InterviewProposalTtlSeconds = Int :| Interval.Closed[3600, 1209600]
type InterviewTopicName = String :| (Match["[A-Za-z0-9._-]{1,249}"] & Not[StrictEqual["."]] & Not[StrictEqual[".."]])
type InterviewGroupName = String :| Match["[A-Za-z0-9._-]{1,200}"]

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
    orchestratorGroup: String = "hiring-interview-orchestrator",
    proposalTtlSeconds: Int = 259200
) {

  /** How long a reschedule proposal stays open: 1 hour to 14 days, 72 hours by default. */
  def proposalTtl: java.time.Duration = java.time.Duration.ofSeconds(proposalTtlSeconds.toLong)
}

final case class DiscoveryConfig(maxTimeMillis: Int = 2000, permits: Int = 4, maxRoots: Int = 4)

final case class AdminSeedConfig(
    enabled: Boolean = false,
    name: Option[String] = None,
    password: Option[String] = None
) {
  override def toString: String = "AdminSeedConfig([REDACTED])"
}
