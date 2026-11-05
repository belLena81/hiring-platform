package com.example.graphQL.cats.service

import cats.{Applicative, MonadThrow}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.policy.{CircuitBreakerPolicy, ConsumerReceiptWindowRefusal}
import com.example.graphQL.cats.service.port.ProducerTransactionalId
import com.example.graphQL.cats.service.resilience.CircuitBreakerName
import com.example.graphQL.cats.shared.{HiringHttpPaths, Parsing}
import java.util.Locale

enum LogLevel {
  case Trace, Debug, Info, Warn, Error

  def label: String = productPrefix.toUpperCase(java.util.Locale.ROOT)
}

enum LogEvent(val category: String, val component: String, val message: String, val level: LogLevel) {
  case ProducerGenerationStarted
      extends LogEvent("PRODUCER_GENERATION_STARTED", "KAFKA", "Producer generation initialized", LogLevel.Info)
  case ProducerGenerationClosed
      extends LogEvent("PRODUCER_GENERATION_CLOSED", "KAFKA", "Producer generation resources released", LogLevel.Info)
  case ConsumerCommitDropped
      extends LogEvent(
        "CONSUMER_COMMIT_DROPPED",
        "KAFKA",
        "An offset commit was dropped because the consumer group rebalanced; the records are redelivered",
        LogLevel.Warn
      )
  case PublicationWaveAbandoned
      extends LogEvent(
        "PUBLICATION_WAVE_ABANDONED",
        "KAFKA",
        "A publication wave was abandoned and its remaining claims released for retry",
        LogLevel.Warn
      )
  case ConfigInvalid extends LogEvent("CONFIG_INVALID", "CONFIG", "Application configuration rejected", LogLevel.Error)
  case MongoUnavailable
      extends LogEvent("MONGO_UNAVAILABLE", "READINESS", "Database readiness check failed", LogLevel.Warn)
  case MongoAuthFailed
      extends LogEvent("MONGO_AUTH_FAILED", "READINESS", "Database authentication failed", LogLevel.Warn)
  case ReadinessDegraded
      extends LogEvent("READINESS_DEGRADED", "READINESS", "A readiness component is degraded", LogLevel.Warn)
  case RequestRejected extends LogEvent("REQUEST_REJECTED", "HTTP", "Request rejected", LogLevel.Warn)
  case StartupFailed extends LogEvent("STARTUP_FAILED", "RUNTIME", "Application startup failed", LogLevel.Error)
  case RuntimeFailed extends LogEvent("RUNTIME_FAILED", "RUNTIME", "Unhandled runtime failure", LogLevel.Error)
  case ConsumerReceiptWindowRefused
      extends LogEvent(
        "CONSUMER_RECEIPT_WINDOW_REFUSED",
        "KAFKA",
        "Operational consumer not started: receipts may expire before Kafka stops delivering",
        LogLevel.Error
      )
  case Started extends LogEvent("STARTED", "RUNTIME", "Application listening for requests", LogLevel.Info)
  case Shutdown extends LogEvent("SHUTDOWN", "RUNTIME", "Application resources released", LogLevel.Info)
  case GraphQLCompleted extends LogEvent("GRAPHQL_COMPLETED", "GRAPHQL", "GraphQL operation finished", LogLevel.Info)
  case MongoProbeFailed extends LogEvent("MONGO_PROBE_FAILED", "MONGO", "MongoDB ping failed", LogLevel.Warn)
  case MongoRepositoryFailed
      extends LogEvent("MONGO_REPOSITORY_FAILED", "MONGO", "Mongo repository operation failed", LogLevel.Error)
  case MongoTransactionExhaustedRetried
      extends LogEvent(
        "MONGO_TRANSACTION_EXHAUSTED_RETRIED",
        "MONGO",
        "A Mongo transaction ran out of retries; its caller retries on its own next cycle",
        LogLevel.Warn
      )
  case MongoSetupFailed extends LogEvent("MONGO_SETUP_FAILED", "MONGO", "MongoDB setup failed", LogLevel.Error)
  case AccessTokenIssuanceFailed
      extends LogEvent("ACCESS_TOKEN_ISSUANCE_FAILED", "AUTH", "Access token issuance failed", LogLevel.Error)
  case EmbeddingWakeFailed
      extends LogEvent("EMBEDDING_WAKE_FAILED", "SEARCH", "Embedding work wake failed", LogLevel.Warn)
  case EmbeddingProcessingFailed
      extends LogEvent("EMBEDDING_PROCESSING_FAILED", "SEARCH", "Embedding work processing failed", LogLevel.Warn)
  case EmbeddingProviderFailed
      extends LogEvent("EMBEDDING_PROVIDER_FAILED", "SEARCH", "Embedding provider request failed", LogLevel.Warn)

  /** A claim took over an expired `Processing` lease. The previous holder may have paid the provider for a vector it
    * never stored, so the count of these events bounds the paid-twice residual of duplicate embedding work. Carries
    * only the attempt count (`LogField.Count`), never an entity id or text.
    */
  case EmbeddingLeaseReclaimed
      extends LogEvent(
        "EMBEDDING_LEASE_RECLAIMED",
        "SEARCH",
        "Embedding work took over an expired lease; the previous holder may already have paid the provider",
        LogLevel.Warn
      )
  case SearchSessionVerificationFailed
      extends LogEvent(
        "SEARCH_SESSION_VERIFICATION_FAILED",
        "MONGO",
        "Retained search session verification failed",
        LogLevel.Warn
      )
  case SearchRecordingFailed
      extends LogEvent(
        "SEARCH_RECORDING_FAILED",
        "SEARCH",
        "Search session recording failed; the results were still returned",
        LogLevel.Warn
      )
  case CircuitBreakerStateChanged
      extends LogEvent(
        "CIRCUIT_BREAKER_STATE_CHANGED",
        "RESILIENCE",
        "Dependency circuit breaker changed state",
        LogLevel.Warn
      )
  case CircuitBreakerDisabled
      extends LogEvent(
        "CIRCUIT_BREAKER_DISABLED",
        "RESILIENCE",
        "Dependency circuit breaker is disabled by configuration",
        LogLevel.Warn
      )
  case BacklogSampleFailed
      extends LogEvent(
        "BACKLOG_SAMPLE_FAILED",
        "TELEMETRY",
        "A durable backlog sample failed; its metrics are unobserved",
        LogLevel.Warn
      )
  case ApplicationWriteConflict
      extends LogEvent(
        "APPLICATION_WRITE_CONFLICT",
        "MONGO",
        "A write was refused as a conflict; the source says which step",
        LogLevel.Warn
      )
  case LocalUnmasked
      extends LogEvent("LOCAL_UNMASKED", "SECURITY", "Diagnostic metadata masking is disabled", LogLevel.Warn)

  def marker: String = s"HP.$component.$category"
  def severity: String = level.label
}

enum LogOutcome(val wire: String) {
  case Completed extends LogOutcome("COMPLETED")
  case Rejected extends LogOutcome("REJECTED")
  case Cancelled extends LogOutcome("CANCELLED")
  case FieldError extends LogOutcome("FIELD_ERROR")
  case Ready extends LogOutcome("READY")
  case NotReady extends LogOutcome("NOT_READY")
  case OwnershipLost extends LogOutcome("OWNERSHIP_LOST")
  case RecoveryDeferred extends LogOutcome("RECOVERY_DEFERRED")
}

enum LogField(val key: String, val sensitive: Boolean = false) {
  case TransactionalId extends LogField("transactionalId")
  case GenerationId extends LogField("generationId")
  case Method extends LogField("method")
  case Route extends LogField("route")
  case Status extends LogField("status")
  case DurationMs extends LogField("durationMs")
  case Reason extends LogField("reason")
  case Outcome extends LogField("outcome")
  case ConfigKey extends LogField("configKey")
  case ErrorType extends LogField("errorType")
  case ErrorLocation extends LogField("errorLocation")
  case BodyBytes extends LogField("bodyBytes")
  case HttpPort extends LogField("httpPort")
  case Environment extends LogField("environment")
  case OperationName extends LogField("operationName", true)
  case MongoHosts extends LogField("mongoHosts", true)
  case MongoDatabase extends LogField("mongoDatabase", true)
  case HttpHost extends LogField("httpHost", true)
  case TraceId extends LogField("traceId", true)
  case SpanId extends LogField("spanId", true)
  case SpanName extends LogField("spanName")
  case Worker extends LogField("worker")
  case EntityId extends LogField("entityId", true)
  case ActorId extends LogField("actorId", true)
  case Count extends LogField("count")
  case Title extends LogField("title", true)
  case Country extends LogField("country", true)
  case City extends LogField("city", true)
  case Skills extends LogField("skills", true)
  case Remote extends LogField("remote")
  case JobStatus extends LogField("jobStatus")
  case Breaker extends LogField("breaker")
  case BreakerTransition extends LogField("breakerTransition")
  case SearchKind extends LogField("searchKind")
  case ErrorCode extends LogField("errorCode")
  case RetentionMs extends LogField("retentionMs")
  case SegmentMs extends LogField("segmentMs")
  case ReceiptTtlMs extends LogField("receiptTtlMs")
  case WindowRefusal extends LogField("windowRefusal")
  case ConflictSource extends LogField("conflictSource")
  case TransactionOperation extends LogField("transactionOperation")
}

/** Names the repository transaction that ran out of retries. A closed set: the log sink allow-lists exactly these
  * labels, so a new transaction caller must add a case here and cannot log an unlisted name. Names are stable
  * `<store>.<operation>` business names. `reportsApplicationWriteConflict` marks the two operations whose exhaustion
  * also emits `APPLICATION_WRITE_CONFLICT`. `exhaustionRetriedByCaller` marks an operation whose exhaustion loses
  * nothing because its caller runs the same transaction again on its next scheduled cycle with no user waiting; its
  * exhaustion is logged as WARN instead of ERROR. Set it only for such a polling caller, never for a user-facing
  * operation (which answers `Conflict`) and never where a missed cycle leaves state unsafe.
  */
enum TransactionOperation(
    val label: String,
    val reportsApplicationWriteConflict: Boolean = false,
    val exhaustionRetriedByCaller: Boolean = false
) {
  case ApplicationWrite extends TransactionOperation("applications.writeTransaction", true)
  case MutationReceiptWrite extends TransactionOperation("mutationReceipt.writeTransaction", true)
  case OutboxClaimBatch extends TransactionOperation("outbox.claimBatch", exhaustionRetriedByCaller = true)
  case OutboxRenewLease extends TransactionOperation("outbox.renewLease")
  case SearchSessionWorkComplete extends TransactionOperation("searchSessionWork.complete")
  case InterviewWorkflowCreate extends TransactionOperation("interviewWorkflow.create")
  case InterviewWorkflowAdvance extends TransactionOperation("interviewWorkflow.advance")
  case InterviewWorkflowCommitHiring extends TransactionOperation("interviewWorkflow.commitHiring")
  case InterviewWorkflowApplyLifecycle extends TransactionOperation("interviewWorkflow.applyLifecycle")
  case InterviewWorkflowApproveRescheduledInterval
      extends TransactionOperation("interviewWorkflow.approveRescheduledInterval")
  case InterviewWorkflowClaimExecution extends TransactionOperation("interviewWorkflow.claimExecution")
  case InterviewWorkflowAuthorizePublication extends TransactionOperation("interviewWorkflow.authorizePublication")
  case InterviewWorkflowRecordResult extends TransactionOperation("interviewWorkflow.recordResult")
  case InterviewWorkflowRequireRepair extends TransactionOperation("interviewWorkflow.requireRepair")
  case InterviewWorkflowRepairNotifications extends TransactionOperation("interviewWorkflow.repairNotifications")
  case InterviewCalendarReserve extends TransactionOperation("interviewCalendar.reserve")
  case InterviewCalendarRelease extends TransactionOperation("interviewCalendar.release")
  case InterviewCalendarCancel extends TransactionOperation("interviewCalendar.cancel")
  case InterviewNotificationDeliver extends TransactionOperation("interviewNotification.deliverOnce")
  case EmbeddingWorkInspect extends TransactionOperation("embeddingWork.inspectForAdmin")
  case EmbeddingWorkRepair extends TransactionOperation("embeddingWork.repairFailed")
  case AnalyticsErasureEnqueue extends TransactionOperation("analyticsErasure.enqueue")
  case AnalyticsErasureMarkComplete extends TransactionOperation("analyticsErasure.markComplete")
  case AnalyticsReportLatest extends TransactionOperation("analyticsReport.latest")
  case RecruiterJobClosureClose extends TransactionOperation("recruiterJobClosure.close")
  case JobCreate extends TransactionOperation("jobs.createWithEvents")
  case JobUpdate extends TransactionOperation("jobs.updateWithEvents")
  case UserBootstrap extends TransactionOperation("users.bootstrap")
  case UserCreateAccount extends TransactionOperation("users.createAccount")
  case UserUpdateProfile extends TransactionOperation("users.updateProfile")
  case UserDeleteAccount extends TransactionOperation("users.deleteAccount")
}

/** Startup-injected allow-lists of values owned by outer layers (adapter/driver failure classes, configuration keys).
  */
final case class PublicLogValues(errorTypes: String => Boolean, configKeys: Set[String])

object PublicLogValues {
  val none: PublicLogValues = PublicLogValues(_ => false, Set.empty)
}

object LogFields {

  /** The documented write-conflict sources (`repository.mongo.ConflictSource`, kept equal by a unit test). */
  val ConflictSourceLabels: Set[String] = Set(
    "receipt-fingerprint",
    "receipt-duplicate-key",
    "receipt-completion-no-match",
    "fence-duplicate-key",
    "fence-write-conflict",
    "status-write-conflict",
    "status-guard-no-match",
    "history-duplicate-key",
    "transaction-exhausted"
  )

  /** The closed refusal labels of the consumer receipt window. */
  val WindowRefusalLabels: Set[String] = ConsumerReceiptWindowRefusal.values.map(_.toString).toSet

  val TransactionOperationLabels: Set[String] = TransactionOperation.values.map(_.label).toSet

  val reasons: Set[String] = FailureReason.values.map(_.reason).toSet

  private val jdkErrorTypes = Set(
    "java.net.BindException",
    "java.net.ConnectException",
    "java.net.SocketTimeoutException",
    "java.util.concurrent.TimeoutException",
    "java.lang.IllegalArgumentException",
    "java.lang.IllegalStateException",
    "java.lang.RuntimeException",
    "java.io.IOException"
  )
  private val Root = "com.example.graphQL.cats."
  private val SearchKindWires: Set[String] =
    com.example.graphQL.cats.service.events.OperationalEventPayload.SearchKind.values.map(_.wire).toSet
  private def canonicalUuid(value: String): Boolean = Parsing.parseUuid(value).exists(_.toString == value)

  def failure(error: Throwable): Map[LogField, String] = {
    val location = error.getStackTrace.iterator
      .take(32)
      .find(frame => frame.getClassName.startsWith(Root))
      .filter(frame => frame.getFileName != null && frame.getLineNumber > 0)
      .fold("unavailable")(frame => s"${frame.getFileName}:${frame.getLineNumber}")
    Map(
      LogField.ErrorType -> error.getClass.getName,
      LogField.ErrorLocation -> location
    )
  }

  /** A repository failure as a safe diagnostic field: the stable upper-snake name of the case, never a message. */
  def repositoryFailure(error: RepositoryError): Map[LogField, String] =
    Map(
      LogField.ErrorCode -> (error match {
        case RepositoryError.DuplicateApplication    => "DUPLICATE_APPLICATION"
        case RepositoryError.AuthorityRevoked        => "AUTHORITY_REVOKED"
        case RepositoryError.Conflict                => "CONFLICT"
        case RepositoryError.InvalidEvent            => "INVALID_EVENT"
        case RepositoryError.InvalidStoredData       => "INVALID_STORED_DATA"
        case RepositoryError.MissingStoredResult     => "MISSING_STORED_RESULT"
        case RepositoryError.Unavailable             => "UNAVAILABLE"
        case RepositoryError.Overloaded              => "OVERLOADED"
        case RepositoryError.VectorSearchUnavailable => "VECTOR_SEARCH_UNAVAILABLE"
      })
    )

  /** Failure class names reach diagnostics raw; the sink reports any class outside the allow-lists as this value. */
  val OtherErrorType = "OtherException"

  private def publicErrorType(value: String, allowed: PublicLogValues): Boolean =
    jdkErrorTypes.contains(value) || allowed.errorTypes(value) || value == OtherErrorType

  def validPublic(field: LogField, value: String, allowed: PublicLogValues = PublicLogValues.none): Boolean =
    field match {
      case LogField.TransactionalId =>
        ProducerTransactionalId.parse(value).isRight
      case LogField.GenerationId => canonicalUuid(value)
      case LogField.Method => Set("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "OTHER").contains(value)
      case LogField.Route  => HiringHttpPaths.public.contains(value) || value == "_unmatched"
      case LogField.Status => value.toIntOption.exists(status => status >= 100 && status <= 599)
      case LogField.DurationMs | LogField.BodyBytes => value.toLongOption.exists(_ >= 0)
      case LogField.HttpPort                        => value.toIntOption.exists(port => port >= 1 && port <= 65535)
      case LogField.Reason                          => reasons.contains(value)
      case LogField.Outcome                         =>
        LogOutcome.values.exists(_.wire == value)
      case LogField.ConfigKey => allowed.configKeys.contains(value)
      case LogField.ErrorType =>
        publicErrorType(value, allowed)
      case LogField.ErrorLocation => value == "unavailable" || value.matches("[A-Za-z]+\\.scala:[1-9][0-9]{0,5}")
      case LogField.Environment   => Set("local", "production").contains(value)
      case LogField.TraceId       => value.matches("[0-9a-fA-F]{32}")
      case LogField.SpanId        => value.matches("[0-9a-fA-F]{16}")
      case LogField.EntityId | LogField.ActorId => canonicalUuid(value.toLowerCase(Locale.ROOT))
      case LogField.Count                       => value.toLongOption.exists(_ >= 0)
      case LogField.SpanName                    => value.matches("[A-Za-z][A-Za-z0-9_.-]{0,127}")
      case LogField.Worker                      => value.matches("[A-Za-z][A-Za-z0-9_.-]{0,63}")
      case LogField.Remote                      => Set("true", "false").contains(value)
      case LogField.JobStatus                   => Set("Draft", "Open", "Closed").contains(value)
      case LogField.Breaker                     => CircuitBreakerName.values.exists(_.wire == value)
      case LogField.BreakerTransition           => CircuitBreakerPolicy.Transition.values.exists(_.wire == value)
      case LogField.SearchKind                  => SearchKindWires.contains(value)
      case LogField.ErrorCode                   => value.matches("[A-Z][A-Z0-9_]{0,63}")
      case LogField.ConflictSource              => ConflictSourceLabels.contains(value)
      case LogField.TransactionOperation        => TransactionOperationLabels.contains(value)
      case LogField.WindowRefusal               => WindowRefusalLabels.contains(value)
      // Broker durations; -1 is meaningful (unbounded retention), anything else non-numeric stays filtered.
      case LogField.RetentionMs | LogField.SegmentMs | LogField.ReceiptTtlMs => value.matches("-1|0|[1-9][0-9]{0,17}")
      case _                                                                 => false
    }
}

trait Diagnostics[F[_]] {
  def event(event: LogEvent, requestId: Option[String] = None, fields: => Map[LogField, String] = Map.empty): F[Unit]
}

object Diagnostics {
  def noop[F[_]: Applicative]: Diagnostics[F] = new Diagnostics[F] {
    def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): F[Unit] =
      Applicative[F].unit
  }

  extension [F[_]](diagnostics: Diagnostics[F])
    /** Emits a diagnostic event; neither a synchronous throw nor an effect failure of the sink reaches the caller. */
    def emit(
        event: LogEvent,
        requestId: Option[String] = None,
        fields: => Map[LogField, String] = Map.empty
    )(using F: MonadThrow[F]): F[Unit] =
      F.catchNonFatal(diagnostics.event(event, requestId, fields)).flatten.handleError(_ => ())
}
