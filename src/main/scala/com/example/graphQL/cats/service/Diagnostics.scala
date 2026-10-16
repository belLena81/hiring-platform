package com.example.graphQL.cats.service

import cats.{Applicative, MonadThrow}
import cats.syntax.all.*
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy
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
  case Started extends LogEvent("STARTED", "RUNTIME", "Application listening for requests", LogLevel.Info)
  case Shutdown extends LogEvent("SHUTDOWN", "RUNTIME", "Application resources released", LogLevel.Info)
  case GraphQLCompleted extends LogEvent("GRAPHQL_COMPLETED", "GRAPHQL", "GraphQL operation finished", LogLevel.Info)
  case MongoProbeFailed extends LogEvent("MONGO_PROBE_FAILED", "MONGO", "MongoDB ping failed", LogLevel.Warn)
  case MongoRepositoryFailed
      extends LogEvent("MONGO_REPOSITORY_FAILED", "MONGO", "Mongo repository operation failed", LogLevel.Error)
  case MongoSetupFailed extends LogEvent("MONGO_SETUP_FAILED", "MONGO", "MongoDB setup failed", LogLevel.Error)
  case AccessTokenIssuanceFailed
      extends LogEvent("ACCESS_TOKEN_ISSUANCE_FAILED", "AUTH", "Access token issuance failed", LogLevel.Error)
  case EmbeddingWakeFailed
      extends LogEvent("EMBEDDING_WAKE_FAILED", "SEARCH", "Embedding work wake failed", LogLevel.Warn)
  case EmbeddingProcessingFailed
      extends LogEvent("EMBEDDING_PROCESSING_FAILED", "SEARCH", "Embedding work processing failed", LogLevel.Warn)
  case EmbeddingProviderFailed
      extends LogEvent("EMBEDDING_PROVIDER_FAILED", "SEARCH", "Embedding provider request failed", LogLevel.Warn)
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
}

/** Startup-injected allow-lists of values owned by outer layers (adapter/driver failure classes, configuration keys).
  */
final case class PublicLogValues(errorTypes: String => Boolean, configKeys: Set[String])

object PublicLogValues {
  val none: PublicLogValues = PublicLogValues(_ => false, Set.empty)
}

object LogFields {
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

  private val transactionalIdPrefixes =
    List("hiring-publisher-", "hiring-interview-orchestrator-", "hiring-interview-worker-")

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

  /** Failure class names reach diagnostics raw; the sink reports any class outside the allow-lists as this value. */
  val OtherErrorType = "OtherException"

  private def publicErrorType(value: String, allowed: PublicLogValues): Boolean =
    jdkErrorTypes.contains(value) || allowed.errorTypes(value) || value == OtherErrorType

  def validPublic(field: LogField, value: String, allowed: PublicLogValues = PublicLogValues.none): Boolean =
    field match {
      case LogField.TransactionalId =>
        transactionalIdPrefixes.exists(prefix => value.startsWith(prefix) && canonicalUuid(value.drop(prefix.length)))
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
      case _                                    => false
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
