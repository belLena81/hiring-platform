package com.example.graphQL.cats.service

import cats.effect.IO
import com.example.graphQL.cats.shared.HiringHttpPaths

enum LogLevel {
  case Trace, Debug, Info, Warn, Error

  def label: String = productPrefix.toUpperCase(java.util.Locale.ROOT)
}

enum LogEvent(val category: String, val component: String, val message: String, val level: LogLevel) {
  case ConfigInvalid extends LogEvent("CONFIG_INVALID", "CONFIG", "Application configuration rejected", LogLevel.Error)
  case MongoUnavailable extends LogEvent("MONGO_UNAVAILABLE", "READINESS", "Database readiness check failed", LogLevel.Warn)
  case MongoAuthFailed extends LogEvent("MONGO_AUTH_FAILED", "READINESS", "Database authentication failed", LogLevel.Warn)
  case RequestRejected extends LogEvent("REQUEST_REJECTED", "HTTP", "Request rejected", LogLevel.Warn)
  case StartupFailed extends LogEvent("STARTUP_FAILED", "RUNTIME", "Application startup failed", LogLevel.Error)
  case RuntimeFailed extends LogEvent("RUNTIME_FAILED", "RUNTIME", "Unhandled runtime failure", LogLevel.Error)
  case Started extends LogEvent("STARTED", "RUNTIME", "Application listening for requests", LogLevel.Info)
  case Shutdown extends LogEvent("SHUTDOWN", "RUNTIME", "Application resources released", LogLevel.Info)
  case GraphQLCompleted extends LogEvent("GRAPHQL_COMPLETED", "GRAPHQL", "GraphQL operation finished", LogLevel.Info)
  case MongoProbeFailed extends LogEvent("MONGO_PROBE_FAILED", "MONGO", "MongoDB ping failed", LogLevel.Warn)
  case MongoSetupFailed extends LogEvent("MONGO_SETUP_FAILED", "MONGO", "MongoDB setup failed", LogLevel.Error)
  case LocalUnmasked extends LogEvent("LOCAL_UNMASKED", "SECURITY", "Diagnostic metadata masking is disabled", LogLevel.Warn)

  def marker: String = s"HP.$component.$category"
  def severity: String = level.label
}

enum LogField(val key: String, val sensitive: Boolean = false) {
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
  case EntityId extends LogField("entityId", true)
  case ActorId extends LogField("actorId", true)
  case Count extends LogField("count")
  case Title extends LogField("title", true)
  case Country extends LogField("country", true)
  case City extends LogField("city", true)
  case Skills extends LogField("skills", true)
  case Remote extends LogField("remote")
  case JobStatus extends LogField("jobStatus")
}

object LogFields {
  val reasons: Set[String] = FailureReason.values.map(_.reason).toSet

  private val errorTypes = Set(
    "java.net.BindException", "java.net.ConnectException", "java.net.SocketTimeoutException",
    "java.util.concurrent.TimeoutException", "java.lang.IllegalArgumentException", "java.lang.IllegalStateException",
    "java.lang.RuntimeException", "java.io.IOException", "com.mongodb.MongoSecurityException",
    "com.mongodb.MongoTimeoutException", "com.mongodb.MongoSocketException", "com.mongodb.MongoSocketOpenException",
    "com.mongodb.MongoSocketReadException", "com.mongodb.MongoSocketReadTimeoutException", "com.mongodb.MongoCommandException"
  )
  private val Root = "com.example.graphQL.cats."

  def failure(error: Throwable): Map[LogField, String] = {
    val errorType = error.getClass.getName
    val location = error.getStackTrace.iterator.take(32)
      .find(frame => frame.getClassName.startsWith(Root))
      .filter(frame => frame.getFileName != null && frame.getLineNumber > 0)
      .fold("unavailable")(frame => s"${frame.getFileName}:${frame.getLineNumber}")
    Map(LogField.ErrorType -> (if (errorTypes.contains(errorType)) errorType else "OtherException"),
      LogField.ErrorLocation -> location)
  }

  def validPublic(field: LogField, value: String): Boolean = field match {
    case LogField.Method => Set("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "OTHER").contains(value)
    case LogField.Route => HiringHttpPaths.public.contains(value) || value == "_unmatched"
    case LogField.Status => value.toIntOption.exists(status => status >= 100 && status <= 599)
    case LogField.DurationMs | LogField.BodyBytes => value.toLongOption.exists(_ >= 0)
    case LogField.HttpPort => value.toIntOption.exists(port => port >= 1 && port <= 65535)
    case LogField.Reason => reasons.contains(value)
    case LogField.Outcome => Set("COMPLETED", "REJECTED", "CANCELLED", "FIELD_ERROR", "READY", "NOT_READY").contains(value)
    case LogField.ConfigKey => com.example.graphQL.cats.config.ConfigError.publicKeys.contains(value)
    case LogField.ErrorType => errorTypes.contains(value) || value == "OtherException"
    case LogField.ErrorLocation => value == "unavailable" || value.matches("[A-Za-z]+\\.scala:[1-9][0-9]{0,5}")
    case LogField.Environment => Set("local", "production").contains(value)
    case LogField.TraceId => value.matches("[0-9a-fA-F]{32}")
    case LogField.SpanId => value.matches("[0-9a-fA-F]{16}")
    case LogField.EntityId | LogField.ActorId =>
      value.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
    case LogField.Count => value.toLongOption.exists(_ >= 0)
    case LogField.SpanName => value.matches("[A-Za-z][A-Za-z0-9_.-]{0,127}")
    case LogField.Remote => Set("true", "false").contains(value)
    case LogField.JobStatus => Set("Draft", "Open", "Closed").contains(value)
    case _ => false
  }
}

trait Diagnostics {
  def event(event: LogEvent, requestId: Option[String] = None, fields: => Map[LogField, String] = Map.empty): IO[Unit]
}

object Diagnostics {
  val noop: Diagnostics = new Diagnostics {
    def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] = IO.unit
  }

  extension (diagnostics: Diagnostics)
    def emit(event: LogEvent, requestId: Option[String] = None,
        fields: => Map[LogField, String] = Map.empty): IO[Unit] =
      IO.defer(diagnostics.event(event, requestId, fields)).handleError(_ => ())
}
