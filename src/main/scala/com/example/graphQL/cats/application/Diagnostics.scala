package com.example.graphQL.cats.application

import cats.effect.IO

enum LogEvent(val category: String, val component: String, val message: String, val severity: String, val rank: Int) {
  case ConfigInvalid extends LogEvent("CONFIG_INVALID", "CONFIG", "Application configuration rejected", "ERROR", 2)
  case MongoUnavailable extends LogEvent("MONGO_UNAVAILABLE", "READINESS", "Database readiness check failed", "WARN", 1)
  case MongoAuthFailed extends LogEvent("MONGO_AUTH_FAILED", "READINESS", "Database authentication failed", "WARN", 1)
  case RequestRejected extends LogEvent("REQUEST_REJECTED", "HTTP", "Request rejected", "WARN", 1)
  case StartupFailed extends LogEvent("STARTUP_FAILED", "RUNTIME", "Application startup failed", "ERROR", 2)
  case RuntimeFailed extends LogEvent("RUNTIME_FAILED", "RUNTIME", "Unhandled runtime failure", "ERROR", 2)
  case Started extends LogEvent("STARTED", "RUNTIME", "Application listening for requests", "INFO", 0)
  case Shutdown extends LogEvent("SHUTDOWN", "RUNTIME", "Application resources released", "INFO", 0)
  case RequestCompleted extends LogEvent("REQUEST_COMPLETED", "HTTP", "Application response created", "INFO", 0)
  case RequestCancelled extends LogEvent("REQUEST_CANCELLED", "HTTP", "Request cancelled; cleanup finished", "INFO", 0)
  case GraphQLCompleted extends LogEvent("GRAPHQL_COMPLETED", "GRAPHQL", "GraphQL operation finished", "INFO", 0)
  case RequestPayload extends LogEvent("REQUEST_PAYLOAD", "GRAPHQL", "Local filtered request payload", "INFO", 0)
  case MongoProbeFailed extends LogEvent("MONGO_PROBE_FAILED", "MONGO", "MongoDB ping failed", "WARN", 1)
  case LocalUnmasked extends LogEvent("LOCAL_UNMASKED", "SECURITY", "Local diagnostic metadata is unmasked; do not deploy", "WARN", 1)
  case LocalPayloadsEnabled extends LogEvent("LOCAL_PAYLOADS_ENABLED", "SECURITY", "Local filtered payload capture enabled; treat logs as sensitive", "WARN", 1)

  def marker: String = s"HP.$component.$category"
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
  case RequestPayload extends LogField("requestPayload", true)
}

object LogFields {
  val reasons: Set[String] = Set(
    "INVALID_REQUEST", "INVALID_QUERY", "UNSUPPORTED_MEDIA", "NOT_ACCEPTABLE", "PAYLOAD_TOO_LARGE",
    "OVERLOADED", "DEADLINE_EXCEEDED", "INTERNAL_ERROR", "METHOD_NOT_ALLOWED", "NOT_FOUND",
    "AUTHENTICATION_FAILED", "DATABASE_UNAVAILABLE", "DATABASE_TIMEOUT", "DATABASE_NETWORK",
    "DATABASE_ERROR", "EMPTY_RESULT", "CANCELLED", "PROBE_TIMEOUT", "CONFIG_INVALID", "BIND_FAILED",
    "STARTUP_FAILED", "RUNTIME_FAILED", "OPERATION_COMPLETED"
  )

  private val errorTypes = Set(
    "java.net.BindException", "java.net.ConnectException", "java.net.SocketTimeoutException",
    "java.util.concurrent.TimeoutException", "java.lang.IllegalArgumentException", "java.lang.IllegalStateException",
    "java.lang.RuntimeException", "java.io.IOException", "com.mongodb.MongoSecurityException",
    "com.mongodb.MongoTimeoutException", "com.mongodb.MongoSocketException", "com.mongodb.MongoSocketOpenException",
    "com.mongodb.MongoSocketReadException", "com.mongodb.MongoSocketReadTimeoutException", "com.mongodb.MongoCommandException"
  )
  private val locations = Map(
    "com.example.graphQL.cats.Main" -> "Main.scala",
    "com.example.graphQL.cats.api.http.HiringApiRoutes" -> "HiringApiRoutes.scala",
    "com.example.graphQL.cats.api.graphql.HiringGraphQLSchema" -> "HiringGraphQLSchema.scala",
    "com.example.graphQL.cats.api.graphql.RequestContext" -> "RequestContext.scala",
    "com.example.graphQL.cats.application.HealthService" -> "HealthService.scala",
    "com.example.graphQL.cats.infrastructure.mongo.MongoDatabaseProbe" -> "MongoDatabaseProbe.scala",
    "com.example.graphQL.cats.infrastructure.mongo.PublisherBridge" -> "PublisherBridge.scala",
    "com.example.graphQL.cats.runtime.HiringPlatformServer" -> "HiringPlatformServer.scala"
  )

  def failure(error: Throwable): Map[LogField, String] = scala.util.Try {
    val errorType = error.getClass.getName
    val location = error.getStackTrace.iterator.take(32).flatMap { frame =>
      locations.get(frame.getClassName.takeWhile(_ != '$')).filter(_ => frame.getLineNumber > 0 && frame.getLineNumber <= 999999)
        .map(file => s"$file:${frame.getLineNumber}")
    }.take(1).toList.headOption.getOrElse("unavailable")
    Map(LogField.ErrorType -> (if (errorTypes.contains(errorType)) errorType else "OtherException"),
      LogField.ErrorLocation -> location)
  }.getOrElse(Map(LogField.ErrorType -> "OtherException", LogField.ErrorLocation -> "unavailable"))

  def validPublic(field: LogField, value: String): Boolean = field match {
    case LogField.Method => Set("GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "OTHER").contains(value)
    case LogField.Route => Set("/health", "/ready", "/graphql", "/schema.graphql", "_unmatched").contains(value)
    case LogField.Status => value.toIntOption.exists(status => status >= 100 && status <= 599)
    case LogField.DurationMs | LogField.BodyBytes => value.toLongOption.exists(_ >= 0)
    case LogField.HttpPort => value.toIntOption.exists(port => port >= 1 && port <= 65535)
    case LogField.Reason => reasons.contains(value)
    case LogField.Outcome => Set("COMPLETED", "CANCELLED", "FIELD_ERROR", "READY", "NOT_READY").contains(value)
    case LogField.ConfigKey => Set("CONFIG_FILE", "HTTP_HOST", "HTTP_PORT", "MONGODB_URI", "MONGODB_DATABASE", "LOG_LEVEL", "LOG_MASK_SENSITIVE", "LOG_REQUEST_PAYLOADS").contains(value)
    case LogField.ErrorType => errorTypes.contains(value) || value == "OtherException"
    case LogField.ErrorLocation => value == "unavailable" || locations.values.exists { file =>
      value.startsWith(s"$file:") && value.drop(file.length + 1).matches("[1-9][0-9]{0,5}")
    }
    case LogField.Environment => Set("local", "production").contains(value)
    case _ => false
  }
}

trait Diagnostics {
  def payloadsEnabled: Boolean = false
  def event(event: LogEvent, requestId: Option[String] = None, fields: Map[LogField, String] = Map.empty): IO[Unit]
}

object Diagnostics {
  val noop: Diagnostics = new Diagnostics {
    def event(event: LogEvent, requestId: Option[String], fields: Map[LogField, String]): IO[Unit] = IO.unit
  }

  def emit(diagnostics: Diagnostics, event: LogEvent, requestId: Option[String] = None,
      fields: Map[LogField, String] = Map.empty): IO[Unit] =
    IO.defer(diagnostics.event(event, requestId, fields)).handleError(_ => ())
}
