package com.example.graphQL.cats.service

import cats.effect.IO
import org.typelevel.otel4s.trace.Tracer

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
  case RequestCompleted extends LogEvent("REQUEST_COMPLETED", "HTTP", "Application response created", LogLevel.Info)
  case RequestCancelled extends LogEvent("REQUEST_CANCELLED", "HTTP", "Request cancelled; cleanup finished", LogLevel.Info)
  case GraphQLCompleted extends LogEvent("GRAPHQL_COMPLETED", "GRAPHQL", "GraphQL operation finished", LogLevel.Info)
  case MongoProbeFailed extends LogEvent("MONGO_PROBE_FAILED", "MONGO", "MongoDB ping failed", LogLevel.Warn)
  case LocalUnmasked extends LogEvent("LOCAL_UNMASKED", "SECURITY", "Diagnostic metadata masking is disabled", LogLevel.Warn)
  case SpanParameters extends LogEvent("SPAN_PARAMETERS", "TRACE", "Execution span parameters captured", LogLevel.Debug)
  case SpanStarted extends LogEvent("SPAN_STARTED", "TRACE", "Execution span started", LogLevel.Trace)
  case SpanSucceeded extends LogEvent("SPAN_SUCCEEDED", "TRACE", "Execution span succeeded", LogLevel.Trace)
  case SpanFailed extends LogEvent("SPAN_FAILED", "TRACE", "Execution span failed", LogLevel.Trace)
  case SpanCancelled extends LogEvent("SPAN_CANCELLED", "TRACE", "Execution span cancelled", LogLevel.Trace)

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
  val reasons: Set[String] = Set(
    "INVALID_REQUEST", "INVALID_QUERY", "UNSUPPORTED_MEDIA", "NOT_ACCEPTABLE", "PAYLOAD_TOO_LARGE",
    "OVERLOADED", "DEADLINE_EXCEEDED", "INTERNAL_ERROR", "METHOD_NOT_ALLOWED", "NOT_FOUND",
    "AUTHENTICATION_FAILED", "RATE_LIMITED", "DATABASE_UNAVAILABLE", "DATABASE_TIMEOUT", "DATABASE_NETWORK",
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
    "com.example.graphQL.cats.transport.http.HiringApiRoutes" -> "HiringApiRoutes.scala",
    "com.example.graphQL.cats.transport.graphql.HiringGraphQLSchema" -> "HiringGraphQLSchema.scala",
    "com.example.graphQL.cats.transport.graphql.RequestContext" -> "RequestContext.scala",
    "com.example.graphQL.cats.service.HealthService" -> "HealthService.scala",
    "com.example.graphQL.cats.repository.mongo.MongoDatabaseProbe" -> "MongoDatabaseProbe.scala",
    "com.example.graphQL.cats.repository.mongo.PublisherBridge" -> "PublisherBridge.scala",
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
    case LogField.Outcome => Set("COMPLETED", "REJECTED", "CANCELLED", "FIELD_ERROR", "READY", "NOT_READY").contains(value)
    case LogField.ConfigKey => Set(
      "CONFIG_FILE", "HTTP_HOST", "HTTP_PORT", "HTTP_ADMISSION_PERMITS", "MONGODB_URI", "MONGODB_DATABASE",
      "LOG_MASK_SENSITIVE", "AUTH_JWT_HS256_SECRET", "AUTH_JWT_ISSUER",
      "AUTH_JWT_AUDIENCE", "VECTOR_SEARCH_ENABLED", "VOYAGE_API_KEY", "VOYAGE_ENDPOINT",
      "VOYAGE_MODEL", "VOYAGE_DIMENSION", "EMBEDDING_VERSION", "EMBEDDING_QUEUE_SIZE",
      "EMBEDDING_PARALLELISM", "EMBEDDING_TIMEOUT_MS", "JOB_VECTOR_INDEX",
      "CANDIDATE_VECTOR_INDEX", "VECTOR_NUM_CANDIDATES"
    ).contains(value)
    case LogField.ErrorType => errorTypes.contains(value) || value == "OtherException"
    case LogField.ErrorLocation => value == "unavailable" || locations.values.exists { file =>
      value.startsWith(s"$file:") && value.drop(file.length + 1).matches("[1-9][0-9]{0,5}")
    }
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

  def emit(diagnostics: Diagnostics, event: LogEvent, requestId: Option[String] = None,
      fields: Map[LogField, String] = Map.empty): IO[Unit] =
    IO.defer(diagnostics.event(event, requestId, fields)).handleError(_ => ())

  /** Emits a complete lifecycle for an effect without changing its cancellation semantics. */
  def spanWith[A](diagnostics: Diagnostics, name: String, fields: Map[LogField, String] = Map.empty,
      requestId: Option[String] = None)(
      action: IO[A]
  )(using tracer: Tracer[IO]): IO[A] =
    tracer.span(name).surround {
      tracer.currentSpanContext.map(_.fold(fields + (LogField.SpanName -> name)) { context =>
        fields ++ Map(
          LogField.SpanName -> name,
          LogField.TraceId -> context.traceIdHex,
          LogField.SpanId -> context.spanIdHex
        )
      }).flatMap { base =>
        emit(diagnostics, LogEvent.SpanParameters, requestId, base) *>
          emit(diagnostics, LogEvent.SpanStarted, requestId, base) *>
          IO.monotonic.flatMap { started =>
            def terminal(event: LogEvent): IO[Unit] = IO.monotonic.flatMap { now =>
              emit(diagnostics, event, requestId, base + (LogField.DurationMs -> (now - started).toMillis.toString))
            }
            action.guaranteeCase {
              case cats.effect.kernel.Outcome.Succeeded(_) => terminal(LogEvent.SpanSucceeded)
              case cats.effect.kernel.Outcome.Errored(_) => terminal(LogEvent.SpanFailed)
              case cats.effect.kernel.Outcome.Canceled() => terminal(LogEvent.SpanCancelled)
            }
          }
      }
    }

  /** A boundary without an inbound request context (for example a repository adapter). */
  def operation[A](diagnostics: Diagnostics, name: String, fields: Map[LogField, String] = Map.empty)(
      action: IO[A]
  )(using tracer: Tracer[IO]): IO[A] =
    spanWith(diagnostics, name, fields)(action)
}
