package com.example.graphQL.cats.repository.mongo

import com.mongodb.{MongoClientSettings, MongoCommandException, MongoExecutionTimeoutException, RequestContext}
import com.mongodb.event.{CommandFailedEvent, CommandListener, CommandStartedEvent, CommandSucceededEvent}
import com.mongodb.reactivestreams.client.ReactiveContextProvider
import io.opentelemetry.api.{OpenTelemetry => JOpenTelemetry}
import io.opentelemetry.api.common.{AttributeKey, Attributes, AttributesBuilder}
import io.opentelemetry.api.metrics.DoubleHistogram
import io.opentelemetry.api.trace.{Span, SpanKind, StatusCode, Tracer => JTracer}
import io.opentelemetry.context.{Context => JContext}
import cats.effect.IOLocal
import org.bson.{BsonDocument, BsonValue}
import org.reactivestreams.Subscriber

import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Which application client issued a command. Background polling is kept apart from request-serving traffic. */
enum MongoClientRole(val label: String) {
  case Hiring extends MongoClientRole("hiring")
  case BacklogSampler extends MongoClientRole("backlog_sampler")
}

/** Observes every MongoDB command at the driver boundary: one duration histogram point and, when a request span is
  * current, one client span.
  *
  * Why the driver and not the repositories: the repositories reach the driver through well over a hundred call sites,
  * and the command listener is the one place every command passes exactly once (a repository wrapper would also
  * double-count the commands of a transaction and of cursor `getMore`).
  *
  * Instruments and attributes (all values from closed sets):
  *   - histogram `db.client.operation.duration` (`s`, driver-measured monotonic time):
  *     - `db.system.name` = `mongodb`
  *     - `db.operation.name`: a command from [[MongoCommandTelemetry.Operations]] or `other`
  *     - `db.collection.name`: a collection from [[MongoCollections.All]], `other`, or absent for a command with none
  *     - `hiring.mongo.client`: `hiring` or `backlog_sampler`
  *     - `error.type` (only on failure): `duplicate_key`, `write_conflict`, `timeout`, `unavailable`, `write_error`,
  *       `command_error`
  *   - span `<operation> <collection>` (kind CLIENT) with the same attributes, as a child of the request span.
  *
  * Never read, stored or exported: the command document beyond its first key and value, filters, queries, update or
  * document contents, ids, error messages, server addresses, the database name and the connection string. Only the
  * numeric code of the first write error of an insert, update or delete reply is inspected.
  *
  * Span parenting. The driver calls the listener on its own threads, so the parent is captured when the operation is
  * subscribed to, which happens on the calling fiber: [[contextProvider]] stores the OpenTelemetry context current at
  * that moment in the driver's per-operation request context. It is the request span only when the Java context storage
  * follows the fiber (`-Dcats.effect.trackFiberContext=true`, see `TelemetryRuntime`). With no valid parent span
  * (background workers such as the outbox publisher and embedding workers, startup, or propagation off) no span is
  * created at all, so polling does not produce a trace per tick; the duration histogram is still recorded.
  */
final class MongoCommandTelemetry private (tracer: JTracer, duration: DoubleHistogram) {
  import MongoCommandTelemetry.*

  private final case class InFlight(
      role: MongoClientRole,
      operation: String,
      collection: Option[String],
      span: Option[Span]
  )

  private val inFlight = new ConcurrentHashMap[(Long, Int), InFlight]()

  private def key(event: com.mongodb.event.CommandEvent): (Long, Int) =
    (event.getConnectionDescription.getConnectionId.getLocalValue, event.getRequestId)

  private def attributes(
      flight: InFlight,
      errorType: Option[String]
  ): Attributes = {
    val builder: AttributesBuilder = Attributes.builder()
    builder.put(DbSystem, "mongodb")
    builder.put(DbOperation, flight.operation)
    flight.collection.foreach(builder.put(DbCollection, _))
    builder.put(ClientRole, flight.role.label)
    errorType.foreach(builder.put(ErrorType, _))
    builder.build()
  }

  private def listener(role: MongoClientRole): CommandListener = new CommandListener {
    override def commandStarted(event: CommandStartedEvent): Unit =
      guarded {
        val operation = operationOf(event.getCommandName)
        val collection = collectionOf(event.getCommandName, event.getCommand)
        val parent = Option(event.getRequestContext)
          .flatMap(context => Option(context.get[JContext](ParentKey)))
          .getOrElse(JContext.root())
        val span =
          if (Span.fromContext(parent).getSpanContext.isValid)
            Some(
              tracer
                .spanBuilder(collection.fold(operation)(name => s"$operation $name"))
                .setParent(parent)
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute(DbSystem, "mongodb")
                .setAttribute(DbOperation, operation)
                .setAttribute(ClientRole, role.label)
                .startSpan()
            )
          else None
        collection.foreach(name => span.foreach(_.setAttribute(DbCollection, name)))
        inFlight.put(key(event), InFlight(role, operation, collection, span))
        ()
      }

    override def commandSucceeded(event: CommandSucceededEvent): Unit =
      complete(event, event.getElapsedTime(TimeUnit.NANOSECONDS), writeFailureType(event.getResponse))

    override def commandFailed(event: CommandFailedEvent): Unit =
      complete(event, event.getElapsedTime(TimeUnit.NANOSECONDS), Some(failureType(event.getThrowable)))

    private def complete(event: com.mongodb.event.CommandEvent, nanos: Long, errorType: Option[String]): Unit =
      guarded {
        Option(inFlight.remove(key(event))).foreach { flight =>
          // The span always ends, even when recording the duration fails.
          try {
            flight.span.foreach { span =>
              errorType.foreach { value =>
                span.setAttribute(ErrorType, value)
                span.setStatus(StatusCode.ERROR)
              }
            }
            // Explicit root context: the implicit one is read from the fiber-backed context storage, which throws
            // when fiber context tracking is off.
            duration.record(nanos / 1e9, attributes(flight, errorType), JContext.root())
          } finally flight.span.foreach(_.end())
        }
      }
  }

  /** Telemetry must never fail a database operation. */
  private def guarded(body: => Unit): Unit =
    try body
    catch { case NonFatal(_) => () }

  /** Adds the command listener and the parent context capture to a client's settings. */
  def instrument(role: MongoClientRole)(builder: MongoClientSettings.Builder): MongoClientSettings.Builder =
    builder.addCommandListener(listener(role)).contextProvider(contextProvider)
}

object MongoCommandTelemetry {
  private val DbSystem = AttributeKey.stringKey("db.system.name")
  private val DbOperation = AttributeKey.stringKey("db.operation.name")
  private val DbCollection = AttributeKey.stringKey("db.collection.name")
  private val ClientRole = AttributeKey.stringKey("hiring.mongo.client")
  private val ErrorType = AttributeKey.stringKey("error.type")
  private val ParentKey = "hiring.otel.parent"
  private val InstrumentationName = "hiring-platform"

  val DurationInstrument = "db.client.operation.duration"

  /** Commands that carry their own label; every other command (authentication, handshake, admin) is `other`. */
  val Operations: Set[String] = Set(
    "find",
    "getMore",
    "aggregate",
    "count",
    "distinct",
    "insert",
    "update",
    "delete",
    "findAndModify",
    "commitTransaction",
    "abortTransaction",
    "createIndexes",
    "listIndexes",
    "listCollections",
    "collMod",
    "create",
    "dropIndexes",
    "drop",
    "killCursors",
    "ping"
  )

  /** Commands whose first command-document value is the collection name. */
  private val CollectionCommands: Set[String] = Set(
    "find",
    "aggregate",
    "count",
    "distinct",
    "insert",
    "update",
    "delete",
    "findAndModify",
    "createIndexes",
    "listIndexes",
    "collMod",
    "create",
    "dropIndexes",
    "drop"
  )

  private val Other = "other"

  // Explicit bucket advice from the OpenTelemetry database semantic conventions.
  private val Buckets: java.util.List[java.lang.Double] =
    List(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0).map(Double.box).asJava

  def create(otel: JOpenTelemetry): MongoCommandTelemetry = {
    val histogram = otel
      .getMeter(InstrumentationName)
      .histogramBuilder(DurationInstrument)
      .setUnit("s")
      .setDescription("Duration of MongoDB commands by bounded command and collection.")
      .setExplicitBucketBoundariesAdvice(Buckets)
      .build()
    new MongoCommandTelemetry(otel.getTracer(InstrumentationName), histogram)
  }

  /** Settings pass through unchanged. */
  val none: MongoClientSettings.Builder => MongoClientSettings.Builder = identity

  /** Stores the context current on the subscribing thread in the per-operation request context. */
  private val contextProvider: ReactiveContextProvider = new ReactiveContextProvider {
    override def getContext(subscriber: Subscriber[?]): RequestContext =
      new CapturedContext(currentContext)
  }

  /** The Java context storage is fiber-backed (`IOLocalContextStorage` by service loader), and reading it throws
    * `UnsupportedOperationException` when `cats.effect.trackFiberContext` is off. Then, and on any failure, there is no
    * parent: the root context.
    */
  private def currentContext: JContext =
    if (IOLocal.isPropagating)
      try JContext.current()
      catch { case NonFatal(_) => JContext.root() }
    else JContext.root()

  private final class CapturedContext(parent: JContext) extends RequestContext {
    private val entries = new ConcurrentHashMap[Object, Object]()
    entries.put(ParentKey, parent)
    override def get[T](key: Object): T = entries.get(key).asInstanceOf[T]
    override def hasKey(key: Object): Boolean = entries.containsKey(key)
    override def isEmpty: Boolean = entries.isEmpty
    override def put(key: Object, value: Object): Unit = { val _ = entries.put(key, value) }
    override def delete(key: Object): Unit = { val _ = entries.remove(key) }
    override def size(): Int = entries.size
    override def stream(): java.util.stream.Stream[java.util.Map.Entry[Object, Object]] = entries.entrySet.stream()
  }

  private[mongo] def operationOf(commandName: String): String =
    if (Operations.contains(commandName)) commandName else Other

  /** The collection a command addresses, from the allow-list only; only the first key and value are read. */
  private[mongo] def collectionOf(commandName: String, command: BsonDocument): Option[String] = {
    val raw: Option[BsonValue] =
      if (CollectionCommands.contains(commandName)) Option(command.get(commandName))
      else if (commandName == "getMore") Option(command.get("collection"))
      else None
    raw
      .filter(_.isString)
      .map(_.asString.getValue)
      .map(name => if (MongoCollections.All.contains(name)) name else Other)
  }

  /** A command that failed as a whole, by exception type and server code only. */
  private[mongo] def failureType(error: Throwable): String =
    error match {
      case MongoDuplicateKey(_)                                                           => "duplicate_key"
      case command: MongoCommandException if MongoErrors.isWriteConflict(command)         => "write_conflict"
      case command: MongoCommandException if command.getErrorCode == MaxTimeMsExpiredCode => "timeout"
      case _: MongoExecutionTimeoutException                                              => "timeout"
      case unreachable if MongoErrors.isDependencyUnavailable(unreachable)                => "unavailable"
      case _                                                                              => "command_error"
    }

  private val MaxTimeMsExpiredCode = 50
  private val WriteConflictCode = 112

  /** A write command that succeeded as a command but reports a write error: the first error's numeric code only. */
  private[mongo] def writeFailureType(reply: BsonDocument): Option[String] =
    Option(reply).flatMap(r => Option(r.get("writeErrors"))).filter(_.isArray).flatMap { errors =>
      errors.asArray.getValues.asScala.headOption.filter(_.isDocument).map { first =>
        Option(first.asDocument.get("code")).filter(_.isNumber).map(_.asNumber.intValue) match {
          case Some(MongoDuplicateKey.Code) => "duplicate_key"
          case Some(WriteConflictCode)      => "write_conflict"
          case _                            => "write_error"
        }
      }
    }
}
