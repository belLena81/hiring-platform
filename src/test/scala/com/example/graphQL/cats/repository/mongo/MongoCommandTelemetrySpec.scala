package com.example.graphQL.cats.repository.mongo

import cats.effect.IO
import com.example.graphQL.cats.infrastructure.telemetry.TelemetryCapture
import com.mongodb.{
  MongoClientSettings,
  MongoCommandException,
  MongoSocketReadTimeoutException,
  RequestContext,
  ServerAddress
}
import com.mongodb.connection.{ClusterId, ConnectionDescription, ServerId}
import com.mongodb.event.{CommandFailedEvent, CommandListener, CommandStartedEvent, CommandSucceededEvent}
import com.mongodb.reactivestreams.client.ReactiveContextProvider
import io.opentelemetry.api.trace.SpanKind
import io.opentelemetry.context.Context
import munit.CatsEffectSuite
import org.bson.{BsonArray, BsonDocument, BsonInt32, BsonString}

import scala.jdk.CollectionConverters.*

/** HOR-02 and HOR-03: MongoDB commands are observed with bounded labels and never export filters or documents. */
final class MongoCommandTelemetrySpec extends CatsEffectSuite {
  private val connection = new ConnectionDescription(
    new ServerId(new ClusterId(), new ServerAddress("db.internal", 27017))
  )
  private val secret = "jane.doe@example.com"
  private val resume = "RESUME-TEXT-PRIVATE"

  private final class Driver(settings: MongoClientSettings) {
    val listener: CommandListener = settings.getCommandListeners.get(0)
    val provider: ReactiveContextProvider = settings.getContextProvider.asInstanceOf[ReactiveContextProvider]
    private val ids = new java.util.concurrent.atomic.AtomicInteger(1)

    def context: RequestContext = provider.getContext(null)

    def succeed(
        command: String,
        document: BsonDocument,
        reply: BsonDocument = new BsonDocument("ok", new BsonInt32(1)),
        request: RequestContext = context
    ): Unit = {
      val id = ids.getAndIncrement()
      listener.commandStarted(new CommandStartedEvent(request, 1L, id, connection, "hiring", command, document))
      listener.commandSucceeded(
        new CommandSucceededEvent(request, 1L, id, connection, "hiring", command, reply, 2_000_000L)
      )
    }

    def fail(command: String, document: BsonDocument, error: Throwable): Unit = {
      val id = ids.getAndIncrement()
      val request = context
      listener.commandStarted(new CommandStartedEvent(request, 1L, id, connection, "hiring", command, document))
      listener.commandFailed(new CommandFailedEvent(request, 1L, id, connection, "hiring", command, 3_000_000L, error))
    }
  }

  private def driver(capture: TelemetryCapture, role: MongoClientRole = MongoClientRole.Hiring) =
    new Driver(MongoCommandTelemetry.create(capture.java).instrument(role)(MongoClientSettings.builder()).build())

  private def find(collection: String): BsonDocument =
    new BsonDocument("find", new BsonString(collection))
      .append("filter", new BsonDocument("email", new BsonString(secret)).append("resume", new BsonString(resume)))
      .append("$db", new BsonString("hiring_production_db"))

  private def histogram(capture: TelemetryCapture) =
    capture.collectMetrics.map(metrics =>
      TelemetryCapture.points(metrics).filter(_.name == MongoCommandTelemetry.DurationInstrument)
    )

  test("HOR-03 a command is recorded with its bounded operation, collection and client, in seconds") {
    TelemetryCapture.resource.use { capture =>
      driver(capture).succeed("find", find(MongoCollections.Applications))
      histogram(capture).map { points =>
        assertEquals(points.map(_.unit).distinct, List("s"))
        assertEquals(
          points.map(_.attributes),
          List(
            Map(
              "db.system.name" -> "mongodb",
              "db.operation.name" -> "find",
              "db.collection.name" -> "applications",
              "hiring.mongo.client" -> "hiring"
            )
          )
        )
      }
    }
  }

  test("HOR-02 no filter, document, id, database name or address reaches any exported metric or span") {
    TelemetryCapture.resource.use { capture =>
      val telemetry = driver(capture)
      val parent = capture.java.getTracer("test").spanBuilder("graphql.field.jobs").startSpan()
      val scope = parent.makeCurrent()
      val request = telemetry.context
      scope.close()
      telemetry.succeed("find", find(MongoCollections.Applications), request = request)
      telemetry.succeed(
        "insert",
        new BsonDocument("insert", new BsonString(MongoCollections.Users))
          .append(
            "documents",
            new BsonArray(java.util.List.of(new BsonDocument("passwordHash", new BsonString(resume))))
          ),
        reply = new BsonDocument("ok", new BsonInt32(1)),
        request = request
      )
      parent.end()
      histogram(capture).map { points =>
        val rendered = (points.toString + capture.finishedSpans
          .map(span => span.getName + span.getAttributes + span.getEvents + span.getLinks + span.getStatus)
          .toString)
        List(secret, resume, "hiring_production_db", "db.internal", "27017", "passwordHash", "jane").foreach { text =>
          assert(!rendered.contains(text), clue(s"$text leaked in $rendered"))
        }
        val spans = capture.finishedSpans.filter(_.getKind == SpanKind.CLIENT)
        assertEquals(spans.map(_.getName).toSet, Set("find applications", "insert users"))
        assertEquals(
          spans.map(_.getAttributes.asMap.asScala.keys.map(_.getKey).toSet).distinct,
          List(Set("db.system.name", "db.operation.name", "db.collection.name", "hiring.mongo.client"))
        )
      }
    }
  }

  test("HOR-02 an unknown collection and an unknown command collapse to other, and series stay bounded") {
    TelemetryCapture.resource.use { capture =>
      val telemetry = driver(capture)
      (1 to 200).foreach { index =>
        telemetry.succeed("find", find(s"candidate_$index"))
        telemetry.succeed(s"customCommand$index", new BsonDocument(s"customCommand$index", new BsonString(secret)))
      }
      telemetry.succeed("aggregate", new BsonDocument("aggregate", new BsonInt32(1)))
      histogram(capture).map { points =>
        val seen: Set[(Option[String], Option[String])] =
          points.map(p => p.attributes.get("db.operation.name") -> p.attributes.get("db.collection.name")).toSet
        assertEquals(
          seen,
          Set[(Option[String], Option[String])](
            Some("find") -> Some("other"),
            Some("other") -> None,
            Some("aggregate") -> None
          )
        )
        assertEquals(points.size, 3)
      }
    }
  }

  test("HOR-02 the collection allow-list lists every persisted collection name") {
    val declared = MongoCollections.getClass.getDeclaredFields.toList
      .filter(_.getType == classOf[String])
      .map { field => field.setAccessible(true); field.get(MongoCollections).asInstanceOf[String] }
    assert(declared.nonEmpty)
    assertEquals(declared.toSet.diff(MongoCollections.All), Set.empty[String])
  }

  test("HOR-03 failures are classified by type and server code only") {
    def command(code: Int, message: String) =
      new MongoCommandException(
        new BsonDocument("ok", new BsonInt32(0))
          .append("code", new BsonInt32(code))
          .append("errmsg", new BsonString(message)),
        new ServerAddress("db.internal", 27017)
      )
    TelemetryCapture.resource.use { capture =>
      val telemetry = driver(capture)
      telemetry.fail("update", find(MongoCollections.Jobs), command(112, s"WriteConflict $secret"))
      telemetry.fail("find", find(MongoCollections.Jobs), command(50, s"MaxTimeMSExpired $secret"))
      telemetry.fail(
        "find",
        find(MongoCollections.Jobs),
        new MongoSocketReadTimeoutException(s"timeout $secret", new ServerAddress("db.internal"), null)
      )
      telemetry.fail("find", find(MongoCollections.Jobs), command(2, s"BadValue $secret"))
      telemetry.succeed(
        "insert",
        new BsonDocument("insert", new BsonString(MongoCollections.Applications)),
        reply = new BsonDocument("ok", new BsonInt32(1))
          .append(
            "writeErrors",
            new BsonArray(
              java.util.List.of(new BsonDocument("code", new BsonInt32(11000)).append("errmsg", new BsonString(secret)))
            )
          )
      )
      histogram(capture).map { points =>
        assertEquals(
          points.flatMap(_.attributes.get("error.type")).sorted,
          List("command_error", "duplicate_key", "timeout", "unavailable", "write_conflict")
        )
        assert(!points.toString.contains(secret))
      }
    }
  }

  test("HOR-03 a failed command marks its span as an error without the exception message") {
    TelemetryCapture.resource.use { capture =>
      val telemetry = driver(capture)
      val parent = capture.java.getTracer("test").spanBuilder("graphql.field.jobs").startSpan()
      val scope = parent.makeCurrent()
      val request = telemetry.context
      scope.close()
      val id = 7
      telemetry.listener.commandStarted(
        new CommandStartedEvent(request, 1L, id, connection, "hiring", "find", find(MongoCollections.Jobs))
      )
      telemetry.listener.commandFailed(
        new CommandFailedEvent(request, 1L, id, connection, "hiring", "find", 1_000_000L, new RuntimeException(secret))
      )
      parent.end()
      IO {
        val span = capture.finishedSpans.find(_.getKind == SpanKind.CLIENT).get
        assertEquals(span.getStatus.getStatusCode, io.opentelemetry.api.trace.StatusCode.ERROR)
        assertEquals(
          span.getAttributes.get(io.opentelemetry.api.common.AttributeKey.stringKey("error.type")),
          "command_error"
        )
        assert(span.getEvents.isEmpty && !span.getStatus.getDescription.contains(secret))
      }
    }
  }

  test("HOR-03 a client span is a child of the span current when the operation was subscribed to") {
    TelemetryCapture.resource.use { capture =>
      val telemetry = driver(capture)
      val parent = capture.java.getTracer("test").spanBuilder("graphql.field.jobs").startSpan()
      val scope = parent.makeCurrent()
      val request = telemetry.context
      scope.close()
      telemetry.succeed("find", find(MongoCollections.Jobs), request = request)
      parent.end()
      IO {
        val child = capture.finishedSpans.find(_.getKind == SpanKind.CLIENT).get
        assertEquals(child.getParentSpanId, parent.getSpanContext.getSpanId)
        assertEquals(child.getTraceId, parent.getSpanContext.getTraceId)
      }
    }
  }

  test("HOR-03 background commands without a current span record the duration but create no span") {
    TelemetryCapture.resource.use { capture =>
      val telemetry = driver(capture, MongoClientRole.BacklogSampler)
      assert(Context.current() == Context.root())
      telemetry.succeed("find", find(MongoCollections.EmbeddingWork))
      histogram(capture).map { points =>
        assertEquals(points.map(_.attributes("hiring.mongo.client")), List("backlog_sampler"))
        assertEquals(capture.finishedSpans, Nil)
      }
    }
  }

  test("HOR-03 an event for a command that was never started is ignored") {
    TelemetryCapture.resource.use { capture =>
      val telemetry = driver(capture)
      telemetry.listener.commandSucceeded(
        new CommandSucceededEvent(
          telemetry.context,
          1L,
          99,
          connection,
          "hiring",
          "find",
          new BsonDocument("ok", new BsonInt32(1)),
          1L
        )
      )
      histogram(capture).map(points => assertEquals(points, Nil))
    }
  }

  test("HOR-03 operation labels come from the fixed allow-list") {
    assert(MongoCommandTelemetry.Operations.contains("find"))
    assertEquals(MongoCommandTelemetry.operationOf("saslStart"), "other")
    assertEquals(
      MongoCommandTelemetry.collectionOf(
        "getMore",
        new BsonDocument("getMore", new org.bson.BsonInt64(5L))
          .append("collection", new BsonString(MongoCollections.Jobs))
      ),
      Some("jobs")
    )
  }
}
