package com.example.graphQL.cats.repository.mongo

import cats.effect.{IO, IOLocal}
import cats.effect.unsafe.implicits.global
import com.example.graphQL.cats.infrastructure.telemetry.TelemetryCapture
import com.mongodb.ServerAddress
import com.mongodb.connection.{ClusterId, ConnectionDescription, ServerId}
import com.mongodb.event.{CommandStartedEvent, CommandSucceededEvent}
import com.mongodb.reactivestreams.client.ReactiveContextProvider
import org.bson.{BsonDocument, BsonInt32, BsonString}

/** Child JVM entry point for the flag-off test: runs without `-Dcats.effect.trackFiberContext`, builds a real driver
  * client with the telemetry installed, subscribes to an operation (an unreachable server, so the operation fails) and
  * replays a command through the listener. Exits non-zero on any unexpected failure.
  */
object MongoFlagOffProbe {
  private val unreachable = "mongodb://127.0.0.1:1"

  def main(args: Array[String]): Unit = {
    require(!IOLocal.isPropagating, "the probe must run with fiber context tracking off")
    // The same first step as Main.run.
    com.example.graphQL.cats.infrastructure.telemetry.TelemetryRuntime.selectContextStorage.unsafeRunSync()
    val outcome = TelemetryCapture.resource.use { capture =>
      val telemetry = MongoCommandTelemetry.create(capture.java)
      val instrumentation = telemetry.instrument(MongoClientRole.Hiring)
      MongoDatabaseProbe.clientResource(unreachable, instrumentation).use { client =>
        for {
          database <- client.getDatabase("probe")
          collection <- database.getCollection("applications")
          // Subscribing reaches the context provider before the failure.
          failure <- collection.count.attempt
          settings = MongoDatabaseProbe.effectiveSettings(unreachable, instrumentation)
          listener = settings.getCommandListeners.get(0)
          context = settings.getContextProvider.asInstanceOf[ReactiveContextProvider].getContext(null)
          description = new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress("127.0.0.1", 1)))
          command = new BsonDocument("find", new BsonString("applications"))
          reply = new BsonDocument("ok", new BsonInt32(1))
          _ <- IO.delay {
            listener.commandStarted(new CommandStartedEvent(context, 1L, 1, description, "probe", "find", command))
            listener.commandSucceeded(
              new CommandSucceededEvent(context, 1L, 1, description, "probe", "find", reply, 1000L)
            )
          }
          // An otel4s instrument must also work: with the fiber-backed storage left in place it throws.
          counter <- capture.otel4s.meterProvider.get("probe").flatMap(_.counter[Long]("probe.count").create)
          _ <- counter.inc()
          points <- capture.collectMetrics.map(TelemetryCapture.points)
        } yield {
          val unsupported = failure.left.toOption.exists(_.isInstanceOf[UnsupportedOperationException])
          require(!unsupported, s"flag-off subscribe threw $failure")
          require(failure.isLeft, "the unreachable server must fail the operation")
          require(points.exists(_.name == MongoCommandTelemetry.DurationInstrument), "the histogram must be recorded")
          require(points.exists(_.name == "probe.count"), "an otel4s counter must record")
          require(capture.finishedSpans.isEmpty, "no span without fiber context")
        }
      }
    }
    outcome.unsafeRunSync()
    println("FLAG_OFF_OK")
    sys.exit(0)
  }
}
