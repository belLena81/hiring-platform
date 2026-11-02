package com.example.graphQL.cats.infrastructure.telemetry

import cats.effect.IO
import munit.CatsEffectSuite
import org.typelevel.otel4s.oteljava.OtelJava

/** HOR-02 and HOR-11: JVM and Cats Effect runtime metrics register through the OpenTelemetry boundary. */
final class RuntimeMetricsSpec extends CatsEffectSuite {
  private val jvmMetrics =
    Set("jvm.memory.used", "jvm.memory.committed", "jvm.thread.count", "jvm.cpu.time", "jvm.class.loaded")
  private val catsEffectMetrics = Set(
    "cats.effect.runtime.cpu.starvation.count",
    "cats.effect.runtime.cpu.starvation.clock.drift.current",
    "cats.effect.runtime.cpu.starvation.clock.drift.max",
    "cats.effect.runtime.wstp.compute.thread.count",
    "cats.effect.runtime.wstp.compute.thread.active.count",
    "cats.effect.runtime.wstp.compute.fiber.enqueued.count"
  )

  test("HOR-11 heap, GC, threads, CPU and Cats Effect runtime metrics are exported once registered") {
    TelemetryCapture.resource.use { capture =>
      RuntimeMetrics.register(capture.java, capture.otel4s.meterProvider, munitIORuntime.metrics).use { _ =>
        IO.blocking(System.gc()) *> capture.collectMetrics.map { metrics =>
          val names = metrics.map(_.getName).toSet
          assertEquals((jvmMetrics ++ catsEffectMetrics).diff(names), Set.empty[String])
          assert(names.contains("jvm.gc.duration") || names.contains("jvm.memory.used_after_last_gc"), clue(names))
        }
      }
    }
  }

  test("HOR-02 runtime metric attributes carry no request data and the per-worker families stay off") {
    TelemetryCapture.resource.use { capture =>
      RuntimeMetrics.register(capture.java, capture.otel4s.meterProvider, munitIORuntime.metrics).use { _ =>
        capture.collectMetrics.map { metrics =>
          val points = TelemetryCapture.points(metrics)
          assert(points.nonEmpty)
          assert(!metrics.map(_.getName).exists(_.startsWith("cats.effect.runtime.wstp.worker")))
          val keys = points.flatMap(_.attributes.keys).toSet
          assertEquals(
            keys.diff(
              Set(
                "jvm.memory.type",
                "jvm.memory.pool.name",
                "jvm.gc.name",
                "jvm.gc.action",
                "jvm.thread.daemon",
                "jvm.thread.state",
                "pool.id",
                "jvm.gc.cause"
              )
            ),
            Set.empty[String]
          )
        }
      }
    }
  }

  test("HOR-11 registration is released with its resource and may be repeated") {
    TelemetryCapture.resource.use { capture =>
      val once = RuntimeMetrics.register(capture.java, capture.otel4s.meterProvider, munitIORuntime.metrics)
      once.use(_ => IO.unit) *> once.use(_ => IO.unit)
    }
  }

  test("HOR-03 with fiber context tracking the Java current context inside IO.delay is the otel4s current span") {
    TelemetryCapture.resource.use { capture =>
      for {
        provider <- TelemetryRuntime.contextProvider
        otel <- OtelJava.fromJOpenTelemetry[IO](capture.java)(using summon, provider)
        tracer <- otel.tracerProvider.get("test")
        seen <- tracer.span("graphql.field.jobs").use { span =>
          IO.delay(io.opentelemetry.api.trace.Span.current().getSpanContext)
            .map(java => java.getSpanId -> span.context.spanIdHex)
        }
      } yield assertEquals(seen._1, seen._2)
    }
  }
}
