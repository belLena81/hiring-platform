package com.example.graphQL.cats.infrastructure.telemetry

import cats.effect.{IO, Resource}
import cats.effect.unsafe.metrics.{IORuntimeMetrics => CatsIORuntimeMetrics}
import io.opentelemetry.api.{OpenTelemetry => JOpenTelemetry}
import io.opentelemetry.instrumentation.runtimetelemetry.RuntimeTelemetry
import org.typelevel.otel4s.instrumentation.ce.IORuntimeMetrics
import org.typelevel.otel4s.metrics.MeterProvider

/** JVM and Cats Effect runtime metrics, registered once at the runtime wiring (`TelemetryRuntime.resource`).
  *
  * JVM metrics come from the OpenTelemetry Java instrumentation `opentelemetry-runtime-telemetry` (`RuntimeTelemetry`)
  * with its default JMX-based set under the stable semantic-convention names: `jvm.memory.used`,
  * `jvm.memory.committed`, `jvm.memory.limit`, `jvm.memory.used_after_last_gc` (attributes `jvm.memory.type`,
  * `jvm.memory.pool.name`), `jvm.gc.duration` (`jvm.gc.name`, `jvm.gc.action`), `jvm.thread.count`
  * (`jvm.thread.daemon`, `jvm.thread.state`), `jvm.class.loaded`, `jvm.class.unloaded`, `jvm.class.count`,
  * `jvm.cpu.time`, `jvm.cpu.count`, `jvm.cpu.recent_utilization`. No experimental telemetry is enabled.
  *
  * Cats Effect metrics come from `otel4s-instrumentation-metrics` under the meter `cats.effect.runtime`: CPU starvation
  * (`cats.effect.runtime.cpu.starvation.count`, `.clock.drift.current`, `.clock.drift.max`) and the compute pool
  * (`cats.effect.runtime.wstp.compute.thread.count`, `.thread.active.count`, `.thread.searching.count`,
  * `.thread.blocked.count`, `.fiber.enqueued.count`, `.fiber.suspended.count`; attribute `pool.id`). The per-worker
  * families (`wstp.worker.*`, one series per worker index) stay off to bound series count.
  *
  * Nothing here carries request or hiring data. Metric attribute sets are fixed by the libraries.
  */
object RuntimeMetrics {
  private[telemetry] val catsEffectConfig: IORuntimeMetrics.Config = {
    import IORuntimeMetrics.Config.WorkStealingThreadPoolConfig as Pool
    IORuntimeMetrics.Config(
      IORuntimeMetrics.Config.CpuStarvationConfig.enabled,
      Pool(Pool.ComputeConfig.enabled, Pool.WorkerThreadsConfig.disabled)
    )
  }

  def register(
      otel: JOpenTelemetry,
      meterProvider: MeterProvider[IO],
      runtimeMetrics: CatsIORuntimeMetrics
  ): Resource[IO, Unit] = {
    given MeterProvider[IO] = meterProvider
    for {
      _ <- Resource.fromAutoCloseable(IO.delay(RuntimeTelemetry.create(otel)))
      _ <- IORuntimeMetrics.register[IO](runtimeMetrics, catsEffectConfig)
    } yield ()
  }
}
