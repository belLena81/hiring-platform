package com.example.graphQL.cats.infrastructure.telemetry

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.MeterProvider

/** Observable gauges over a [[BacklogSampler]]. The callbacks only read memory, never the database.
  *
  * Instruments (attribute `hiring.capability` is the only label):
  *   - `hiring.backlog.pending` `{item}`: claimable-now count, capped at 10000 (10000 means "at least"), emitted only
  *     for a fresh sample.
  *   - `hiring.backlog.oldest_pending_age` `s`: emitted only for a fresh, non-empty backlog.
  *   - `hiring.backlog.sample_age` `s`: age of the last successful sample; absent after a failed sample.
  */
object BacklogMetrics {
  private val MeterName = "hiring-platform"
  private val CapabilityKey = "hiring.capability"

  private def attributes(capability: BacklogCapability) = Attribute(CapabilityKey, capability.label)

  def register(meterProvider: MeterProvider[IO], sampler: BacklogSampler): Resource[IO, Unit] =
    registerObservations(meterProvider, sampler.observations)

  private[telemetry] def registerObservations(
      meterProvider: MeterProvider[IO],
      observations: IO[List[BacklogObservation]]
  ): Resource[IO, Unit] =
    Resource.eval(meterProvider.get(MeterName)).flatMap { meter =>
      // An instrument callback must not fail: a read error simply emits nothing.
      val current = observations.handleError(_ => Nil)
      for {
        _ <- meter
          .observableGauge[Long]("hiring.backlog.pending")
          .withUnit("{item}")
          .withDescription(
            "Durable work items claimable now (capped at 10000: a value of 10000 means at least that many); absent when the backlog sample is missing or stale."
          )
          .createWithCallback(measurement =>
            current.flatMap(
              _.filter(_.fresh).traverse_(o => measurement.record(o.snapshot.pending, attributes(o.capability)))
            )
          )
        _ <- meter
          .observableGauge[Double]("hiring.backlog.oldest_pending_age")
          .withUnit("s")
          .withDescription(
            "Approximate age of the oldest pending durable work item (first row by availability, not an exact minimum); absent for an empty or unobserved backlog."
          )
          .createWithCallback(measurement =>
            current.flatMap(
              _.filter(_.fresh).traverse_(o =>
                o.snapshot.oldestPendingAge
                  .traverse_(age => measurement.record(age.toMillis / 1000.0, attributes(o.capability)))
              )
            )
          )
        _ <- meter
          .observableGauge[Double]("hiring.backlog.sample_age")
          .withUnit("s")
          .withDescription("Age of the last successful backlog sample; absent after a failed sample.")
          .createWithCallback(measurement =>
            current
              .flatMap(_.traverse_(o => measurement.record(o.sampleAge.toMillis / 1000.0, attributes(o.capability))))
          )
      } yield ()
    }
}
