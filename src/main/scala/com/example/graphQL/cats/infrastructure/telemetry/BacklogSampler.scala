package com.example.graphQL.cats.infrastructure.telemetry

import cats.effect.{Clock, IO, Ref, Resource}
import cats.syntax.all.*
import com.example.graphQL.cats.service.{Diagnostics, LogEvent, LogField}
import com.example.graphQL.cats.service.Diagnostics.*
import scala.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration

/** One backlog sample and how old it is. A sample older than the freshness window is not [[fresh]]. */
final case class BacklogObservation(
    capability: BacklogCapability,
    snapshot: BacklogSnapshot,
    sampleAge: FiniteDuration,
    fresh: Boolean
)

/** Samples durable backlogs on a fixed interval into process-local memory.
  *
  * A failed or timed-out sample removes the previous one, so the capability becomes unobserved immediately rather than
  * reporting a stale or zero value. Sampling failures never propagate to the caller.
  */
final class BacklogSampler private (
    sources: List[(BacklogCapability, BacklogSource)],
    samples: Ref[IO, Map[BacklogCapability, BacklogSampler.Sample]],
    failing: Ref[IO, Set[BacklogCapability]],
    diagnostics: Diagnostics[IO],
    interval: FiniteDuration,
    freshness: FiniteDuration
) {

  /** Samples every source in parallel, so a slow source never delays or stales another. */
  def sampleAll: IO[Unit] = sources.parTraverse_((capability, source) => sampleOne(capability, source))

  private def sampleOne(capability: BacklogCapability, source: BacklogSource): IO[Unit] =
    for {
      wallClock <- Clock[IO].realTimeInstant
      outcome <- source.sample(wallClock).timeout(interval).attempt
      // Captured after the read: the sample is as old as the moment the read completed.
      taken <- Clock[IO].monotonic
      _ <- outcome.fold(
        error => samples.update(_ - capability) >> reportFailure(capability, error),
        snapshot =>
          samples.update(_.updated(capability, BacklogSampler.Sample(snapshot, taken))) >> failing.update(
            _ - capability
          )
      )
    } yield ()

  /** One bounded event when a capability becomes unobserved; repeated failures stay silent until it recovers. */
  private def reportFailure(capability: BacklogCapability, error: Throwable): IO[Unit] = {
    val reason = error match {
      case _: TimeoutException => "timeout"
      case _                   => "failure"
    }
    failing
      .modify(current => (current + capability, !current.contains(capability)))
      .flatMap(first =>
        diagnostics
          .emit(
            LogEvent.MongoRepositoryFailed,
            fields = Map(LogField.SpanName -> s"backlogSample.${capability.label}.$reason")
          )
          .whenA(first)
      )
  }

  /** Present samples with their age; failed samples are absent. */
  def observations: IO[List[BacklogObservation]] =
    (samples.get, Clock[IO].monotonic).mapN { (current, now) =>
      BacklogCapability.values.toList.flatMap { capability =>
        current.get(capability).map { sample =>
          val age = (now - sample.taken).max(scala.concurrent.duration.Duration.Zero)
          BacklogObservation(capability, sample.snapshot, age, age <= freshness)
        }
      }
    }

  /** Samples immediately, then once per interval, from a fiber owned by this resource. */
  def background: Resource[IO, Unit] =
    (for {
      started <- Clock[IO].monotonic
      _ <- sampleAll
      finished <- Clock[IO].monotonic
      _ <- IO.sleep((interval - (finished - started)).max(scala.concurrent.duration.Duration.Zero))
    } yield ()).foreverM.background.void
}

object BacklogSampler {
  private[telemetry] final case class Sample(snapshot: BacklogSnapshot, taken: FiniteDuration)

  /** A sample stays observable for this many intervals, which tolerates one missed tick. */
  private val FreshIntervals = 3

  def create(
      sources: List[(BacklogCapability, BacklogSource)],
      interval: FiniteDuration,
      diagnostics: Diagnostics[IO]
  ): IO[BacklogSampler] =
    (Ref.of[IO, Map[BacklogCapability, Sample]](Map.empty), Ref.of[IO, Set[BacklogCapability]](Set.empty)).mapN(
      new BacklogSampler(sources, _, _, diagnostics, interval, interval * FreshIntervals)
    )
}
