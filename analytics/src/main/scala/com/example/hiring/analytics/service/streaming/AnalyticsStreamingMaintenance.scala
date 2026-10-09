package com.example.hiring.analytics.service.streaming

import cats.effect.{Async, Outcome, Ref, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.service.batch.AnalyticsLakehouseLock

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** Idle maintenance shares callback ownership. A busy mutex defers a tick; processing failures terminate the query
  * owner. Continuous contention can postpone maintenance, so successful ticks and retention still need runtime
  * evidence.
  */
final class AnalyticsStreamingMaintenance[F[_]: Async](
    lakehouseRoot: String,
    lakehouseLock: AnalyticsLakehouseLock[F],
    interval: FiniteDuration,
    maintain: Instant => F[Unit],
    observe: Option[AnalyticsStreamingMaintenance.Observation => F[Unit]] = None
) {
  import AnalyticsStreamingMaintenance.*

  /** Finish an acquired tick before shutdown releases its mutex and the shared Spark resources. */
  private def maintainOwned: F[Unit] = Async[F].uncancelable(_ => Async[F].realTimeInstant.flatMap(maintain))

  private final case class State(
      started: FiniteDuration,
      lastSuccessMonotonic: Option[FiniteDuration],
      lastSuccess: Option[Instant],
      lastAttempt: Option[Instant],
      outcome: Option[TickOutcome],
      consecutiveDeferrals: Long,
      maximumElapsed: FiniteDuration
  ) {
    def observation(now: FiniteDuration): Observation =
      Observation(
        lastSuccess,
        lastAttempt,
        outcome,
        consecutiveDeferrals,
        now - lastSuccessMonotonic.getOrElse(started),
        maximumElapsed.max(now - lastSuccessMonotonic.getOrElse(started))
      )
  }

  private def snapshot(state: Ref[F, State]): F[Observation] =
    (state.get, Async[F].monotonic).mapN((current, now) => current.observation(now))

  private def record(state: Ref[F, State], outcome: TickOutcome): F[Unit] =
    (Async[F].realTimeInstant, Async[F].monotonic).tupled.flatMap { case (at, now) =>
      state.update { previous =>
        val current = previous.copy(maximumElapsed =
          previous.maximumElapsed.max(now - previous.lastSuccessMonotonic.getOrElse(previous.started))
        )
        outcome match {
          case TickOutcome.Started   => current.copy(lastAttempt = Some(at), outcome = Some(outcome))
          case TickOutcome.Succeeded =>
            current.copy(
              lastSuccess = Some(at),
              lastSuccessMonotonic = Some(now),
              outcome = Some(outcome),
              consecutiveDeferrals = 0L
            )
          case TickOutcome.Deferred =>
            current.copy(outcome = Some(outcome), consecutiveDeferrals = current.consecutiveDeferrals + 1L)
          case _ => current.copy(outcome = Some(outcome))
        }
      } *> observe.traverse_(callback => snapshot(state).flatMap(callback))
    }

  /** One tick: a busy mutex defers, any other lock failure propagates, and an acquired mutex runs maintenance. */
  private[analytics] def tick: F[TickOutcome] =
    lakehouseLock
      .resource(lakehouseRoot)
      .attempt
      .use {
        case Left(AnalyticsError.LakehouseLockTimeout) => Async[F].pure(TickOutcome.Deferred)
        case Left(error)                               => Async[F].raiseError[TickOutcome](error)
        case Right(_)                                  => maintainOwned.as(TickOutcome.Succeeded)
      }

  private def observedTick(state: Ref[F, State]): F[Unit] =
    (record(state, TickOutcome.Started) *> tick.flatMap(record(state, _))).guaranteeCase {
      case Outcome.Canceled()   => record(state, TickOutcome.Cancelled)
      case Outcome.Errored(_)   => record(state, TickOutcome.Failed)
      case Outcome.Succeeded(_) => Async[F].unit
    }

  /** State is allocated for each runtime; elapsed time also advances while no tick can acquire ownership. */
  def observedResource: Resource[F, Runtime[F]] =
    for {
      started <- Resource.eval(Async[F].monotonic)
      state <- Resource.eval(
        Ref.of[F, State](State(started, None, None, None, None, 0L, scala.concurrent.duration.Duration.Zero))
      )
      _ <- Resource.make(Async[F].unit)(_ => record(state, TickOutcome.Stopped))
      joined <- (Async[F].sleep(interval) *> observedTick(state)).foreverM[Unit].background
    } yield Runtime(joined.flatMap(_.embedNever), snapshot(state))

  /** Race the returned join effect against query execution; Resource cancels maintenance during shutdown. */
  def resource: Resource[F, F[Unit]] = observedResource.map(_.join)
}

object AnalyticsStreamingMaintenance {
  enum TickOutcome {
    case Started, Succeeded, Deferred, Failed, Cancelled, Stopped
  }

  /** Sanitized process-local progress, containing no storage identity, payload, or exception detail. Without a
    * successful tick, elapsed time is measured from runtime acquisition.
    */
  final case class Observation(
      lastSuccess: Option[Instant],
      lastAttempt: Option[Instant],
      outcome: Option[TickOutcome],
      consecutiveDeferrals: Long,
      elapsedSinceSuccess: FiniteDuration,
      maximumElapsedSinceSuccess: FiniteDuration
  )

  final case class Runtime[F[_]](join: F[Unit], observation: F[Observation])
}
