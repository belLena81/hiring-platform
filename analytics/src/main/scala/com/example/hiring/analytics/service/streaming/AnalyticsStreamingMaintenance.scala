package com.example.hiring.analytics.service.streaming

import cats.effect.{Async, Resource}
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
    maintain: Instant => F[Unit]
) {
  private val F = Async[F]

  def runOnce: F[Unit] = lakehouseLock.resource(lakehouseRoot).attempt.use {
    case Left(AnalyticsError.LakehouseLockTimeout) => F.unit
    case Left(error)                               => F.raiseError(error)
    case Right(_)                                  => F.realTimeInstant.flatMap(maintain)
  }

  def run: F[Unit] = (F.sleep(interval) *> runOnce).foreverM

  /** Race the returned join effect against query execution; Resource cancels maintenance during shutdown. */
  def resource: Resource[F, F[Unit]] = run.background.map(joined => joined.flatMap(_.embedNever))
}
