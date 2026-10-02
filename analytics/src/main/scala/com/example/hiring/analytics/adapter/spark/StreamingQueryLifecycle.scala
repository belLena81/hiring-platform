package com.example.hiring.analytics.adapter.spark

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import com.example.hiring.analytics.errors.AnalyticsError

/** Adapter-private handle: waiting runs independently of the serialized Spark driver executor. */
private[analytics] trait StreamingQueryHandle[F[_]] {
  def awaitTermination: F[Unit]
  def stop: F[Unit]
}

private[analytics] trait StreamingQueryFactory[F[_]] {
  def start: F[StreamingQueryHandle[F]]
}

private[analytics] object StreamingQueryLifecycle {
  def resource[F[_]: Async](
      factory: StreamingQueryFactory[F],
      expiry: F[Unit],
      maintenance: Resource[F, F[Unit]]
  ): Resource[F, Unit] = {
    val F = Async[F]
    // Register a successful handle while masked so simultaneous expiry/cancellation cannot lose its finalizer.
    Resource
      .make(F.ref(Option.empty[StreamingQueryHandle[F]]))(owned => owned.get.flatMap(_.traverse_(_.stop)))
      .flatMap { owned =>
        val start = F.uncancelable(poll => poll(factory.start).flatTap(query => owned.set(Some(query))))
        Resource
          .eval(F.race(start, expiry).flatMap {
            case Left(query) => F.pure(query)
            case Right(_)    =>
              F.raiseError[StreamingQueryHandle[F]](
                AnalyticsError.InvalidConfiguration("analytics streaming activation ended during startup")
              )
          })
          .flatMap { query =>
            maintenance.flatMap { maintenanceFailure =>
              // Stop callbacks before waiting for maintenance cancellation. The outer owner covers failed acquisition.
              Resource.make(F.unit)(_ => owned.get.flatMap(_.traverse_(handle => handle.stop *> owned.set(None)))) *>
                Resource.eval(F.race(query.awaitTermination, F.race(expiry, maintenanceFailure)).void)
            }
          }
      }
  }
}
