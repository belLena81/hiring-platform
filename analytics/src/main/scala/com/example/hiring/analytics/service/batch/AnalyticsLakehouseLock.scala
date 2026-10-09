package com.example.hiring.analytics.service.batch

import cats.effect.{Async, Resource}
import cats.effect.Ref
import cats.effect.std.Semaphore
import cats.syntax.all.*

/** Serializes every repository-managed batch, erasure, and retention operation for one lakehouse root. */
private[analytics] trait AnalyticsLakehouseLock[F[_]] {
  def resource(root: String): Resource[F, Unit]
}

/** Resource-owned lakehouse lock composition. */
private[analytics] object AnalyticsLakehouseLock {

  /** One runtime's contenders for each root queue in FIFO order before persistent ownership. The stream-owner root
    * remains independent of the callback/maintenance root. Gates live for this resource's lifetime (streaming uses
    * these two configured roots); Mongo acquisition retains its own timeout after cancellable local waiting.
    */
  def serialized[F[_]: Async](delegate: AnalyticsLakehouseLock[F]): Resource[F, AnalyticsLakehouseLock[F]] =
    Resource.make(Ref.of[F, Map[String, Semaphore[F]]](Map.empty))(_.set(Map.empty)).map { permits => (root: String) =>
      Resource
        .eval(
          Semaphore[F](1L).flatMap { candidate =>
            permits.modify { current =>
              current.get(root) match {
                case Some(existing) => current -> existing
                case None           => current.updated(root, candidate) -> candidate
              }
            }
          }
        )
        .flatMap(_.permit) *> delegate.resource(root)
    }
}
