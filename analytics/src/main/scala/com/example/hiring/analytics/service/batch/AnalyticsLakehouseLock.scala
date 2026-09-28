package com.example.hiring.analytics.service.batch

import cats.effect.{Async, Resource}
import cats.effect.Ref
import cats.effect.std.Semaphore
import cats.syntax.all.*

/** Serializes every repository-managed batch, erasure, and retention operation for one lakehouse root. */
private[analytics] trait AnalyticsLakehouseLock[F[_]] {
  def resource(root: String): Resource[F, Unit]
}

/** Process-local lock for explicitly selected local/test compositions. */
private[analytics] object AnalyticsLakehouseLock {
  def processLocal[F[_]: Async]: Resource[F, AnalyticsLakehouseLock[F]] =
    Resource.eval(Ref.of[F, Map[String, Semaphore[F]]](Map.empty)).map { locks => (root: String) =>
      Resource
        .eval(
          Semaphore[F](1L).flatMap { candidate =>
            locks.modify { current =>
              current.get(root) match {
                case Some(existing) => current -> existing
                case None           => current.updated(root, candidate) -> candidate
              }
            }
          }
        )
        .flatMap(_.permit)
    }
}
