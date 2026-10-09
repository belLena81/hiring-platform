package com.example.hiring.analytics

import com.example.hiring.analytics.service.batch.AnalyticsLakehouseLock

import cats.effect.{Async, Ref, Resource}
import cats.effect.std.Mutex
import cats.syntax.all.*

object AnalyticsTestLakehouseLocks {

  /** Process-local ownership for explicitly selected local/test compositions. */
  def processLocal[F[_]: Async]: Resource[F, AnalyticsLakehouseLock[F]] =
    Resource.eval(Ref.of[F, Map[String, Mutex[F]]](Map.empty)).map { locks => (root: String) =>
      Resource
        .eval(
          Mutex[F].flatMap { candidate =>
            locks.modify { current =>
              current.get(root) match {
                case Some(existing) => current -> existing
                case None           => current.updated(root, candidate) -> candidate
              }
            }
          }
        )
        .flatMap(_.lock)
    }
}
