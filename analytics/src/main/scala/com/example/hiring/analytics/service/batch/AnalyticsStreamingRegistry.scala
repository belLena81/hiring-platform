package com.example.hiring.analytics.service.batch

import cats.effect.Async
import cats.syntax.all.*
import com.example.hiring.analytics.domain.AnalyticsLakehouseIdentity
import com.example.hiring.analytics.errors.AnalyticsError

/** Prevents one-shot batch work from bypassing an activated stream's event-time admission. */
trait AnalyticsStreamingRegistry[F[_]] {
  def rejectBatchIfRegistered(lakehouseRoot: String): F[Unit]
}

object AnalyticsStreamingRegistry {
  def allowUnregistered[F[_]: Async]: AnalyticsStreamingRegistry[F] = (_: String) => Async[F].unit

  def lakehouseId(root: String): Either[AnalyticsError, String] =
    AnalyticsLakehouseIdentity
      .from(root)
      .leftMap(_ => AnalyticsError.InvalidConfiguration("analytics lakehouse root is invalid"))

  def ownerLockRoot(root: String): Either[AnalyticsError, String] =
    lakehouseId(root).map(identity => s"file:///hiring-analytics-stream-owners/$identity")
}
