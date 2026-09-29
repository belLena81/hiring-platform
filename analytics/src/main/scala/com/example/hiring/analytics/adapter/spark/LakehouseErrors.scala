package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.Async

import scala.util.control.NonFatal

private[spark] object LakehouseErrors {
  def adapt[F[_]: Async, A](work: F[A]): F[A] =
    Async[F].handleErrorWith(work) {
      case error: AnalyticsError => Async[F].raiseError(error)
      case NonFatal(cause)       => Async[F].raiseError(AnalyticsError.LakehouseFailure(cause))
      case error                 => Async[F].raiseError(error)
    }
}
