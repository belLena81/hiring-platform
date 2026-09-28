package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.errors.*
import cats.effect.Async
import cats.syntax.all.*
import scala.util.control.NonFatal

/** Shared boundary for blocking Spark/Delta operations and their expected analytics failures. */
private[analytics] trait LakehouseOperation[F[_]: Async] {
  protected def async: Async[F]

  protected final def lakehouse[A](work: => A): F[A] = adaptLakehouseErrors(async.blocking(work))

  protected final def lakehouseIO[A](work: F[A]): F[A] = adaptLakehouseErrors(work)

  protected final def lakehouseEither[A](work: => Either[AnalyticsError, A]): F[A] =
    async.flatMap(lakehouse(work))(value => async.fromEither(value))

  private def adaptLakehouseErrors[A](work: F[A]): F[A] =
    async.handleErrorWith(work) {
      case error: AnalyticsError => async.raiseError(error)
      case NonFatal(cause)       => async.raiseError(AnalyticsError.LakehouseFailure(cause))
      case error                 => async.raiseError(error)
    }
}
