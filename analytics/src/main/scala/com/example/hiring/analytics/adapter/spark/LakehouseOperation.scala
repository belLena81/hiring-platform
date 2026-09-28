package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.Async
import scala.util.control.NonFatal

/** Shared boundary for blocking Spark/Delta operations and their expected analytics failures. */
private[analytics] trait LakehouseOperation[F[_]: Async] extends SparkExecution[F] {
  protected def sparkExecution: SparkExecution[F]

  final override def apply[A](work: => A): F[A] = adaptLakehouseErrors(sparkExecution(work))

  protected final def lakehouseIO[A](work: F[A]): F[A] = adaptLakehouseErrors(work)

  final override def either[A](work: => Either[AnalyticsError, A]): F[A] =
    adaptLakehouseErrors(sparkExecution.either(work))

  protected final def lakehouse[A](work: => A): F[A] = apply(work)

  protected final def lakehouseEither[A](work: => Either[AnalyticsError, A]): F[A] = either(work)

  private def adaptLakehouseErrors[A](work: F[A]): F[A] =
    Async[F].handleErrorWith(work) {
      case error: AnalyticsError => Async[F].raiseError(error)
      case NonFatal(cause)       => Async[F].raiseError(AnalyticsError.LakehouseFailure(cause))
      case error                 => Async[F].raiseError(error)
    }
}
