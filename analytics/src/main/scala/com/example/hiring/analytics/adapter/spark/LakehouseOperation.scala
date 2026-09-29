package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.errors.AnalyticsError

import cats.effect.Async

/** Composes the managed Spark executor with lakehouse-specific error translation. */
private[analytics] final class LakehouseOperation[F[_]: Async](sparkExecution: SparkExecution[F])
    extends SparkExecution[F] {
  override def apply[A](work: => A): F[A] = adaptLakehouseErrors(sparkExecution(work))

  def effect[A](work: F[A]): F[A] = adaptLakehouseErrors(work)

  override def either[A](work: => Either[AnalyticsError, A]): F[A] =
    adaptLakehouseErrors(sparkExecution.either(work))

  private def adaptLakehouseErrors[A](work: F[A]): F[A] = LakehouseErrors.adapt(work)
}
