package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.errors.AnalyticsErrorTranslation.translating

import cats.effect.Async

/** Composes the managed Spark executor with lakehouse-specific error translation. */
private[analytics] final class LakehouseOperation[F[_]: Async](sparkExecution: SparkExecution[F])
    extends SparkExecution[F] {
  override def apply[A](work: => A): F[A] = effect(sparkExecution(work))

  def effect[A](work: F[A]): F[A] = work.translating(AnalyticsError.LakehouseFailure(_))

  override def either[A](work: => Either[AnalyticsError, A]): F[A] = effect(sparkExecution.either(work))
}
