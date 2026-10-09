package com.example.hiring.analytics.adapter.spark

import com.example.hiring.analytics.errors.AnalyticsError
import com.example.hiring.analytics.errors.AnalyticsErrorTranslation.translating

import cats.effect.Async

private[spark] object LakehouseErrors {
  def adapt[F[_]: Async, A](work: F[A]): F[A] =
    work.translating(AnalyticsError.LakehouseFailure(_))
}
