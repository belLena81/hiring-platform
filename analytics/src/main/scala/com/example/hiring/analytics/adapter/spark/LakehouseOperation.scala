package com.example.hiring.analytics.adapter.spark
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import cats.effect.IO

import scala.util.control.NonFatal

/** Shared boundary for blocking Spark/Delta operations and their expected analytics failures. */
private[analytics] trait LakehouseOperation {
  protected final def lakehouse[A](work: => A): IO[A] =
    adaptLakehouseErrors(IO.blocking(work))

  protected final def lakehouseIO[A](work: IO[A]): IO[A] =
    adaptLakehouseErrors(work)

  protected final def lakehouseEither[A](work: => Either[AnalyticsError, A]): IO[A] =
    lakehouse(work).flatMap(IO.fromEither)

  private def adaptLakehouseErrors[A](work: IO[A]): IO[A] =
    work.adaptError {
      case error: AnalyticsError => error
      case NonFatal(cause)       => AnalyticsError.LakehouseFailure(cause)
    }
}
