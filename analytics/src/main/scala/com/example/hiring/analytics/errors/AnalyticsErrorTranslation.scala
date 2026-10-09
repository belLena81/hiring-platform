package com.example.hiring.analytics.errors

import cats.MonadError
import cats.syntax.monadError.*

import scala.util.control.NonFatal

/** The single place that turns unexpected adapter failures into [[AnalyticsError]] values.
  *
  * An [[AnalyticsError]] always passes through untouched; `special` cases are tried next; every other non-fatal
  * throwable is wrapped by `wrap` so its cause is retained.
  */
object AnalyticsErrorTranslation {
  extension [F[_], A](work: F[A])(using F: MonadError[F, Throwable]) {
    def translating(wrap: Throwable => AnalyticsError): F[A] =
      translatingWith(PartialFunction.empty)(wrap)

    def translatingWith(special: PartialFunction[Throwable, AnalyticsError])(wrap: Throwable => AnalyticsError): F[A] =
      work.adaptError {
        case error: AnalyticsError               => error
        case error if special.isDefinedAt(error) => special(error)
        case NonFatal(cause)                     => wrap(cause)
      }
  }
}
