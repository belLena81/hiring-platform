package com.example.graphQL.cats.shared

import cats.Applicative
import cats.effect.Clock
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

/** A clock whose wall time carries the precision MongoDB stores for an `Instant` (milliseconds), so a timestamp a
  * service returns on first execution equals the value read back on a replay. The monotonic reading is untouched.
  */
object MillisecondClock {
  def apply[F[_]](underlying: Clock[F]): Clock[F] = new Clock[F] {
    override def applicative: Applicative[F] = underlying.applicative
    override def monotonic: F[FiniteDuration] = underlying.monotonic
    override def realTime: F[FiniteDuration] =
      underlying.applicative.map(underlying.realTime)(time => FiniteDuration(time.toMillis, MILLISECONDS))
  }
}
