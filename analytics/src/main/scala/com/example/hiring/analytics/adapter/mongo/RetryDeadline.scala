package com.example.hiring.analytics.adapter.mongo

import cats.Applicative
import cats.effect.Clock
import cats.syntax.functor.*
import retry.PolicyDecision.{DelayAndRetry, GiveUp}
import retry.RetryPolicy

import scala.concurrent.duration.*

/** Retry policy that is bounded by a monotonic-clock deadline rather than an attempt count. */
private[analytics] object RetryDeadline {

  /** Retries without delay while the monotonic clock is strictly before `deadline`; give up at or after it. Combine
    * with a backoff policy through `join` to add delays.
    */
  def until[F[_]: Applicative](clock: Clock[F], deadline: FiniteDuration): RetryPolicy[F, Any] =
    RetryPolicy.withShow[F, Any](
      (_, _) => clock.monotonic.map(now => if (now < deadline) DelayAndRetry(Duration.Zero) else GiveUp),
      s"until(monotonic=$deadline)"
    )
}
