package com.example.graphQL.cats.api.admission

import cats.effect.IO
import com.example.graphQL.cats.config.InterviewActionRateLimitConfig
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.github.benmanes.caffeine.cache.Ticker

/** Interview cancel and reschedule actions per authenticated actor. Every action counts once, whichever mutation it is
  * and however many share a request, and each actor has an isolated allowance. It runs before any storage work.
  */
object InterviewActionRateLimiter {
  type RateLimited = FixedWindowRateLimiter.RateLimited
  val RateLimited: FixedWindowRateLimiter.RateLimited.type = FixedWindowRateLimiter.RateLimited
  type Limiter = FixedWindowRateLimiter[UserId]

  def create(config: InterviewActionRateLimitConfig): IO[Limiter] =
    create(config, Ticker.systemTicker())

  private[admission] def create(config: InterviewActionRateLimitConfig, ticker: Ticker): IO[Limiter] =
    FixedWindowRateLimiter.create[UserId](
      config.windowSeconds,
      config.attempts,
      config.maxBuckets,
      ticker,
      identity,
      failClosedWhenFull = true
    )
}
