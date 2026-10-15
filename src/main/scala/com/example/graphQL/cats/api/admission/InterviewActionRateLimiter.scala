package com.example.graphQL.cats.api.admission

import cats.effect.IO
import com.example.graphQL.cats.config.RateLimitConfig
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.github.benmanes.caffeine.cache.Ticker

/** Interview cancel and reschedule actions per authenticated actor. Every action counts once, whichever mutation it is
  * and however many share a request, and each actor has an isolated allowance. It runs before any storage work. When
  * `maxBuckets` actors are tracked, unseen actors share overflow counters instead of being refused or evicting a
  * counted actor.
  */
object InterviewActionRateLimiter {
  type RateLimited = FixedWindowRateLimiter.RateLimited
  val RateLimited: FixedWindowRateLimiter.RateLimited.type = FixedWindowRateLimiter.RateLimited
  type Limiter = FixedWindowRateLimiter[UserId]

  def create(config: RateLimitConfig): IO[Limiter] =
    create(config, Ticker.systemTicker())

  private[admission] def create(config: RateLimitConfig, ticker: Ticker): IO[Limiter] =
    FixedWindowRateLimiter.create[UserId](
      config.windowSeconds,
      config.attempts,
      config.maxBuckets,
      ticker,
      identity,
      config.overflowBuckets,
      "interview-action"
    )
}
