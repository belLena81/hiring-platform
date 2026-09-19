package com.example.graphQL.cats.api.http

import scala.concurrent.duration.*
import cats.effect.IO
import munit.CatsEffectSuite

final class FixedWindowRateLimiterSpec extends CatsEffectSuite {
  test("retry-after seconds round up and never return zero") {
    assertEquals(FixedWindowRateLimiter.RateLimited(Duration.Zero).retryAfterSeconds, 1L)
    assertEquals(FixedWindowRateLimiter.RateLimited(999.millis).retryAfterSeconds, 1L)
    assertEquals(FixedWindowRateLimiter.RateLimited(1.second).retryAfterSeconds, 1L)
    assertEquals(FixedWindowRateLimiter.RateLimited(1.second + 1.millis).retryAfterSeconds, 2L)
    assertEquals(FixedWindowRateLimiter.RateLimited(2500.millis).retryAfterSeconds, 3L)
  }

  test("evicts expired buckets in the managed resource") {
    val config = com.example.graphQL.cats.config.AuthRateLimitConfig(windowSeconds = 1, attempts = 1, maxBuckets = 1)
    val first = FixedWindowRateLimiter.Key("203.0.113.1", FixedWindowRateLimiter.Operation.Login)
    val second = FixedWindowRateLimiter.Key("203.0.113.2", FixedWindowRateLimiter.Operation.Login)

    FixedWindowRateLimiter.resource(config, 20.millis).use { limiter =>
      for {
        firstResult <- limiter.permit(first)
        _ <- IO.sleep(1100.millis)
        secondResult <- limiter.permit(second)
      } yield {
        assertEquals(firstResult, Right(()))
        assertEquals(secondResult, Right(()))
      }
    }
  }
}
