package com.example.graphQL.cats.api.http

import scala.concurrent.duration.*
import munit.FunSuite

final class FixedWindowRateLimiterSpec extends FunSuite {
  test("retry-after seconds round up and never return zero") {
    assertEquals(FixedWindowRateLimiter.RateLimited(Duration.Zero).retryAfterSeconds, 1L)
    assertEquals(FixedWindowRateLimiter.RateLimited(999.millis).retryAfterSeconds, 1L)
    assertEquals(FixedWindowRateLimiter.RateLimited(1.second).retryAfterSeconds, 1L)
    assertEquals(FixedWindowRateLimiter.RateLimited(1.second + 1.millis).retryAfterSeconds, 2L)
    assertEquals(FixedWindowRateLimiter.RateLimited(2500.millis).retryAfterSeconds, 3L)
  }
}
