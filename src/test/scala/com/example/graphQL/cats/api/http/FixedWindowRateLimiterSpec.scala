package com.example.graphQL.cats.api.http

import scala.concurrent.duration.*
import cats.effect.{Clock, IO, Ref}
import java.time.Instant
import munit.CatsEffectSuite

final class FixedWindowRateLimiterSpec extends CatsEffectSuite {
  test("retry-after seconds round up and never return zero") {
    assertEquals(FixedWindowRateLimiter.RateLimited(Duration.Zero).retryAfterSeconds, 1L)
    assertEquals(FixedWindowRateLimiter.RateLimited(999.millis).retryAfterSeconds, 1L)
    assertEquals(FixedWindowRateLimiter.RateLimited(1.second).retryAfterSeconds, 1L)
    assertEquals(FixedWindowRateLimiter.RateLimited(1.second + 1.millis).retryAfterSeconds, 2L)
    assertEquals(FixedWindowRateLimiter.RateLimited(2500.millis).retryAfterSeconds, 3L)
  }

  test("prunes expired buckets before making an admission capacity decision") {
    val config = com.example.graphQL.cats.config.AuthRateLimitConfig(windowSeconds = 1, attempts = 1, maxBuckets = 1)
    val first = FixedWindowRateLimiter.Key("203.0.113.1", FixedWindowRateLimiter.Operation.Login)
    val second = FixedWindowRateLimiter.Key("203.0.113.2", FixedWindowRateLimiter.Operation.Login)

    for {
      now <- Ref.of[IO, Instant](Instant.parse("2026-09-19T00:00:00Z"))
      clock = new Clock[IO] {
        override val applicative: cats.Applicative[IO] = IO.asyncForIO
        override def realTime = now.get.map(instant => instant.toEpochMilli.millis)
        override def monotonic = IO.pure(Duration.Zero)
      }
      limiter <- FixedWindowRateLimiter.create(config, clock)
      results <- {
      for {
        firstResult <- limiter.permit(first)
        _ <- now.update(_.plusSeconds(1))
        secondResult <- limiter.permit(second)
        resetResult <- limiter.permit(first)
      } yield (firstResult, secondResult, resetResult)
      }
    } yield assertEquals(results, (Right(()), Right(()), Left(FixedWindowRateLimiter.RateLimited(1.second))))
  }
}
