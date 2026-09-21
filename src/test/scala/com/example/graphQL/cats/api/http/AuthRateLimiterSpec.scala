package com.example.graphQL.cats.api.http

import cats.effect.IO
import cats.syntax.all.*
import com.comcast.ip4s.IpAddress
import com.example.graphQL.cats.config.AuthRateLimitConfig
import scala.concurrent.duration.*
import munit.CatsEffectSuite

final class AuthRateLimiterSpec extends CatsEffectSuite {
  test("retry-after seconds round up and never return zero") {
    assertEquals(AuthRateLimiter.RateLimited(Duration.Zero).retryAfterSeconds, 1L)
    assertEquals(AuthRateLimiter.RateLimited(999.millis).retryAfterSeconds, 1L)
    assertEquals(AuthRateLimiter.RateLimited(1.second).retryAfterSeconds, 1L)
    assertEquals(AuthRateLimiter.RateLimited(1.second + 1.millis).retryAfterSeconds, 2L)
    assertEquals(AuthRateLimiter.RateLimited(2500.millis).retryAfterSeconds, 3L)
  }

  test("token buckets refill at the configured interval") {
    val config = AuthRateLimitConfig(windowSeconds = 1, attempts = 2, maxBuckets = 2)
    val initial = AuthRateLimiter.Bucket(config.attempts, 0L)
    val (afterFirst, firstResult) = initial.consume(0L, config)
    val (afterSecond, secondResult) = afterFirst.consume(0L, config)
    val (blocked, blockedResult) = afterSecond.consume(0L, config)
    val (_, refilledResult) = blocked.consume(1.second.toNanos, config)

    assertEquals(firstResult, Right(()))
    assertEquals(secondResult, Right(()))
    assertEquals(blockedResult, Left(AuthRateLimiter.RateLimited(1.second)))
    assertEquals(refilledResult, Right(()))
  }

  test("a full cache admits new keys instead of failing closed") {
    val config = AuthRateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 1)
    val first = AuthRateLimiter.Key(Some(IpAddress.fromString("203.0.113.1").get), AuthRateLimiter.Operation.Login)
    val second = AuthRateLimiter.Key(Some(IpAddress.fromString("203.0.113.2").get), AuthRateLimiter.Operation.Login)

    for {
      limiter <- AuthRateLimiter.create(config)
      firstResult <- limiter.permit(first)
      secondResult <- limiter.permit(second)
    } yield {
      assertEquals(firstResult, Right(()))
      assertEquals(secondResult, Right(()))
    }
  }

  test("concurrent permits for one key do not lose bucket updates") {
    val config = AuthRateLimitConfig(windowSeconds = 60, attempts = 40, maxBuckets = 10)
    val key = AuthRateLimiter.Key(None, AuthRateLimiter.Operation.Login)

    for {
      limiter <- AuthRateLimiter.create(config)
      results <- (1 to 80).toList.parTraverse(_ => limiter.permit(key))
    } yield assertEquals(results.count(_.isRight), config.attempts)
  }
}
