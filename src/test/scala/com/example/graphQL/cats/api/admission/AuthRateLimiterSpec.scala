package com.example.graphQL.cats.api.admission

import cats.effect.IO
import cats.syntax.all.*
import com.comcast.ip4s.IpAddress
import com.example.graphQL.cats.config.AuthRateLimitConfig
import com.github.benmanes.caffeine.cache.Ticker
import scala.concurrent.duration.*
import java.util.concurrent.atomic.AtomicLong
import munit.CatsEffectSuite

final class AuthRateLimiterSpec extends CatsEffectSuite {
  private def testTicker: IO[(Ticker, FiniteDuration => IO[Unit])] = IO.delay {
    val now = new AtomicLong(0L)
    val ticker = new Ticker {
      override def read(): Long = now.get()
    }
    (ticker, delta => IO.delay(now.addAndGet(delta.toNanos)).void)
  }

  private def key(
      address: String,
      operation: AuthRateLimiter.Operation = AuthRateLimiter.Operation.Login
  ): AuthRateLimiter.Key =
    AuthRateLimiter.Key(Some(IpAddress.fromString(address).get), operation)
  test("retry-after seconds round up and never return zero") {
    assertEquals(AuthRateLimiter.RateLimited(Duration.Zero).retryAfterSeconds, 1L)
    assertEquals(AuthRateLimiter.RateLimited(999.millis).retryAfterSeconds, 1L)
    assertEquals(AuthRateLimiter.RateLimited(1.second).retryAfterSeconds, 1L)
    assertEquals(AuthRateLimiter.RateLimited(1.second + 1.millis).retryAfterSeconds, 2L)
    assertEquals(AuthRateLimiter.RateLimited(2500.millis).retryAfterSeconds, 3L)
  }
  test("permits configured attempts and then returns a conservative retry window") {
    val config = AuthRateLimitConfig(windowSeconds = 60, attempts = 2, maxBuckets = 2)
    for {
      limiter <- AuthRateLimiter.create(config);
      results <- (1 to 3).toList.traverse(_ => limiter.permit(key("203.0.113.1")))
    } yield {
      assertEquals(results.take(2), List(Right(()), Right(())))
      assertEquals(results(2), Left(AuthRateLimiter.RateLimited(60.seconds)))
    }
  }
  test("IPv6 addresses in the same /64 share a bucket") {
    val config = AuthRateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 10)
    for {
      limiter <- AuthRateLimiter.create(config); first <- limiter.permit(key("2001:db8:1:2::1"));
      sameNetwork <- limiter.permit(key("2001:db8:1:2::2")); otherNetwork <- limiter.permit(key("2001:db8:1:3::1"))
    } yield {
      assertEquals(first, Right(())); assertEquals(sameNetwork, Left(AuthRateLimiter.RateLimited(60.seconds)));
      assertEquals(otherNetwork, Right(()))
    }
  }
  test("expired windows admit a new attempt") {
    val config = AuthRateLimitConfig(windowSeconds = 1, attempts = 1, maxBuckets = 1)
    for {
      (ticker, advance) <- testTicker
      limiter <- AuthRateLimiter.create(config, ticker)
      first <- limiter.permit(key("203.0.113.1"))
      blocked <- limiter.permit(key("203.0.113.1"))
      _ <- advance(900.millis)
      stillBlocked <- limiter.permit(key("203.0.113.1"))
      _ <- advance(100.millis)
      afterExpiry <- limiter.permit(key("203.0.113.1"))
    } yield {
      assertEquals(first, Right(())); assertEquals(blocked, Left(AuthRateLimiter.RateLimited(1.second)))
      assertEquals(stillBlocked, Left(AuthRateLimiter.RateLimited(1.second)))
      assertEquals(afterExpiry, Right(()))
    }
  }
  test("frequently used buckets survive bounded eviction") {
    val config = AuthRateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 2)
    val hot = key("203.0.113.1")
    val cold = key("203.0.113.2")
    val replacement = key("203.0.113.3")
    for {
      limiter <- AuthRateLimiter.create(config)
      _ <- limiter.permit(hot)
      _ <- limiter.permit(cold)
      limitedHot <- limiter.permit(hot)
      _ <- limiter.permit(replacement)
      stillLimitedHot <- limiter.permit(hot)
    } yield {
      assertEquals(limitedHot, Left(AuthRateLimiter.RateLimited(60.seconds)))
      assertEquals(stillLimitedHot, Left(AuthRateLimiter.RateLimited(60.seconds)))
    }
  }
  test("unique IPv6 flooding stays within the bucket bound") {
    val config = AuthRateLimitConfig(windowSeconds = 60, attempts = 2, maxBuckets = 256)
    val hot = key("2001:db8:ffff::1");
    val flood = (1 to 1000).toList.map(index => key(s"2001:db8:${index.toHexString}::1"))
    for {
      limiter <- AuthRateLimiter.create(config)
      _ <- (1 to 200).toList.traverse_(_ => limiter.permit(hot))
      limitedHot <- limiter.permit(hot)
      _ <- flood.traverse_(limiter.permit)
      size <- limiter.cleanUpAndSize
    } yield {
      assertEquals(limitedHot, Left(AuthRateLimiter.RateLimited(60.seconds)))
      assert(size <= config.maxBuckets.toLong)
    }
  }
  test("cache size remains bounded while admitting unique keys") {
    val config = AuthRateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 8)
    for {
      limiter <- AuthRateLimiter.create(config)
      _ <- (1 to 100).toList.traverse_(index => limiter.permit(key(s"203.0.113.$index")))
      size <- limiter.cleanUpAndSize
    } yield assert(size <= config.maxBuckets.toLong)
  }
  test("concurrent permits for one key do not lose atomic updates") {
    val config = AuthRateLimitConfig(windowSeconds = 60, attempts = 40, maxBuckets = 10);
    val shared = AuthRateLimiter.Key(None, AuthRateLimiter.Operation.Login)
    for {
      limiter <- AuthRateLimiter.create(config); results <- (1 to 80).toList.parTraverse(_ => limiter.permit(shared))
    } yield assertEquals(results.count(_.isRight), config.attempts)
  }
}
