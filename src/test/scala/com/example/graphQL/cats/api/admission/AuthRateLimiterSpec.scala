package com.example.graphQL.cats.api.admission

import cats.effect.{Clock, IO, Ref}
import cats.syntax.all.*
import com.comcast.ip4s.IpAddress
import com.example.graphQL.cats.config.AuthRateLimitConfig
import scala.concurrent.duration.*
import munit.CatsEffectSuite

final class AuthRateLimiterSpec extends CatsEffectSuite {
  private def testClock: IO[(Clock[IO], FiniteDuration => IO[Unit])] =
    Ref.of[IO, FiniteDuration](Duration.Zero).map { now =>
      val clock = new Clock[IO] {
        override val applicative: cats.Applicative[IO] = IO.asyncForIO
        override def realTime: IO[FiniteDuration] = now.get
        override def monotonic: IO[FiniteDuration] = now.get
      }
      (clock, delta => now.update(_ + delta))
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
      (clock, advance) <- testClock
      limiter <- AuthRateLimiter.create(config, clock)
      first <- limiter.permit(key("203.0.113.1"))
      blocked <- limiter.permit(key("203.0.113.1"))
      _ <- advance(1.second)
      afterExpiry <- limiter.permit(key("203.0.113.1"))
    } yield {
      assertEquals(first, Right(())); assertEquals(blocked, Left(AuthRateLimiter.RateLimited(1.second)));
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
  test("frequent buckets resist one-hit IPv6 flooding") {
    val config = AuthRateLimitConfig(windowSeconds = 60, attempts = 2, maxBuckets = 256)
    val hot = key("2001:db8:100::1");
    val flood = (1 to 200).toList.map(index => key(s"2001:db8:${index.toHexString}::1"))
    for {
      limiter <- AuthRateLimiter.create(config); _ <- (1 to 200).toList.traverse_(_ => limiter.permit(hot));
      _ <- flood.traverse_(limiter.permit); stillLimited <- limiter.permit(hot)
    } yield assertEquals(stillLimited, Left(AuthRateLimiter.RateLimited(60.seconds)))
  }
  test("concurrent permits for one key do not lose atomic updates") {
    val config = AuthRateLimitConfig(windowSeconds = 60, attempts = 40, maxBuckets = 10);
    val shared = AuthRateLimiter.Key(None, AuthRateLimiter.Operation.Login)
    for {
      limiter <- AuthRateLimiter.create(config); results <- (1 to 80).toList.parTraverse(_ => limiter.permit(shared))
    } yield assertEquals(results.count(_.isRight), config.attempts)
  }
}
