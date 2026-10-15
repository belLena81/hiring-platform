package com.example.graphQL.cats.api.admission

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.config.RateLimitConfig
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.github.benmanes.caffeine.cache.Ticker
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** DHW-34: the allowance belongs to one authenticated actor, counts every action and resets with its window. */
final class InterviewActionRateLimiterSpec extends CatsEffectSuite {
  private def testTicker: IO[(Ticker, FiniteDuration => IO[Unit])] = IO.delay {
    val now = new AtomicLong(0L)
    val ticker = new Ticker {
      override def read(): Long = now.get()
    }
    (ticker, delta => IO.delay(now.addAndGet(delta.toNanos)).void)
  }

  private def actor(n: Long) = UserId(new UUID(0L, n))

  test("an actor gets exactly the configured number of actions per window, then a typed refusal") {
    val config = RateLimitConfig(windowSeconds = 60, attempts = 3, maxBuckets = 10)
    for {
      limiter <- InterviewActionRateLimiter.create(config)
      results <- List.fill(5)(limiter.permit(actor(1))).sequence
    } yield {
      assertEquals(results.take(3), List.fill(3)(Right(())))
      assertEquals(results.drop(3).map(_.left.map(_.retryAfterSeconds)), List.fill(2)(Left(60L)))
    }
  }

  test("one actor cannot consume another actor's allowance") {
    val config = RateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 10)
    for {
      limiter <- InterviewActionRateLimiter.create(config)
      first <- limiter.permit(actor(1))
      exhausted <- limiter.permit(actor(1))
      other <- limiter.permit(actor(2))
      third <- limiter.permit(actor(3))
    } yield {
      assertEquals(first, Right(()))
      assert(exhausted.isLeft)
      assertEquals(other, Right(()))
      assertEquals(third, Right(()))
    }
  }

  test("the allowance resets when its window ends and not before") {
    val config = RateLimitConfig(windowSeconds = 10, attempts = 1, maxBuckets = 10)
    for {
      (ticker, advance) <- testTicker
      limiter <- InterviewActionRateLimiter.create(config, ticker)
      first <- limiter.permit(actor(1))
      _ <- advance(9.seconds)
      early <- limiter.permit(actor(1))
      _ <- advance(1.second)
      afterWindow <- limiter.permit(actor(1))
    } yield {
      assertEquals(first, Right(()))
      assert(early.isLeft)
      assertEquals(afterWindow, Right(()))
    }
  }

  test("the refusal reports only a retry delay, never a limit or a count") {
    val config = RateLimitConfig(windowSeconds = 30, attempts = 1, maxBuckets = 10)
    for {
      limiter <- InterviewActionRateLimiter.create(config)
      _ <- limiter.permit(actor(1))
      refused <- limiter.permit(actor(1))
    } yield assertEquals(refused.left.map(_.retryAfterSeconds), Left(30L))
  }

  test("AC-11.1 a refusal reports the time left in the window, rounded up and never below one second") {
    val config = RateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 10)
    for {
      (ticker, advance) <- testTicker
      limiter <- InterviewActionRateLimiter.create(config, ticker)
      _ <- limiter.permit(actor(1))
      _ <- advance(44.seconds + 500.millis)
      midWindow <- limiter.permit(actor(1))
      _ <- advance(15.seconds + 500.millis - 1.nanos)
      lastNanosecond <- limiter.permit(actor(1))
    } yield {
      assertEquals(midWindow.left.map(_.retryAfterSeconds), Left(16L))
      assertEquals(lastNanosecond.left.map(_.retryAfterSeconds), Left(1L))
    }
  }

  test("a full cache charges unseen actors to shared overflow counters instead of refusing or evicting a counted one") {
    val config = RateLimitConfig(windowSeconds = 10, attempts = 1, maxBuckets = 3, overflowBuckets = 4)
    for {
      (ticker, advance) <- testTicker
      limiter <- InterviewActionRateLimiter.create(config, ticker)
      admitted <- List(1L, 2L, 3L).traverse(n => limiter.permit(actor(n)))
      newcomers <- (4L to 40L).toList.traverse(n => limiter.permit(actor(n)))
      // None of the counted actors was evicted, so each is still limited.
      counted <- List(1L, 2L, 3L).traverse(n => limiter.permit(actor(n)))
      _ <- advance(10.seconds)
      afterWindow <- limiter.permit(actor(4))
    } yield {
      assertEquals(admitted, List.fill(3)(Right(())))
      assert(newcomers.exists(_.isRight), "some newcomers are still admitted through the overflow counters")
      assert(newcomers.count(_.isRight) <= 4, "each overflow counter admits only its own allowance")
      assert(counted.forall(_.isLeft), clue(counted))
      assertEquals(afterWindow, Right(()))
    }
  }

  test("memory is bounded by the configured number of buckets") {
    val config = RateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 5)
    for {
      limiter <- InterviewActionRateLimiter.create(config)
      _ <- (1L to 50L).toList.traverse_(n => limiter.permit(actor(n)))
      size <- limiter.cleanUpAndSize
    } yield assert(size <= 5L, clue(size))
  }
}
