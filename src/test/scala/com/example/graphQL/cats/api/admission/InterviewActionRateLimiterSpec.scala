package com.example.graphQL.cats.api.admission

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.config.InterviewActionRateLimitConfig
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
    val config = InterviewActionRateLimitConfig(windowSeconds = 60, attempts = 3, maxBuckets = 10)
    for {
      limiter <- InterviewActionRateLimiter.create(config)
      results <- List.fill(5)(limiter.permit(actor(1))).sequence
    } yield {
      assertEquals(results.take(3), List.fill(3)(Right(())))
      assertEquals(results.drop(3), List.fill(2)(Left(InterviewActionRateLimiter.RateLimited(60.seconds))))
    }
  }

  test("one actor cannot consume another actor's allowance") {
    val config = InterviewActionRateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 10)
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
    val config = InterviewActionRateLimitConfig(windowSeconds = 10, attempts = 1, maxBuckets = 10)
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
    val config = InterviewActionRateLimitConfig(windowSeconds = 30, attempts = 1, maxBuckets = 10)
    for {
      limiter <- InterviewActionRateLimiter.create(config)
      _ <- limiter.permit(actor(1))
      refused <- limiter.permit(actor(1))
    } yield assertEquals(refused.left.map(_.retryAfterSeconds), Left(30L))
  }

  test("a full cache refuses unseen actors instead of evicting a counted one, and admits them once windows expire") {
    val config = InterviewActionRateLimitConfig(windowSeconds = 10, attempts = 1, maxBuckets = 3)
    for {
      (ticker, advance) <- testTicker
      limiter <- InterviewActionRateLimiter.create(config, ticker)
      admitted <- List(1L, 2L, 3L).traverse(n => limiter.permit(actor(n)))
      unseen <- limiter.permit(actor(4))
      unseenAgain <- limiter.permit(actor(4))
      // None of the counted actors was evicted, so each is still limited.
      counted <- List(1L, 2L, 3L).traverse(n => limiter.permit(actor(n)))
      _ <- advance(10.seconds)
      afterWindow <- limiter.permit(actor(4))
    } yield {
      assertEquals(admitted, List.fill(3)(Right(())))
      assert(unseen.isLeft && unseenAgain.isLeft, "an unseen actor is refused while the cache is full")
      assert(counted.forall(_.isLeft), clue(counted))
      assertEquals(afterWindow, Right(()))
    }
  }

  test("memory is bounded by the configured number of buckets") {
    val config = InterviewActionRateLimitConfig(windowSeconds = 60, attempts = 1, maxBuckets = 5)
    for {
      limiter <- InterviewActionRateLimiter.create(config)
      _ <- (1L to 50L).toList.traverse_(n => limiter.permit(actor(n)))
      size <- limiter.cleanUpAndSize
    } yield assert(size <= 5L, clue(size))
  }
}
