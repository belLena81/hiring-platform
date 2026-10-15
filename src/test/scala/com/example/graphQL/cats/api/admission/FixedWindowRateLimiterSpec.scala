package com.example.graphQL.cats.api.admission

import cats.effect.IO
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.github.benmanes.caffeine.cache.Ticker
import java.util.concurrent.atomic.AtomicLong
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** ARR-14.1 and ARR-14.2: a weighted acquire charges a cost against a window and never lets other keys reset it. */
final class FixedWindowRateLimiterSpec extends CatsEffectSuite {
  private def testTicker: IO[(Ticker, FiniteDuration => IO[Unit])] = IO.delay {
    val now = new AtomicLong(0L)
    val ticker = new Ticker { override def read(): Long = now.get() }
    (ticker, delta => IO.delay(now.addAndGet(delta.toNanos)).void)
  }

  private def limiter(
      budget: Int,
      maxBuckets: Int = 10,
      ticker: Ticker = Ticker.systemTicker(),
      overflowBuckets: Int = 4
  ) =
    FixedWindowRateLimiter.create[String](60, budget, maxBuckets, ticker, identity, overflowBuckets, "test")

  test("ARR-14.1 weighted charges pass until the budget is spent and are then refused") {
    for {
      budget <- limiter(100)
      first <- budget.acquire("a", 60)
      second <- budget.acquire("a", 40)
      third <- budget.acquire("a", 1)
    } yield {
      assertEquals(first, Right(()))
      assertEquals(second, Right(()))
      assertEquals(third.left.map(_.retryAfterSeconds), Left(60L))
    }
  }

  test("ARR-14.1 a single charge above the whole budget is refused") {
    limiter(100).flatMap(_.acquire("a", 101)).map(result => assert(result.isLeft))
  }

  test("ARR-14.1 a charge below one still costs one and plain permit still counts one call") {
    for {
      budget <- limiter(2)
      results <- List(0, -5).traverse(budget.acquire("a", _))
      refused <- budget.permit("a")
    } yield {
      assertEquals(results, List(Right(()), Right(())))
      assert(refused.isLeft)
    }
  }

  test("ARR-14.1 the budget resets when its window ends and not before") {
    for {
      (ticker, advance) <- testTicker
      budget <- limiter(10, ticker = ticker)
      _ <- budget.acquire("a", 10)
      early <- (advance(59.seconds) *> budget.acquire("a", 1))
      later <- (advance(1.second) *> budget.acquire("a", 10))
    } yield {
      assert(early.isLeft)
      assertEquals(later, Right(()))
    }
  }

  test("ARR-14.1 charges that overflow a counter stay refused") {
    for {
      budget <- limiter(10)
      _ <- budget.acquire("a", Int.MaxValue)
      _ <- budget.acquire("a", Int.MaxValue)
      refused <- budget.acquire("a", 1)
    } yield assert(refused.isLeft)
  }

  test("ARR-14.2 keys have separate budgets") {
    for {
      budget <- limiter(5)
      _ <- budget.acquire("a", 5)
      exhausted <- budget.acquire("a", 1)
      other <- budget.acquire("b", 5)
    } yield {
      assert(exhausted.isLeft)
      assertEquals(other, Right(()))
    }
  }

  test("ARR-14.2 flooding distinct keys past the bucket limit does not reset an exhausted key") {
    for {
      budget <- limiter(5, maxBuckets = 3)
      _ <- budget.acquire("victim", 5)
      _ <- budget.acquire("victim", 1)
      _ <- (1 to 200).toList.traverse(index => budget.acquire(s"flood-$index", 1))
      victim <- budget.acquire("victim", 1)
      size <- budget.cleanUpAndSize
    } yield {
      assert(victim.isLeft)
      assert(size <= 3L, s"cache grew to $size")
    }
  }

  test("ARR-14.2 unseen keys are admitted through overflow counters while the cache is full") {
    for {
      budget <- limiter(5, maxBuckets = 2)
      _ <- List("a", "b").traverse(budget.acquire(_, 1))
      newcomer <- budget.acquire("newcomer", 1)
    } yield {
      assertEquals(newcomer, Right(()))
      assertEquals(budget.overflowCharges, 1L)
    }
  }

  test("ARR-14.2 overflow counters keep window semantics: shared budget, cost charged, reset when the window ends") {
    for {
      (ticker, advance) <- testTicker
      budget <- limiter(5, maxBuckets = 1, ticker = ticker, overflowBuckets = 1)
      _ <- budget.acquire("tracked", 1)
      first <- budget.acquire("x", 3)
      second <- budget.acquire("y", 2)
      shared <- budget.acquire("z", 1)
      _ <- advance(60.seconds)
      afterWindow <- budget.acquire("z", 5)
    } yield {
      assertEquals((first, second), (Right(()), Right(())))
      assert(shared.isLeft, "the single overflow counter is shared by every unseen key")
      assertEquals(afterWindow, Right(()))
    }
  }

  test("ARR-14.2 an expired tracked key is replaced in place and capacity frees up after the window") {
    for {
      (ticker, advance) <- testTicker
      budget <- limiter(1, maxBuckets = 1, ticker = ticker)
      _ <- budget.acquire("a", 1)
      _ <- advance(61.seconds)
      again <- budget.acquire("a", 1)
      other <- budget.acquire("b", 1)
    } yield {
      assertEquals(again, Right(()))
      assertEquals(other, Right(()))
    }
  }

  test("ARR-14.2 concurrent charges against one key admit exactly the budget") {
    for {
      budget <- limiter(100, maxBuckets = 2)
      results <- (1 to 500).toList.parTraverse(_ => budget.acquire("hot", 1))
    } yield {
      assertEquals(results.count(_.isRight), 100)
      assertEquals(results.count(_.isLeft), 400)
    }
  }

  test("ARR-14.2 concurrent charges against one overflow counter admit exactly the budget") {
    for {
      budget <- limiter(100, maxBuckets = 1, overflowBuckets = 1)
      _ <- budget.acquire("tracked", 1)
      results <- (1 to 500).toList.parTraverse(index => budget.acquire(s"key-$index", 1))
    } yield assertEquals(results.count(_.isRight), 100)
  }

  test("ARR-14.2 when every overflow counter is exhausted an unseen key is refused until the window ends") {
    for {
      (ticker, advance) <- testTicker
      budget <- limiter(1, maxBuckets = 1, ticker = ticker, overflowBuckets = 2)
      _ <- budget.acquire("tracked", 1)
      _ <- (1 to 200).toList.traverse(index => budget.acquire(s"flood-$index", 1))
      refused <- budget.acquire("late-comer", 1)
      tracked <- budget.acquire("tracked", 1)
      _ <- advance(60.seconds)
      afterWindow <- budget.acquire("late-comer", 1)
    } yield {
      assert(refused.isLeft, "the shared allowance is spent: documents the limit of the overflow design")
      assert(tracked.isLeft)
      assertEquals(afterWindow, Right(()))
    }
  }

  test("ARR-14.2 concurrent first-time inserts overshoot the bucket limit by at most the racing threads") {
    val racers = 64
    for {
      budget <- limiter(5, maxBuckets = 8)
      _ <- (1 to 4000).toList.parTraverseN(racers)(index => budget.acquire(s"key-$index", 1))
      size <- budget.cleanUpAndSize
    } yield assert(size <= 8L + racers, s"cache grew to $size")
  }

  test("ARR-14.2 stripe selection is seeded per instance and balanced") {
    val keys = (1 to 6400).map(index => s"198.51.${index / 250}.${index % 250}")
    for {
      first <- limiter(5, overflowBuckets = 64)
      second <- limiter(5, overflowBuckets = 64)
    } yield {
      val firstStripes = keys.map(first.stripeOf)
      val secondStripes = keys.map(second.stripeOf)
      val agreeing = firstStripes.zip(secondStripes).count { case (a, b) => a == b }
      assert(agreeing < keys.size / 4, s"two instances agree on $agreeing of ${keys.size} keys")
      val counts = firstStripes.groupBy(identity).view.mapValues(_.size).toMap
      assertEquals(counts.size, 64)
      assert(counts.values.forall(count => count > 50 && count < 170), counts.values.toList.sorted.toString)
    }
  }
}
