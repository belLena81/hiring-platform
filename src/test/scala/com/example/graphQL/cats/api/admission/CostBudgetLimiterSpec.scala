package com.example.graphQL.cats.api.admission

import cats.effect.IO
import cats.syntax.all.*
import com.comcast.ip4s.IpAddress
import com.example.graphQL.cats.config.CostBudgetConfig
import com.example.graphQL.cats.domain.model.Identifiers.UserId
import com.github.benmanes.caffeine.cache.Ticker
import java.util.UUID
import munit.CatsEffectSuite

/** ARR-14.2: anonymous key flooding must degrade into a shared budget, never into a lockout of new callers. */
final class CostBudgetLimiterSpec extends CatsEffectSuite {
  private val config =
    CostBudgetConfig(windowSeconds = 60, maxCost = 10, maxActorBuckets = 5, maxClientBuckets = 5, overflowBuckets = 4)

  private def client(n: Int): CostBudgetLimiter.Key =
    CostBudgetLimiter.Key.Client(IpAddress.fromString(s"198.51.${n / 250}.${n % 250}"))

  private def actor(n: Int): CostBudgetLimiter.Key =
    CostBudgetLimiter.Key.Actor(UserId(new UUID(0L, n.toLong)))

  private def limiter: IO[CostBudgetLimiter] = CostBudgetLimiter.create(config, Ticker.systemTicker())

  test("ARR-14.2 filling the anonymous cache with attacker keys does not lock out an authenticated actor") {
    for {
      budget <- limiter
      _ <- (1 to 500).toList.traverse(n => IO(budget.acquireNow(client(n), 1)))
      admitted <- IO(budget.acquireNow(actor(1), 10))
    } yield assertEquals(admitted, Right(()))
  }

  test("ARR-14.2 filling the actor cache does not take capacity from anonymous clients") {
    for {
      budget <- limiter
      _ <- (1 to 500).toList.traverse(n => IO(budget.acquireNow(actor(n), 1)))
      admitted <- IO(budget.acquireNow(client(1), 10))
    } yield {
      assertEquals(admitted, Right(()))
      assertEquals(budget.clientLimiter.overflowCharges, 0L)
    }
  }

  test("ARR-14.2 an anonymous newcomer is admitted through overflow once the anonymous cache is full") {
    for {
      budget <- limiter
      _ <- (1 to 5).toList.traverse(n => IO(budget.acquireNow(client(n), 1)))
      newcomer <- IO(budget.acquireNow(client(1000), 1))
    } yield {
      assertEquals(newcomer, Right(()))
      assertEquals(budget.clientLimiter.overflowCharges, 1L)
    }
  }

  test("ARR-14.2 flooding does not reset an exhausted victim, anonymous or authenticated") {
    for {
      budget <- limiter
      _ <- IO(budget.acquireNow(client(1), 10))
      _ <- IO(budget.acquireNow(actor(1), 10))
      refusedBefore <- IO((budget.acquireNow(client(1), 1), budget.acquireNow(actor(1), 1)))
      _ <- (2 to 800).toList.traverse(n => IO(budget.acquireNow(client(n), 1)) *> IO(budget.acquireNow(actor(n), 1)))
      after <- IO((budget.acquireNow(client(1), 1), budget.acquireNow(actor(1), 1)))
    } yield {
      assert(refusedBefore._1.isLeft && refusedBefore._2.isLeft)
      assert(after._1.isLeft && after._2.isLeft, clue(after))
    }
  }

  test("ARR-14.2 the overflow charge uses the same cost as a tracked one") {
    for {
      budget <- limiter
      _ <- (1 to 5).toList.traverse(n => IO(budget.acquireNow(client(n), 1)))
      // A single overflow counter set of 4 stripes: a heavy newcomer exhausts its stripe with one query.
      heavy <- IO(budget.acquireNow(client(1000), 10))
      again <- IO(budget.acquireNow(client(1000), 1))
    } yield {
      assertEquals(heavy, Right(()))
      assert(again.isLeft)
    }
  }
}
