package com.example.graphQL.cats.api.graphql

import cats.effect.IO
import cats.syntax.all.*
import com.example.graphQL.cats.api.admission.{CostBudgetLimiter, FixedWindowRateLimiter}
import com.example.graphQL.cats.api.admission.FixedWindowRateLimiter.Grant
import com.example.graphQL.cats.service.{ActorContext, Diagnostics}
import com.example.graphQL.cats.domain.model.UserRole
import com.example.graphQL.cats.service.ServiceFixtures.candidateId
import munit.CatsEffectSuite

/** GDF-01.3: the cost receipt of one request adds up the query charge and every relaxation charge, whatever the grant.
  */
final class RequestContextCostReceiptSpec extends CatsEffectSuite {
  private def withGrants(grants: List[Grant])(use: RequestContext => IO[Unit]): IO[Unit] =
    IO.pure(new java.util.concurrent.atomic.AtomicReference(grants)).flatMap { remaining =>
      val next: (CostBudgetLimiter.Key, Int) => Either[FixedWindowRateLimiter.RateLimited, Grant] =
        (_, _) => Right(remaining.getAndUpdate(_.drop(1)).headOption.getOrElse(Grant.Shared))
      RequestContextFactory.resource
        .flatMap(
          _.resource(
            RequestContextParameters(
              Some(ActorContext(candidateId, UserRole.Candidate)),
              TestGraphQLSupport.emptyServices,
              IO.pure(com.example.graphQL.cats.service.ProbeResult.Ready),
              diagnostics = Diagnostics.noop,
              costBudget = next
            )
          )
        )
        .use(use)
    }

  private val tracked = Grant.Tracked(100, 70, 30)

  test("GDF-01.3 a shared grant on a relaxation keeps the disclosed receipt and still adds its charge") {
    withGrants(List(tracked, Grant.Shared)) { context =>
      IO {
        assert(context.chargeCostBudget(10).isRight)
        assert(context.chargeRelaxationCost(10).isRight)
        assertEquals(context.costReceipt.map(_.requested), Some(20))
        assertEquals(context.costReceipt.map(_.remaining), Some(70L))
      }
    }
  }

  test("GDF-01.3 a shared query charge followed by a tracked relaxation reports the whole requested cost") {
    withGrants(List(Grant.Shared, tracked)) { context =>
      IO {
        assert(context.chargeCostBudget(10).isRight)
        assertEquals(context.costReceipt, None)
        assert(context.chargeRelaxationCost(10).isRight)
        assertEquals(context.costReceipt.map(_.requested), Some(20))
        assertEquals(context.costReceipt.map(_.limit), Some(100))
        assertEquals(context.costReceipt.map(_.remaining), Some(70L))
      }
    }
  }

  test("GDF-01.3 a tracked relaxation after a tracked query charge replaces the numbers and totals the charges") {
    withGrants(List(tracked, Grant.Tracked(100, 55, 20))) { context =>
      IO {
        assert(context.chargeCostBudget(10).isRight)
        assert(context.chargeRelaxationCost(15).isRight)
        assertEquals(context.costReceipt, Some(CostReceipt(25, 100, 55L, 20L)))
      }
    }
  }

  test("GDF-01.3 two interleaved relaxation charges end on the latest grant with the whole requested total") {
    import java.util.concurrent.{CountDownLatch, TimeUnit}
    import java.util.concurrent.atomic.AtomicInteger
    val base = 10
    val calls = new AtomicInteger(0)
    val secondGranted = new CountDownLatch(1)
    // The base charge (call 1) is instant. Of the two relaxation charges, the first one to be granted (call 2) holds its
    // grant (remaining 90) until the second has been granted (remaining 80) and had time to record its receipt. Without
    // serialized charges the stale first grant is then written last. Under the lock the second charge cannot be granted
    // while the first is in progress, so the bounded wait times out instead of deadlocking.
    val next: (CostBudgetLimiter.Key, Int) => Either[FixedWindowRateLimiter.RateLimited, Grant] = (_, _) =>
      calls.incrementAndGet() match {
        case 1 => Right(Grant.Tracked(100, 95, 30))
        case 2 =>
          secondGranted.await(500, TimeUnit.MILLISECONDS)
          Thread.sleep(200)
          Right(Grant.Tracked(100, 90, 30))
        case _ =>
          secondGranted.countDown()
          Right(Grant.Tracked(100, 80, 30))
      }
    RequestContextFactory.resource
      .flatMap(
        _.resource(
          RequestContextParameters(
            Some(ActorContext(candidateId, UserRole.Candidate)),
            TestGraphQLSupport.emptyServices,
            IO.pure(com.example.graphQL.cats.service.ProbeResult.Ready),
            diagnostics = Diagnostics.noop,
            costBudget = next
          )
        )
      )
      .use { context =>
        for {
          _ <- IO(assert(context.chargeCostBudget(base).isRight))
          // The first relaxation is started alone so that it takes call 2; the second follows while it is held.
          first <- IO.blocking(context.chargeRelaxationCost(4)).start
          _ <- IO.sleep(scala.concurrent.duration.DurationInt(100).millis)
          second <- IO.blocking(context.chargeRelaxationCost(5)).start
          r1 <- first.joinWithNever
          r2 <- second.joinWithNever
        } yield {
          assert(r1.isRight && r2.isRight)
          assertEquals(context.costReceipt.map(_.requested), Some(base + 4 + 5))
          assertEquals(context.costReceipt.map(_.remaining), Some(80L))
        }
      }
  }

  test("GDF-01.3 parallel tracked relaxation charges total every charge and end on the latest grant") {
    val n = 50
    val base = 10
    // A limiter that drains one shared bucket, as the real one does: remaining only ever decreases.
    val bucket = new java.util.concurrent.atomic.AtomicLong(1000)
    val next: (CostBudgetLimiter.Key, Int) => Either[FixedWindowRateLimiter.RateLimited, Grant] =
      (_, cost) => Right(Grant.Tracked(1000, bucket.addAndGet(-cost.toLong), 30))
    RequestContextFactory.resource
      .flatMap(
        _.resource(
          RequestContextParameters(
            Some(ActorContext(candidateId, UserRole.Candidate)),
            TestGraphQLSupport.emptyServices,
            IO.pure(com.example.graphQL.cats.service.ProbeResult.Ready),
            diagnostics = Diagnostics.noop,
            costBudget = next
          )
        )
      )
      .use { context =>
        val costs = (1 to n).toList.map(i => 1 + i % 3)
        for {
          _ <- IO(assert(context.chargeCostBudget(base).isRight))
          _ <- costs.parTraverse_(c => IO.cede *> IO(assert(context.chargeRelaxationCost(c).isRight)))
        } yield {
          assertEquals(context.costReceipt.map(_.requested), Some(base + costs.sum))
          assertEquals(context.costReceipt.map(_.remaining), Some(bucket.get))
          assertEquals(bucket.get, 1000L - base - costs.sum)
        }
      }
  }
}
