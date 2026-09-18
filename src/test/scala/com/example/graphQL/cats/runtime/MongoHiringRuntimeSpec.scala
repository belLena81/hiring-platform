package com.example.graphQL.cats.runtime

import cats.effect.{Deferred, IO, Ref}
import com.example.graphQL.cats.service.{DatabaseProbe, Diagnostics, HealthService, ProbeResult}
import munit.CatsEffectSuite

class MongoHiringRuntimeSpec extends CatsEffectSuite {
  test("readiness observes setup without cancelling its resource-owned operation") {
    for {
      entered <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      setupCalls <- Ref.of[IO, Int](0)
      setup = setupCalls.update(_ + 1) *> entered.complete(()).void *> release.get
      result <- SetupLifecycle.resource(setup).use { lifecycle =>
        val probe = new DatabaseProbe {
          override def check: IO[ProbeResult] = lifecycle.ready.map {
            if (_) ProbeResult.Ready else ProbeResult.Unavailable
          }
        }
        for {
          _ <- entered.get
          before <- new HealthService(probe, Diagnostics.noop).readiness(None)
          _ <- release.complete(()).void
          _ <- lifecycle.await
          after <- new HealthService(probe, Diagnostics.noop).readiness(None)
          calls <- setupCalls.get
        } yield (before, after, calls)
      }
    } yield {
      assertEquals(result, (ProbeResult.Unavailable, ProbeResult.Ready, 1))
    }
  }

  test("setup failure is recorded once and does not trigger readiness retries") {
    for {
      setupCalls <- Ref.of[IO, Int](0)
      setup = setupCalls.update(_ + 1) *> IO.raiseError[Unit](new RuntimeException("synthetic setup failure"))
      result <- SetupLifecycle.resource(setup).use { lifecycle =>
        for {
          first <- lifecycle.await
          second <- lifecycle.await
          ready <- lifecycle.ready
          calls <- setupCalls.get
        } yield (first, second, ready, calls)
      }
    } yield {
      assertEquals(result, (false, false, false, 1))
    }
  }
}
