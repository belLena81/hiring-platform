package com.example.graphQL.cats.application

import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import munit.CatsEffectSuite
import scala.concurrent.duration.*

class HealthServiceSpec extends CatsEffectSuite {
  private val requestId = Some("fe211944-7015-4e73-8dc1-000000000001")

  private def diagnostics(events: Ref[IO, Vector[(LogEvent, Option[String])]]): Diagnostics =
    new Diagnostics {
      def event(event: LogEvent, requestId: Option[String]): IO[Unit] =
        events.update(_ :+ (event -> requestId))
    }

  private def probe(result: IO[ProbeResult]): DatabaseProbe = new DatabaseProbe {
    def check: IO[ProbeResult] = result
  }

  test("P1-AC04 successful readiness produces no failure diagnostic") {
    for {
      events <- Ref.of[IO, Vector[(LogEvent, Option[String])]](Vector.empty)
      result <- new HealthService(probe(IO.pure(ProbeResult.Ready)), diagnostics(events)).readiness(requestId)
      recorded <- events.get
    } yield {
      assertEquals(result, ProbeResult.Ready)
      assertEquals(recorded, Vector.empty)
    }
  }

  test("P1-AC04 unavailable, authentication and exception failures emit typed correlated events") {
    List(
      (IO.pure(ProbeResult.Unavailable), ProbeResult.Unavailable, LogEvent.MongoUnavailable),
      (IO.pure(ProbeResult.AuthenticationFailed), ProbeResult.AuthenticationFailed, LogEvent.MongoAuthFailed),
      (IO.raiseError[ProbeResult](new RuntimeException("mongodb://user:synthetic-secret@host")),
        ProbeResult.Unavailable, LogEvent.MongoUnavailable)
    ).traverse_ { case (effect, expected, expectedEvent) =>
      for {
        events <- Ref.of[IO, Vector[(LogEvent, Option[String])]](Vector.empty)
        result <- new HealthService(probe(effect), diagnostics(events)).readiness(requestId)
        recorded <- events.get
      } yield {
        assertEquals(result, expected)
        assertEquals(recorded, Vector(expectedEvent -> requestId))
        assert(!recorded.toString.contains("synthetic-secret"))
      }
    }
  }

  test("P1-AC04 readiness recovers on the next check without caching failure") {
    for {
      calls <- Ref.of[IO, Int](0)
      events <- Ref.of[IO, Vector[(LogEvent, Option[String])]](Vector.empty)
      service = new HealthService(probe(calls.getAndUpdate(_ + 1).map {
        case 0 => ProbeResult.Unavailable
        case _ => ProbeResult.Ready
      }), diagnostics(events))
      first <- service.readiness(requestId)
      second <- service.readiness(requestId)
      count <- calls.get
      recorded <- events.get
    } yield {
      assertEquals(first, ProbeResult.Unavailable)
      assertEquals(second, ProbeResult.Ready)
      assertEquals(count, 2)
      assertEquals(recorded, Vector(LogEvent.MongoUnavailable -> requestId))
    }
  }

  test("P1-AC04 two-second deadline cancels the probe before reporting unavailable; four-second harness tolerance") {
    (for {
      entered <- Deferred[IO, Unit]
      canceled <- Deferred[IO, Unit]
      events <- Ref.of[IO, Vector[(LogEvent, Option[String])]](Vector.empty)
      service = new HealthService(probe(
        (entered.complete(()).void *> IO.never[ProbeResult]).onCancel(canceled.complete(()).void)
      ), diagnostics(events))
      started <- IO.monotonic
      result <- service.readiness(requestId).background.use { completion =>
        entered.get *> completion.flatMap(_.embedNever)
      }
      elapsed <- IO.monotonic.map(_ - started)
      finalized <- canceled.tryGet
      recorded <- events.get
    } yield {
      assertEquals(result, ProbeResult.Unavailable)
      assertEquals(finalized, Some(()))
      assert(elapsed >= 2.seconds, clues(elapsed))
      assert(elapsed < 6.seconds, clues(elapsed))
      assertEquals(recorded, Vector(LogEvent.MongoUnavailable -> requestId))
    }).timeout(6.seconds)
  }

  test("P1-AC06 caller cancellation finalizes the fake probe and remains cancellation") {
    (for {
      entered <- Deferred[IO, Unit]
      canceled <- Deferred[IO, Unit]
      events <- Ref.of[IO, Vector[(LogEvent, Option[String])]](Vector.empty)
      service = new HealthService(probe(
        (entered.complete(()).void *> IO.never[ProbeResult]).onCancel(canceled.complete(()).void)
      ), diagnostics(events))
      _ <- service.readiness(requestId).start.bracket { fiber =>
        for {
          _ <- entered.get
          _ <- fiber.cancel
          outcome <- fiber.join
          finalized <- canceled.tryGet
          recorded <- events.get
        } yield {
          assert(outcome.isCanceled)
          assertEquals(finalized, Some(()))
          assertEquals(recorded, Vector.empty)
        }
      }(_.cancel)
    } yield ()).timeout(6.seconds)
  }
}
