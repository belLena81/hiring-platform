package com.example.graphQL.cats.service

import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import munit.CatsEffectSuite
import scala.concurrent.duration.*

class HealthServiceSpec extends CatsEffectSuite {
  private val requestId = Some("fe211944-7015-4e73-8dc1-000000000001")

  private def diagnostics(events: Ref[IO, Vector[(LogEvent, Option[String])]]): Diagnostics =
    new Diagnostics {
      def event(event: LogEvent, requestId: Option[String], fields: Map[LogField, String]): IO[Unit] =
        events.update(_ :+ (event -> requestId))
    }

  private def probe(result: IO[ProbeResult]): DatabaseProbe = new DatabaseProbe {
    def check: IO[ProbeResult] = result
  }

  test("readiness forwards correlation and records safe failure classification and elapsed time") {
    for {
      forwarded <- Ref.of[IO, Option[String]](None)
      recorded <- Ref.of[IO, Map[LogField, String]](Map.empty)
      contextual = new DatabaseProbe {
        def check: IO[ProbeResult] = IO.raiseError(new AssertionError("Context overload required"))
        override def check(id: Option[String]): IO[ProbeResult] =
          forwarded.set(id) *> IO.raiseError(new IllegalStateException("synthetic-service-secret"))
      }
      sink = new Diagnostics {
        def event(event: LogEvent, id: Option[String], fields: Map[LogField, String]): IO[Unit] = recorded.set(fields)
      }
      result <- new HealthService(contextual, sink).readiness(requestId)
      id <- forwarded.get
      fields <- recorded.get
    } yield {
      assertEquals(result, ProbeResult.Unavailable)
      assertEquals(id, requestId)
      assertEquals(fields.get(LogField.Reason), Some("DATABASE_ERROR"))
      assertEquals(fields.get(LogField.ErrorType), Some("java.lang.IllegalStateException"))
      assert(fields.get(LogField.DurationMs).flatMap(_.toLongOption).exists(_ >= 0))
      assert(fields.forall { case (field, value) => LogFields.validPublic(field, value) })
      assert(!fields.toString.contains("synthetic-service-secret"))
    }
  }

  test("synchronous and effectful diagnostic failures cannot change probe outcomes") {
    List(true, false).traverse_ { synchronous =>
      val sink = new Diagnostics {
        def event(event: LogEvent, id: Option[String], fields: Map[LogField, String]): IO[Unit] =
          if (synchronous) throw new IllegalStateException("synthetic-sink-secret")
          else IO.raiseError(new IllegalStateException("synthetic-sink-secret"))
      }
      List(ProbeResult.Ready, ProbeResult.Unavailable, ProbeResult.AuthenticationFailed).traverse_ { expected =>
        new HealthService(probe(IO.pure(expected)), sink).readiness(requestId)
          .map(result => assertEquals(result, expected))
      }
    }
  }

  test("deadline diagnostics distinguish the service timeout after probe finalization") {
    for {
      finalized <- Ref.of[IO, Boolean](false)
      captured <- Ref.of[IO, Map[LogField, String]](Map.empty)
      sink = new Diagnostics {
        def event(event: LogEvent, id: Option[String], fields: Map[LogField, String]): IO[Unit] =
          finalized.get.flatMap(done => IO(assert(done))) *> captured.set(fields)
      }
      result <- new HealthService(probe(IO.never[ProbeResult].onCancel(finalized.set(true))), sink).readiness(requestId)
      fields <- captured.get
    } yield {
      assertEquals(result, ProbeResult.Unavailable)
      assertEquals(fields.get(LogField.Reason), Some("PROBE_TIMEOUT"))
      assertEquals(fields.get(LogField.ErrorType), Some("java.util.concurrent.TimeoutException"))
      assert(fields.get(LogField.ErrorLocation).exists(_.matches("HealthService\\.scala:[1-9][0-9]*")))
      assert(fields.get(LogField.DurationMs).flatMap(_.toLongOption).exists(_ >= 2000))
    }
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
