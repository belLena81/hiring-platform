package com.example.graphQL.cats.service

import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** ARR-07.1: concurrent readiness probes share one collection and a result is reused only for the cache window. */
final class ReadinessCacheSpec extends CatsEffectSuite {
  private def counted(runs: Ref[IO, Int]): IO[Int] = runs.updateAndGet(_ + 1) <* IO.sleep(100.millis)

  test("ARR-07.1 fifty concurrent probes share one underlying collection") {
    TestControl.executeEmbed(
      for {
        cache <- ReadinessCache.create[IO, Int](1.second)
        runs <- Ref.of[IO, Int](0)
        answers <- cache.get(counted(runs)).parReplicateA(50)
        total <- runs.get
      } yield {
        assertEquals(total, 1)
        assertEquals(answers.toSet, Set(1))
      }
    )
  }

  test("ARR-07.1 an answer is reused inside the window and collected again after it expires") {
    TestControl.executeEmbed(
      for {
        cache <- ReadinessCache.create[IO, Int](1.second)
        runs <- Ref.of[IO, Int](0)
        first <- cache.get(counted(runs))
        _ <- IO.sleep(500.millis)
        within <- cache.get(counted(runs))
        _ <- IO.sleep(1.second)
        after <- cache.get(counted(runs))
      } yield assertEquals((first, within, after), (1, 1, 2))
    )
  }

  test("ARR-07.1 a failed collection reaches the callers waiting on it and is not cached") {
    TestControl.executeEmbed(
      for {
        cache <- ReadinessCache.create[IO, Int](10.seconds)
        runs <- Ref.of[IO, Int](0)
        failing = runs.update(_ + 1) *> IO.sleep(10.millis) *> IO.raiseError[Int](new IllegalStateException("down"))
        failures <- cache.get(failing).attempt.parReplicateA(5)
        recovered <- cache.get(IO.pure(7))
        total <- runs.get
      } yield {
        assertEquals(total, 1)
        assert(failures.forall(_.isLeft))
        assertEquals(recovered, 7)
      }
    )
  }

  test("ARR-07.1 a cancelled collection does not strand the probes waiting on it") {
    TestControl.executeEmbed(
      for {
        cache <- ReadinessCache.create[IO, Int](10.seconds)
        leader <- cache.get(IO.never[Int]).start
        _ <- IO.sleep(10.millis)
        waiter <- cache.get(IO.pure(3)).start
        _ <- IO.sleep(10.millis)
        _ <- leader.cancel
        answer <- waiter.joinWithNever
      } yield assertEquals(answer, 3)
    )
  }

  test("ARR-07.1 readiness diagnostics for a lost database are emitted once per window, not once per probe") {
    TestControl.executeEmbed(
      for {
        emitted <- Ref.of[IO, Int](0)
        sink = new Diagnostics[IO] {
          def event(event: LogEvent, requestId: Option[String], fields: => Map[LogField, String]): IO[Unit] =
            emitted.update(_ + 1)
        }
        probe = new DatabaseProbe[IO] {
          def check(requestId: Option[String]): IO[ProbeResult] = IO.sleep(50.millis).as(ProbeResult.Unavailable)
        }
        cache <- ReadinessCache.create[IO, ReadinessReport](1.second)
        service = new HealthService[IO](probe, sink, None, Some(cache))
        _ <- service.report(None).parReplicateA(100)
        firstWindow <- emitted.get
        _ <- IO.sleep(2.seconds)
        _ <- service.report(None).parReplicateA(100)
        secondWindow <- emitted.get
      } yield assertEquals((firstWindow, secondWindow), (1, 2))
    )
  }
}
