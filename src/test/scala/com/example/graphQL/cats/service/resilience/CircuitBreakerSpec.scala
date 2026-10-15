package com.example.graphQL.cats.service.resilience

import cats.effect.{Deferred, IO, Ref}
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import com.example.graphQL.cats.domain.policy.CircuitBreakerPolicy.{Settings, State, Transition}
import munit.CatsEffectSuite
import scala.concurrent.duration.*

final class CircuitBreakerSpec extends CatsEffectSuite {
  private val settings = Settings(3, 10.seconds, 2.seconds, 8.seconds, 1)
  private val unavailable = new RuntimeException("dependency down")
  private val countAll = CircuitBreaker.classifier[IO, Unit](_ => true)

  private def breakerWithLog: IO[(CircuitBreaker[IO], Ref[IO, List[Transition]])] =
    Ref.of[IO, List[Transition]](Nil).flatMap { log =>
      CircuitBreaker.create[IO](settings, t => log.update(_ :+ t)).tupleRight(log)
    }

  private def failOnce(breaker: CircuitBreaker[IO], calls: Ref[IO, Int]) =
    breaker.protect(calls.update(_ + 1) *> IO.raiseError[Unit](unavailable))(countAll).attempt

  test("CBR-02.1 concurrent callers reaching the threshold open the breaker exactly once") {
    TestControl.executeEmbed(
      for {
        pair <- breakerWithLog
        (breaker, log) = pair
        calls <- Ref.of[IO, Int](0)
        _ <- List.fill(20)(failOnce(breaker, calls)).parSequence
        transitions <- log.get
        state <- breaker.current
      } yield {
        assertEquals(transitions, List(Transition.Opened))
        assert(state.isInstanceOf[State.Open], clue(state))
      }
    )
  }

  test("CBR-02.3 an open breaker does not evaluate the protected effect") {
    TestControl.executeEmbed(
      for {
        pair <- breakerWithLog
        (breaker, _) = pair
        calls <- Ref.of[IO, Int](0)
        _ <- List.fill(3)(failOnce(breaker, calls)).sequence
        rejected <- breaker.protect(calls.update(_ + 1))(countAll)
        count <- calls.get
      } yield {
        assertEquals(count, 3)
        assert(rejected.left.exists(_.retryAt.nonEmpty), clue(rejected))
      }
    )
  }

  test("CBR-02.5 each state change is emitted once through open, half-open and recovery") {
    TestControl.executeEmbed(
      for {
        pair <- breakerWithLog
        (breaker, log) = pair
        calls <- Ref.of[IO, Int](0)
        _ <- List.fill(3)(failOnce(breaker, calls)).sequence
        _ <- breaker.protect(IO.unit)(countAll) // still open: no change
        _ <- IO.sleep(2.seconds)
        probe <- breaker.protect(IO.pure(1))(CircuitBreaker.classifier(_ => true))
        transitions <- log.get
        state <- breaker.current
      } yield {
        assertEquals(probe, Right(1))
        assertEquals(transitions, List(Transition.Opened, Transition.HalfOpened, Transition.Closed))
        assertEquals(state, State.Closed(Vector.empty))
      }
    )
  }

  test("CBR-02.5 a failed probe reopens with a longer period and reports the reopening") {
    TestControl.executeEmbed(
      for {
        pair <- breakerWithLog
        (breaker, log) = pair
        calls <- Ref.of[IO, Int](0)
        _ <- List.fill(3)(failOnce(breaker, calls)).sequence
        _ <- IO.sleep(2.seconds)
        _ <- failOnce(breaker, calls)
        early <- breaker.protect(IO.unit)(countAll)
        _ <- IO.sleep(3.seconds)
        stillOpen <- breaker.protect(IO.unit)(countAll)
        _ <- IO.sleep(1.second)
        probe <- breaker.protect(IO.unit)(countAll)
        transitions <- log.get
      } yield {
        assert(early.isLeft && stillOpen.isLeft, clues(early, stillOpen))
        assert(probe.isRight, clue(probe))
        assertEquals(
          transitions,
          List(Transition.Opened, Transition.HalfOpened, Transition.Reopened, Transition.HalfOpened, Transition.Closed)
        )
      }
    )
  }

  test("CBR-02.2 a cancelled half-open probe releases its slot and is not counted") {
    TestControl.executeEmbed(
      for {
        pair <- breakerWithLog
        (breaker, log) = pair
        calls <- Ref.of[IO, Int](0)
        _ <- List.fill(3)(failOnce(breaker, calls)).sequence
        _ <- IO.sleep(2.seconds)
        fiber <- breaker.protect(IO.never[Unit])(countAll).start
        _ <- IO.sleep(1.second)
        blocked <- breaker.protect(IO.unit)(countAll)
        _ <- fiber.cancel
        state <- breaker.current
        next <- breaker.protect(IO.unit)(countAll)
        transitions <- log.get
      } yield {
        assert(blocked.isLeft, clue(blocked))
        assert(state match { case State.HalfOpen(0, 1, _) => true; case _ => false }, clue(state))
        assert(next.isRight, clue(next))
        assertEquals(transitions, List(Transition.Opened, Transition.HalfOpened, Transition.Closed))
      }
    )
  }

  test("CBR-02.2 cancelled and ignored failures while closed are not counted") {
    TestControl.executeEmbed(
      for {
        breaker <- CircuitBreaker.create[IO](settings, _ => IO.unit)
        fiber <- breaker.protect(IO.never[Unit])(countAll).start
        _ <- IO.sleep(1.second)
        _ <- fiber.cancel
        ignore = CircuitBreaker.classifier[IO, Unit](_ => false)
        _ <- List.fill(5)(breaker.protect(IO.raiseError[Unit](unavailable))(ignore).attempt).sequence
        state <- breaker.current
      } yield assertEquals(state, State.Closed(Vector.empty))
    )
  }

  test("CBR-02.6 a failing transition listener neither fails the call nor leaks the probe slot") {
    TestControl.executeEmbed(
      for {
        breaker <- CircuitBreaker.create[IO](settings, _ => IO.raiseError(new RuntimeException("listener down")))
        calls <- Ref.of[IO, Int](0)
        _ <- List.fill(3)(failOnce(breaker, calls)).sequence
        _ <- IO.sleep(2.seconds)
        probe <- breaker.protect(IO.pure("ok"))(CircuitBreaker.classifier(_ => true))
        state <- breaker.current
      } yield {
        assertEquals(probe, Right("ok"))
        assertEquals(state, State.Closed(Vector.empty))
      }
    )
  }

  test("CBR-02.7 a probe of an earlier cycle finishing late cannot close the next cycle") {
    TestControl.executeEmbed(
      for {
        breaker <- CircuitBreaker.create[IO](settings.copy(halfOpenProbes = 2), _ => IO.unit)
        calls <- Ref.of[IO, Int](0)
        _ <- List.fill(3)(failOnce(breaker, calls)).sequence
        _ <- IO.sleep(2.seconds)
        oldGate <- Deferred[IO, Unit]
        newGate <- Deferred[IO, Unit]
        oldProbe <- breaker.protect(oldGate.get)(CircuitBreaker.classifier(_ => true)).start
        _ <- IO.sleep(1.second)
        // The second probe of the first cycle fails: the breaker reopens while the first probe is still running.
        _ <- failOnce(breaker, calls)
        _ <- IO.sleep(4.seconds)
        newProbe <- breaker.protect(newGate.get)(CircuitBreaker.classifier(_ => true)).start
        _ <- IO.sleep(1.second)
        _ <- oldGate.complete(())
        _ <- oldProbe.joinWithNever
        afterStale <- breaker.current
        _ <- newGate.complete(())
        _ <- newProbe.joinWithNever
        afterCurrent <- breaker.current
      } yield {
        assert(afterStale.isInstanceOf[State.HalfOpen], clue(afterStale))
        assertEquals(afterCurrent, State.Closed(Vector.empty))
      }
    )
  }

  test("CBR-02.4 a disabled breaker passes every call through and reports no state change") {
    val breaker = CircuitBreaker.disabled[IO]
    for {
      calls <- Ref.of[IO, Int](0)
      _ <- List.fill(10)(failOnce(breaker, calls)).sequence
      result <- breaker.protect(IO.pure("ok"))(CircuitBreaker.classifier(_ => true))
      count <- calls.get
      admits <- breaker.admits
    } yield {
      assertEquals(count, 10)
      assertEquals(result, Right("ok"))
      assert(admits)
    }
  }
}
