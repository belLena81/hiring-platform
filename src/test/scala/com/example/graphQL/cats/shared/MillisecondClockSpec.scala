package com.example.graphQL.cats.shared

import cats.effect.{Clock, IO}
import cats.Applicative
import munit.CatsEffectSuite
import scala.concurrent.duration.*

final class MillisecondClockSpec extends CatsEffectSuite {
  private def fixed(time: FiniteDuration, monotonicTime: FiniteDuration): Clock[IO] = new Clock[IO] {
    override def applicative: Applicative[IO] = Applicative[IO]
    override def monotonic: IO[FiniteDuration] = IO.pure(monotonicTime)
    override def realTime: IO[FiniteDuration] = IO.pure(time)
  }

  test("wall time is truncated to the millisecond MongoDB stores, so a stamped instant survives a round trip") {
    // Given a clock with microsecond precision
    val clock = MillisecondClock(fixed(1_700_000_000_123_456L.micros, 7.nanos))
    // When an instant is stamped
    clock.realTimeInstant.map { instant =>
      // Then the sub-millisecond part is dropped
      assertEquals(instant.toEpochMilli, 1_700_000_000_123L)
      assertEquals(instant.getNano % 1_000_000, 0)
    }
  }

  test("monotonic time is passed through unchanged") {
    MillisecondClock(fixed(1.second, 987_654_321.nanos)).monotonic.assertEquals(987_654_321.nanos)
  }

  test("an already millisecond-precise time is unchanged") {
    MillisecondClock(fixed(42.millis, 0.nanos)).realTime.assertEquals(42.millis)
  }
}
