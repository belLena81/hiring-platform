package com.example.hiring.analytics

import cats.Applicative
import cats.effect.{Clock, IO}

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

/** Deterministic clocks for tests that need a fixed instant instead of wall-clock time. */
object AnalyticsTestClocks {

  /** Always reports `at` as the wall-clock instant; the monotonic clock follows the real one. */
  def fixed(at: Instant): Clock[IO] = new Clock[IO] {
    override val applicative: Applicative[IO] = IO.asyncForIO
    override def monotonic: IO[FiniteDuration] = IO.monotonic
    override def realTime: IO[FiniteDuration] =
      IO.pure(
        FiniteDuration(at.getEpochSecond, TimeUnit.SECONDS) + FiniteDuration(at.getNano.toLong, TimeUnit.NANOSECONDS)
      )
    override def realTimeInstant: IO[Instant] = IO.pure(at)
  }
}
