package com.example.graphQL.cats

import cats.effect.{Clock, IO}
import java.time.Instant
import scala.concurrent.duration.*

object FixedTestClock {
  def at(now: Instant): Clock[IO] = new Clock[IO] {
    override val applicative: cats.Applicative[IO] = IO.asyncForIO
    override def realTime: IO[FiniteDuration] = IO.pure(now.toEpochMilli.millis)
    override def monotonic: IO[FiniteDuration] = IO.pure(Duration.Zero)
  }
}
