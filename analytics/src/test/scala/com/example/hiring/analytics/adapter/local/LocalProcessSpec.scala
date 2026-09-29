package com.example.hiring.analytics.adapter.local

import cats.effect.IO
import munit.CatsEffectSuite

import scala.concurrent.duration.*

final class LocalProcessSpec extends CatsEffectSuite {
  test("local process runner preserves arguments and merges output") {
    LocalProcess
      .run[IO](
        Vector("sh", "-c", "printf '%s' \"$1\"; printf '%s' \"$2\" >&2", "sh", "first value", "stderr"),
        "command timed out",
        "command could not start"
      )
      .map { result =>
        assertEquals(result.exitCode, 0)
        assertEquals(result.output, "first valuestderr")
      }
  }

  test("local process runner returns a nonzero exit code") {
    LocalProcess
      .run[IO](Vector("sh", "-c", "exit 7"), "command timed out", "command could not start")
      .map(result => assertEquals(result.exitCode, 7))
  }

  test("local process timeout releases the child process") {
    LocalProcess
      .run[IO](Vector("sh", "-c", "exec sleep 10"), "command timed out", "command could not start", 1.second)
      .attempt
      .map(result => assert(result.left.exists(_.getMessage == "command timed out")))
  }
}
