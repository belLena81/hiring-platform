package com.example.hiring.analytics.app

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import munit.FunSuite

final class StreamingBatchCallbackSpec extends FunSuite {
  test("untimed work runs unchanged when no cost observer is configured") {
    val work = StreamingBatchCallback.timed[IO, Int](
      None,
      com.example.hiring.analytics.adapter.spark.StreamingBatchCost.Stage.Callback
    )(IO.pure(7))
    assertEquals(work.unsafeRunSync(), 7)
  }
}
