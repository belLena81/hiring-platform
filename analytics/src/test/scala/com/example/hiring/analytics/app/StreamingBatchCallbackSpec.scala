package com.example.hiring.analytics.app

import com.example.hiring.analytics.service.streaming.AnalyticsStreamingMaintenance.{Observation, TickOutcome}

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import munit.FunSuite

import java.time.Instant
import scala.concurrent.duration.*

final class StreamingBatchCallbackSpec extends FunSuite {
  private val observation = Observation(
    Some(Instant.parse("2026-10-10T00:00:00Z")),
    None,
    Some(TickOutcome.Succeeded),
    2L,
    5.seconds,
    9.seconds
  )

  test("maintenance observations render one sanitized structured line") {
    assertEquals(
      StreamingBatchCallback.maintenanceLogLine(observation),
      "STREAMING_MAINTENANCE outcome=Succeeded elapsedSinceSuccessMillis=5000 maximumElapsedSinceSuccessMillis=9000 " +
        "consecutiveDeferrals=2 lastSuccess=2026-10-10T00:00:00Z"
    )
    assert(
      StreamingBatchCallback
        .maintenanceLogLine(observation.copy(outcome = None, lastSuccess = None))
        .endsWith("lastSuccess=none")
    )
    StreamingBatchCallback.logMaintenance[IO](observation).unsafeRunSync()
  }

  test("untimed work runs unchanged when no cost observer is configured") {
    val work = StreamingBatchCallback.timed[IO, Int](
      None,
      com.example.hiring.analytics.adapter.spark.StreamingBatchCost.Stage.Callback
    )(IO.pure(7))
    assertEquals(work.unsafeRunSync(), 7)
  }
}
