package com.example.hiring.analytics.cli

import com.example.hiring.analytics.errors.AnalyticsError

final class StreamingCheckpointStartupProofSpec extends munit.FunSuite {
  test("only the exact typed checkpoint owner failure passes") {
    assert(
      StreamingCheckpointStartupProofMain.isCheckpointFailure(
        AnalyticsError.InvalidConfiguration(StreamingCheckpointStartupProofMain.CheckpointFailure)
      )
    )
    Vector[Throwable](
      AnalyticsError.InvalidConfiguration("analytics streaming activation grant expired"),
      AnalyticsError.InvalidConfiguration("analytics configuration is invalid"),
      new IllegalStateException(StreamingCheckpointStartupProofMain.CheckpointFailure),
      AnalyticsError.LakehouseFailure(
        AnalyticsError.InvalidConfiguration(StreamingCheckpointStartupProofMain.CheckpointFailure)
      )
    ).foreach(error => assert(!StreamingCheckpointStartupProofMain.isCheckpointFailure(error)))
  }
}
