package com.example.hiring.analytics.cli

import com.example.hiring.analytics.errors.AnalyticsError

final class StreamingGrantExpiryProofSpec extends munit.FunSuite {
  test("expiry observations recognize only typed production timer and activation gate errors") {
    assert(
      StreamingGrantExpiryProofMain.isExpiryFailure(
        AnalyticsError.InvalidConfiguration("analytics streaming activation grant expired")
      )
    )
    assert(
      StreamingGrantExpiryProofMain.isExpiryFailure(
        AnalyticsError.InvalidConfiguration(
          "analytics streaming activation is absent, malformed, or does not match this runtime"
        )
      )
    )
    Vector[Throwable](
      AnalyticsError.InvalidConfiguration(StreamingCheckpointStartupProofMain.CheckpointFailure),
      AnalyticsError.InvalidConfiguration("analytics configuration is invalid"),
      new IllegalStateException("analytics streaming activation grant expired")
    ).foreach(error => assert(!StreamingGrantExpiryProofMain.isExpiryFailure(error)))
  }
}
