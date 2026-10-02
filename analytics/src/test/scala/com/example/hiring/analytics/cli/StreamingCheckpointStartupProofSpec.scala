package com.example.hiring.analytics.cli

import com.example.hiring.analytics.errors.AnalyticsError

final class StreamingCheckpointStartupProofSpec extends munit.FunSuite {
  test("foreign checkpoint identity preserves the canonical shape and changes only its stream") {
    val original = "owned-stream\ncluster/topic\nlocal-lakehouse\ncontract-digest\nsettings-digest"
    assertEquals(
      StreamingCheckpointStartupProofMain.foreignIdentity(original, "owned-stream"),
      Some("owned-stream-foreign\ncluster/topic\nlocal-lakehouse\ncontract-digest\nsettings-digest")
    )
    Vector(
      original + "\n",
      original.replace("cluster/topic", ""),
      original.replace("local-lakehouse", "\r"),
      StreamingCheckpointStartupProofMain.CorruptionSentinel,
      "different-stream\ncluster/topic\nlocal-lakehouse\ncontract-digest\nsettings-digest"
    ).foreach(value => assertEquals(StreamingCheckpointStartupProofMain.foreignIdentity(value, "owned-stream"), None))
  }

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
