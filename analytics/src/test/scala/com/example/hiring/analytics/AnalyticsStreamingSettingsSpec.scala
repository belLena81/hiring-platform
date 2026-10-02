package com.example.hiring.analytics

import com.example.hiring.analytics.config.AnalyticsStreamingSettings
import com.example.hiring.analytics.config.MaximumOffsetsPerTrigger.*
import com.example.hiring.analytics.errors.AnalyticsError

import munit.FunSuite
import scala.concurrent.duration.*

class AnalyticsStreamingSettingsSpec extends FunSuite {
  private val valid = """
    |analytics.streaming {
    |  stream-id = "hiring-events"
    |  activation-grant-id = "grant-2026-09"
    |  checkpoint-location = "file:///var/lib/hiring-analytics/checkpoints/hiring-events"
    |  trigger-interval = 10 seconds
    |  max-offsets-per-trigger = 1000
    |  maintenance-interval = 60 seconds
    |  progress-retention = 7 days
    |  maximum-replay-records = 1000
    |  initial-offsets = [{ partition = 0, offset = 12 }, { partition = 1, offset = 0 }]
    |}
    |""".stripMargin

  test("stream settings accept explicit unique offsets for checkpoint initialization") {
    val parsed = AnalyticsStreamingSettings.fromHocon(valid)
    assert(parsed.isRight)
    assertEquals(parsed.toOption.map(_.initialOffsets.size), Some(2))
    assertEquals(parsed.toOption.map(_.maxOffsetsPerTrigger.value), Some(1000))
    assertEquals(parsed.flatMap(AnalyticsStreamingSettings.validatePartitionCoverage(_, Set(0, 1))), Right(()))
    assert(parsed.flatMap(AnalyticsStreamingSettings.validatePartitionCoverage(_, Set(0))).isLeft)
    assert(parsed.flatMap(AnalyticsStreamingSettings.validatePartitionCoverage(_, Set(0, 1, 2))).isLeft)
  }

  test("trigger interval is configurable up to the ten-second default bound") {
    val configured = valid.replace("trigger-interval = 10 seconds", "trigger-interval = 5 seconds")
    assertEquals(AnalyticsStreamingSettings.fromHocon(configured).map(_.triggerInterval), Right(5.seconds))
  }

  test("activation identity is bound to Kafka cluster and topic IDs") {
    val settings = AnalyticsStreamingSettings.fromHocon(valid).fold(error => fail(error.getMessage), identity)
    val original = settings.activationIdentity("cluster-a", "topic-a", "hiring.events", "file:///tmp/lakehouse")
    val changedCluster = settings.activationIdentity("cluster-b", "topic-a", "hiring.events", "file:///tmp/lakehouse")
    val changedTopic = settings.activationIdentity("cluster-a", "topic-b", "hiring.events", "file:///tmp/lakehouse")

    assert(original.isRight)
    assertNotEquals(original.toOption.map(_.sourceIdentity), changedCluster.toOption.map(_.sourceIdentity))
    assertNotEquals(original.toOption.map(_.sourceIdentity), changedTopic.toOption.map(_.sourceIdentity))
  }

  test("activation grant renewal does not change checkpoint identity") {
    val original = AnalyticsStreamingSettings.fromHocon(valid).toOption.get
    val renewed = AnalyticsStreamingSettings
      .fromHocon(
        valid.replace("grant-2026-09", "grant-2026-10")
      )
      .toOption
      .get
    assertEquals(
      original.activationIdentity("cluster-a", "topic-a", "hiring.events", "file:///tmp/lakehouse"),
      renewed.activationIdentity("cluster-a", "topic-a", "hiring.events", "file:///tmp/lakehouse")
    )
  }

  test("stream settings reject implicit latest offsets and duplicate partition declarations") {
    val emptyOffsets = valid.replace(
      "[{ partition = 0, offset = 12 }, { partition = 1, offset = 0 }]",
      "[]"
    )
    val duplicateOffsets = valid.replace(
      "{ partition = 1, offset = 0 }",
      "{ partition = 0, offset = 30 }"
    )

    assert(AnalyticsStreamingSettings.fromHocon(emptyOffsets).left.exists {
      case AnalyticsError.InvalidConfiguration(message) => message.contains("explicitly name every source partition")
      case _                                            => false
    })
    assert(AnalyticsStreamingSettings.fromHocon(duplicateOffsets).left.exists {
      case AnalyticsError.InvalidConfiguration(message) => message.contains("each partition once")
      case _                                            => false
    })
  }

  test("replay cap cannot exceed one thousand retained records") {
    val overLimit = valid.replace("maximum-replay-records = 1000", "maximum-replay-records = 1001")
    assert(AnalyticsStreamingSettings.fromHocon(overLimit).left.exists {
      case AnalyticsError.InvalidConfiguration(message) => message.contains("between 1 and 1000")
      case _                                            => false
    })
  }

  test("checkpoint storage must remain inside an owned ignored analytics runtime directory") {
    val unowned = valid.replace(
      "file:///var/lib/hiring-analytics/checkpoints/hiring-events",
      "file:///tmp/hiring-events"
    )
    assert(AnalyticsStreamingSettings.fromHocon(unowned).isLeft)
  }

  test("checkpoint identity rejects escaped aliases and accepts canonical escaped physical names") {
    val original = "file:///var/lib/hiring-analytics/checkpoints/hiring-events"
    val canonical = "file:///var/lib/hiring-analytics/checkpoints/hiring+events%20with%25percent"
    assert(AnalyticsStreamingSettings.fromHocon(valid.replace(original, canonical)).isRight)
    Vector(
      "file:///var/lib/hiring-analytics/checkpoints/hiring%2Bevents",
      "file:///var/lib/hiring-analytics/checkpoints/%68iring-events"
    ).foreach(alias => assert(AnalyticsStreamingSettings.fromHocon(valid.replace(original, alias)).isLeft))
  }

  test("batch limits and trigger policy are bounded and invalid values are not echoed in diagnostics") {
    val excessiveOffsets = valid.replace("max-offsets-per-trigger = 1000", "max-offsets-per-trigger = 100001")
    val unsupportedTrigger = valid.replace("trigger-interval = 10 seconds", "trigger-interval = 11 seconds")
    val sensitiveInvalidValue = valid.replace("trigger-interval = 10 seconds", "trigger-interval = \"private-value\"")

    assert(AnalyticsStreamingSettings.fromHocon(excessiveOffsets).isLeft)
    assert(AnalyticsStreamingSettings.fromHocon(unsupportedTrigger).isLeft)
    assert(AnalyticsStreamingSettings.fromHocon(sensitiveInvalidValue).left.exists {
      case AnalyticsError.InvalidConfiguration(message) => !message.contains("private-value")
      case _                                            => false
    })
  }
  test("maintenance and progress retention settings are positive and identity-bound") {
    val settings = AnalyticsStreamingSettings.fromHocon(valid).toOption.get
    assertEquals(settings.maintenanceInterval, 60.seconds)
    assertEquals(settings.progressRetention, 7.days)
    List(
      "maintenance-interval = 60 seconds" -> "maintenance-interval = 0 seconds",
      "progress-retention = 7 days" -> "progress-retention = 0 days"
    ).foreach { case (old, changed) =>
      assert(AnalyticsStreamingSettings.fromHocon(valid.replace(old, changed)).isLeft)
    }
    val changed = AnalyticsStreamingSettings
      .fromHocon(valid.replace("progress-retention = 7 days", "progress-retention = 8 days"))
      .toOption
      .get
    assertNotEquals(
      settings.activationIdentity("cluster", "topic", "hiring.events", "file:///tmp/lakehouse"),
      changed.activationIdentity("cluster", "topic", "hiring.events", "file:///tmp/lakehouse")
    )
  }

}
