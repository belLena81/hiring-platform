package com.example.hiring.analytics

import com.example.hiring.analytics.config.AnalyticsStreamingSettings
import com.example.hiring.analytics.config.MaximumOffsetsPerTrigger.*
import com.example.hiring.analytics.errors.AnalyticsError

import munit.FunSuite

class AnalyticsStreamingSettingsSpec extends FunSuite {
  private val valid = """
    |analytics.streaming {
    |  stream-id = "hiring-events"
    |  checkpoint-location = "file:///var/lib/hiring-analytics/checkpoints/hiring-events"
    |  trigger-interval = 60 seconds
    |  max-offsets-per-trigger = 1000
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

  test("batch limits and trigger policy are bounded and invalid values are not echoed in diagnostics") {
    val excessiveOffsets = valid.replace("max-offsets-per-trigger = 1000", "max-offsets-per-trigger = 100001")
    val unsupportedTrigger = valid.replace("trigger-interval = 60 seconds", "trigger-interval = 1 nanosecond")
    val sensitiveInvalidValue = valid.replace("trigger-interval = 60 seconds", "trigger-interval = \"private-value\"")

    assert(AnalyticsStreamingSettings.fromHocon(excessiveOffsets).isLeft)
    assert(AnalyticsStreamingSettings.fromHocon(unsupportedTrigger).isLeft)
    assert(AnalyticsStreamingSettings.fromHocon(sensitiveInvalidValue).left.exists {
      case AnalyticsError.InvalidConfiguration(message) => !message.contains("private-value")
      case _                                            => false
    })
  }
}
