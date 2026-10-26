package com.example.graphQL.cats.config

import munit.FunSuite
import scala.concurrent.duration.*

final class TelemetryConfigSpec extends FunSuite {
  private val packaged: String = {
    val stream = Option(getClass.getClassLoader.getResourceAsStream("application.conf"))
      .getOrElse(throw new IllegalArgumentException("Missing resource application.conf"))
    try new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
    finally stream.close()
  }

  private val local =
    """http {
      |  host = "127.0.0.1"
      |  port = 8080
      |  admission-permits = 16
      |  request-timeout-ms = 5000
      |  trusted-proxy-cidrs = []
      |}
      |mongo.uri = "mongodb://127.0.0.1:27017"
      |auth.jwt.hs256-secret = "01234567890123456789012345678901"
      |""".stripMargin

  private def load(extra: String = "") = AppConfigFixtures.fromConfig(packaged + local + extra, Map.empty)

  test("the packaged backlog sample interval equals the typed default") {
    assertEquals(load().map(_.telemetry), Right(TelemetryConfig()))
    assertEquals(TelemetryConfig().backlogSampleInterval, 15.seconds)
  }

  test("an out-of-range backlog sample interval is rejected and the range edges are accepted") {
    load("telemetry.backlog-sample-seconds = 0\n") match {
      case Left(issues) => assert(issues.toList.contains(ConfigError.InvalidBacklogSampleInterval), clue(issues))
      case other        => fail(s"Expected an interval error, got $other")
    }
    assert(load("telemetry.backlog-sample-seconds = 3601\n").isLeft)
    assertEquals(load("telemetry.backlog-sample-seconds = 1\n").map(_.telemetry.backlogSampleInterval), Right(1.second))
    assert(load("telemetry.backlog-sample-seconds = 3600\n").isRight)
  }
}
