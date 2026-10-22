package com.example.graphQL.cats.config

import munit.FunSuite
import scala.concurrent.duration.*

final class ReadinessConfigSpec extends FunSuite {
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

  test("ARR-07.1 packaged readiness thresholds equal the typed defaults") {
    assertEquals(load().map(_.readiness), Right(ReadinessConfig()))
    assertEquals(ReadinessConfig(), ReadinessConfig(60.seconds, 300.seconds, 1800.seconds, 1000.millis))
  }

  test("ARR-07.2 the closure-row age threshold exceeds the row retry cap so a backing-off row is not an alert") {
    val config = load().toOption.get
    assert(config.readiness.maxClosureWorkAge.toMillis > config.recruiterJobClosure.rowRetryMaxMillis.toLong)
  }

  test("ARR-07.1 an out-of-range threshold is rejected with every violation reported together") {
    val extra =
      """readiness.worker-staleness-seconds = 0
        |readiness.max-outbox-age-seconds = -5
        |readiness.max-closure-work-age-seconds = 604801
        |readiness.report-cache-millis = 10001
        |""".stripMargin
    load(extra) match {
      case Left(issues) =>
        assert(issues.toList.contains(ConfigError.InvalidReadinessWorkerStaleness), clue(issues))
        assert(issues.toList.contains(ConfigError.InvalidReadinessOutboxAge), clue(issues))
        assert(issues.toList.contains(ConfigError.InvalidReadinessClosureWorkAge), clue(issues))
        assert(issues.toList.contains(ConfigError.InvalidReadinessCache), clue(issues))
      case other => fail(s"Expected every readiness error, got $other")
    }
  }

  test("ARR-07.1 thresholds on the range edges are accepted") {
    val extra =
      """readiness.worker-staleness-seconds = 1
        |readiness.max-outbox-age-seconds = 604800
        |readiness.max-closure-work-age-seconds = 604800
        |readiness.report-cache-millis = 100
        |""".stripMargin
    assertEquals(
      load(extra).map(_.readiness),
      Right(ReadinessConfig(1.second, 604800.seconds, 604800.seconds, 100.millis))
    )
    assert(load("readiness.report-cache-millis = 10000\n").isRight)
  }
}
