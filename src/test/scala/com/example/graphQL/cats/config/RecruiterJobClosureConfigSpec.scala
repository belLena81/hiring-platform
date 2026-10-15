package com.example.graphQL.cats.config

import munit.FunSuite

/** The `recruiter-job-closure` section: packaged defaults, absent section, bounds, backoff order, accumulation. */
final class RecruiterJobClosureConfigSpec extends FunSuite {
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

  private def section(overrides: String): String =
    s"""recruiter-job-closure {
       |  page-size = 1000
       |  idle-poll-millis = 1000
       |  failure-backoff-initial-millis = 1000
       |  failure-backoff-max-millis = 30000
       |  row-retry-initial-millis = 5000
       |  row-retry-max-millis = 900000
       |  $overrides
       |}
       |""".stripMargin

  private def load(extra: String) = AppConfigFixtures.fromConfig(packaged + local + extra, Map.empty)

  private def overridden(overrides: String): Either[List[ConfigError], RecruiterJobClosureConfig] =
    load(section(overrides)).left.map(_.toList).map(_.recruiterJobClosure)

  test("packaged defaults equal the typed defaults") {
    assertEquals(load("").map(_.recruiterJobClosure), Right(RecruiterJobClosureConfig()))
  }

  test("an absent section keeps the typed defaults") {
    val withoutSection = packaged.linesIterator
      .foldLeft((List.empty[String], false)) {
        case ((kept, true), line)  => (kept, !line.trim.startsWith("}"))
        case ((kept, false), line) =>
          if (line.startsWith("recruiter-job-closure")) (kept, true) else (line :: kept, false)
      }
      ._1
      .reverse
      .mkString("\n")
    assert(!withoutSection.contains("recruiter-job-closure {"), clue(withoutSection))
    assertEquals(
      AppConfigFixtures.fromConfig(withoutSection + "\n" + local, Map.empty).map(_.recruiterJobClosure),
      Right(RecruiterJobClosureConfig())
    )
  }

  test("each out-of-range value is rejected with its own error") {
    val cases = List(
      "page-size = 0" -> ConfigError.InvalidRecruiterJobClosurePageSize,
      "page-size = 1001" -> ConfigError.InvalidRecruiterJobClosurePageSize,
      "idle-poll-millis = 9" -> ConfigError.InvalidRecruiterJobClosureIdlePoll,
      "idle-poll-millis = 86400001" -> ConfigError.InvalidRecruiterJobClosureIdlePoll,
      "failure-backoff-initial-millis = 9" -> ConfigError.InvalidRecruiterJobClosureFailureBackoff,
      "failure-backoff-max-millis = 86400001" -> ConfigError.InvalidRecruiterJobClosureFailureBackoff,
      "row-retry-initial-millis = 0" -> ConfigError.InvalidRecruiterJobClosureRowRetry,
      "row-retry-max-millis = 86400001" -> ConfigError.InvalidRecruiterJobClosureRowRetry
    )
    cases.foreach { case (value, expected) =>
      assertEquals(overridden(value), Left(List(expected)), value)
    }
  }

  test("range bounds are inclusive") {
    assertEquals(
      overridden(
        "page-size = 1\nidle-poll-millis = 10\nfailure-backoff-max-millis = 86400000\nrow-retry-max-millis = 86400000"
      ).map(c => (c.pageSize, c.idlePollMillis, c.failureBackoffMaxMillis, c.rowRetryMaxMillis)),
      Right((1, 10, 86400000, 86400000))
    )
  }

  test("an initial backoff above its maximum is rejected; equal values are accepted") {
    assertEquals(
      overridden("failure-backoff-initial-millis = 40000"),
      Left(List(ConfigError.InvalidRecruiterJobClosureBackoffOrder))
    )
    assertEquals(
      overridden("row-retry-initial-millis = 900001"),
      Left(List(ConfigError.InvalidRecruiterJobClosureBackoffOrder))
    )
    assert(overridden("failure-backoff-initial-millis = 30000\nrow-retry-initial-millis = 900000").isRight)
  }

  test("an out-of-range backoff does not also report the order error") {
    assertEquals(
      overridden("failure-backoff-initial-millis = 9\nfailure-backoff-max-millis = 5"),
      Left(
        List(
          ConfigError.InvalidRecruiterJobClosureFailureBackoff,
          ConfigError.InvalidRecruiterJobClosureFailureBackoff
        )
      )
    )
  }

  test("every violation is reported together") {
    overridden(
      "page-size = 0\nidle-poll-millis = 0\nrow-retry-initial-millis = 0\nfailure-backoff-initial-millis = 40000"
    ) match {
      case Left(issues) =>
        assertEquals(
          issues.toSet,
          Set(
            ConfigError.InvalidRecruiterJobClosurePageSize,
            ConfigError.InvalidRecruiterJobClosureIdlePoll,
            ConfigError.InvalidRecruiterJobClosureRowRetry,
            ConfigError.InvalidRecruiterJobClosureBackoffOrder
          )
        )
      case other => fail(s"Expected accumulated errors, got $other")
    }
  }
}
