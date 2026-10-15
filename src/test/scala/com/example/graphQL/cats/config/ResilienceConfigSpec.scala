package com.example.graphQL.cats.config

import munit.FunSuite

final class ResilienceConfigSpec extends FunSuite {
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

  private def load(extra: String = "", env: Map[String, String] = Map.empty) =
    AppConfigFixtures.fromConfig(packaged + local + extra, env)

  test("CBR-07.1 packaged defaults load with the conservative Mongo write breaker") {
    val resilience = load().map(_.resilience)
    assertEquals(resilience.map(_.mongoWrites), Right(CircuitBreakerConfig(true, 20, 10000, 2000, 30000, 1)))
    assertEquals(resilience.map(_.voyage), Right(ResilienceConfig.Voyage))
    assertEquals(resilience.map(_.atlasSearch), Right(ResilienceConfig.AtlasSearch))
    assertEquals(resilience.map(_.kafkaPublisher), Right(ResilienceConfig.KafkaPublisher))
  }

  test("CBR-07.1 environment overrides switch a breaker off and change its threshold") {
    val resilience = load(env =
      Map("BREAKER_VOYAGE_ENABLED" -> "false", "BREAKER_MONGO_WRITES_FAILURE_THRESHOLD" -> "50")
    ).map(_.resilience)
    assertEquals(resilience.map(_.voyage.enabled), Right(false))
    assertEquals(resilience.map(_.mongoWrites.failureThreshold), Right(50))
  }

  test("CBR-07.2 open-ms above max-open-ms and a zero threshold are reported together") {
    val extra =
      """resilience.breakers.mongo-writes.open-ms = 40000
        |resilience.breakers.voyage.failure-threshold = 0
        |""".stripMargin
    load(extra) match {
      case Left(issues) =>
        assert(issues.toList.contains(ConfigError.InvalidBreakerOpenWindow), clue(issues))
        assert(issues.toList.contains(ConfigError.InvalidBreakerFailureThreshold), clue(issues))
      case other => fail(s"Expected both breaker errors, got $other")
    }
  }

  test("CBR-07.2 a window, probe count or open period outside its range is rejected") {
    List(
      ("resilience.breakers.atlas-search.half-open-probes = 0", ConfigError.InvalidBreakerHalfOpenProbes),
      ("resilience.breakers.kafka-publisher.window-ms = 5", ConfigError.InvalidBreakerWindow),
      ("resilience.breakers.mongo-writes.open-ms = 5", ConfigError.InvalidBreakerOpen)
    ).foreach { case (line, expected) =>
      load(line) match {
        case Left(issues) => assert(issues.toList.contains(expected), clues(line, issues))
        case other        => fail(s"Expected $expected for $line, got $other")
      }
    }
  }

  test("CBR-07.1 an absent resilience section keeps the defaults") {
    val withoutSection = packaged.linesIterator.takeWhile(!_.startsWith("resilience.breakers")).mkString("\n")
    assertEquals(
      AppConfigFixtures.fromConfig(withoutSection + "\n" + local, Map.empty).map(_.resilience),
      Right(ResilienceConfig())
    )
  }
}
