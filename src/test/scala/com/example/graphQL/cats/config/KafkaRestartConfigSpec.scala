package com.example.graphQL.cats.config

import munit.FunSuite

final class KafkaRestartConfigSpec extends FunSuite {
  private def configuration(overrides: String) = AppConfigFixtures.withPackagedDefaults(overrides)

  test("restart cap defaults to thirty seconds") {
    assertEquals(configuration("").fold(errors => fail(errors.toString), identity).kafka.restartMaxDelaySeconds, 30)
  }

  test("restart cap accepts endpoints and rejects out of range or malformed settings") {
    List(1, 300).foreach { seconds =>
      assertEquals(
        configuration(s"kafka.restart-max-delay-seconds=$seconds")
          .fold(errors => fail(errors.toString), identity)
          .kafka
          .restartMaxDelaySeconds,
        seconds
      )
    }
    List("0", "301", "invalid").foreach { value =>
      val errors = configuration(s"kafka.restart-max-delay-seconds=$value").swap.toOption
        .getOrElse(fail("Expected invalid configuration"))
      assert(errors.toList.contains(ConfigError.InvalidKafkaRestartMaxDelay))
    }
  }
}
