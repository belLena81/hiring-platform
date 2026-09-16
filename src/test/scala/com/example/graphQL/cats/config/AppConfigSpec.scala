package com.example.graphQL.cats.config

import munit.FunSuite

class AppConfigSpec extends FunSuite {
  test("P1-AC01 defaults and explicit overrides") {
    assertEquals(AppConfig.fromEnvironment(Map.empty), Right(AppConfig(
      "127.0.0.1", 8080, "mongodb://127.0.0.1:27017", "hiring", "INFO"
    )))
    assertEquals(AppConfig.fromEnvironment(Map(
      "HTTP_HOST" -> "::1", "HTTP_PORT" -> "65535",
      "MONGODB_URI" -> "mongodb://test-user:synthetic-secret@localhost:27018/?authSource=admin",
      "MONGODB_DATABASE" -> "hiring_test-2", "LOG_LEVEL" -> "WARN"
    )), Right(AppConfig("::1", 65535,
      "mongodb://test-user:synthetic-secret@localhost:27018/?authSource=admin",
      "hiring_test-2", "WARN")))
  }

  test("P1-AC01 rejects invalid values without returning their contents") {
    val invalid = List(
      ("HTTP_HOST", List("localhost", "", "999.1.1.1", "127.0.0.1\n"), ConfigError.InvalidHost),
      ("HTTP_PORT", List("0", "65536", "-1", "+80", " 80", "2147483648", ""), ConfigError.InvalidPort),
      ("MONGODB_URI", List("https://synthetic-secret", "mongodb://", "mongodb://host:wrong"), ConfigError.InvalidMongoUri),
      ("MONGODB_DATABASE", List("", "a/b", "a.b", "a b", "a$b", "a\u0000b", "a" * 64), ConfigError.InvalidMongoDatabase),
      ("LOG_LEVEL", List("DEBUG", "info", "", "synthetic-secret"), ConfigError.InvalidLogLevel)
    )
    invalid.foreach { case (key, values, error) =>
      values.foreach { value =>
        val result = AppConfig.fromEnvironment(Map(key -> value))
        assertEquals(result, Left(error), clues(key))
        assert(!result.toString.contains("synthetic-secret"))
      }
    }
  }

  test("P1-AC01 accepts boundary ports and all supported severities") {
    List("1", "65535").foreach { port =>
      List("INFO", "WARN", "ERROR").foreach { level =>
        assert(AppConfig.fromEnvironment(Map("HTTP_PORT" -> port, "LOG_LEVEL" -> level)).isRight)
      }
    }
  }

  test("P1-AC01 config rendering never exposes a secret-bearing URI") {
    val result = AppConfig.fromEnvironment(Map(
      "MONGODB_URI" -> "mongodb://user:synthetic-secret@localhost:27017"
    ))
    assert(result.isRight)
    assert(!result.toString.contains("synthetic-secret"))
    assert(!result.toString.contains("mongodb://"))
  }

  test("LOG-03 secure defaults apply even to direct configuration construction") {
    val config = AppConfig("127.0.0.1", 8080, "mongodb://localhost", "hiring", "INFO")
    assertEquals(config.appEnv, "production")
    assert(config.maskSensitive)
    assert(!config.requestPayloads)
    assertEquals(AppConfig.fromEnvironment(Map.empty).map(value =>
      (value.appEnv, value.maskSensitive, value.requestPayloads)), Right(("production", true, false)))
  }

  test("LOG-03 explicit local unmasking and payload opt-in accept parsed loopback addresses") {
    List("127.0.0.1", "127.0.0.0", "127.25.67.89", "127.255.255.255",
      "::1", "0:0:0:0:0:0:0:1", "0000:0000:0000:0000:0000:0000:0000:0001",
      "::ffff:127.0.0.1").foreach { host =>
      List("false", "true").foreach { payloads =>
        val result = AppConfig.fromEnvironment(Map(
          "HTTP_HOST" -> host, "APP_ENV" -> "local", "LOG_MASK_SENSITIVE" -> "false",
          "LOG_REQUEST_PAYLOADS" -> payloads
        ))
        assertEquals(result.map(value => (value.appEnv, value.maskSensitive, value.requestPayloads)),
          Right(("local", false, payloads == "true")), clues(host))
      }
    }
  }

  test("LOG-03 unsafe logging flags require explicit local loopback and disabled masking") {
    List("0.0.0.0", "::", "192.0.2.1", "126.255.255.255", "128.0.0.1",
      "::2", "2001:db8::1", "::ffff:192.0.2.1").foreach { host =>
      assertEquals(AppConfig.fromEnvironment(Map("HTTP_HOST" -> host, "APP_ENV" -> "local",
        "LOG_MASK_SENSITIVE" -> "false")), Left(ConfigError.UnsafeMaskSensitive), clues(host))
      assert(AppConfig.fromEnvironment(Map("HTTP_HOST" -> host)).isRight, clues(host))
    }
    List(Map.empty[String, String], Map("APP_ENV" -> "production")).foreach { environment =>
      assertEquals(AppConfig.fromEnvironment(environment + ("LOG_MASK_SENSITIVE" -> "false")),
        Left(ConfigError.UnsafeMaskSensitive))
      assertEquals(AppConfig.fromEnvironment(environment + ("LOG_REQUEST_PAYLOADS" -> "true")),
        Left(ConfigError.UnsafeRequestPayloads))
    }
    assertEquals(AppConfig.fromEnvironment(Map("APP_ENV" -> "local", "LOG_REQUEST_PAYLOADS" -> "true")),
      Left(ConfigError.UnsafeRequestPayloads))
  }

  test("LOG-03 environment and logging booleans are strict with safe configuration keys") {
    List(
      ("APP_ENV", List("", "LOCAL", "development", " local", "local\n", "synthetic-secret"), ConfigError.InvalidAppEnv),
      ("LOG_MASK_SENSITIVE", List("", "TRUE", "0", "false ", "synthetic-secret"), ConfigError.InvalidMaskSensitive),
      ("LOG_REQUEST_PAYLOADS", List("", "FALSE", "1", "true\n", "synthetic-secret"), ConfigError.InvalidRequestPayloads)
    ).foreach { case (key, values, expected) =>
      values.foreach { value =>
        val result = AppConfig.fromEnvironment(Map(key -> value))
        assertEquals(result, Left(expected), clues(key))
        assertEquals(expected.key, key)
        assert(!result.toString.contains("synthetic-secret"))
      }
    }
    assertEquals(AppConfig.fromEnvironment(Map("APP_ENV" -> "local", "HTTP_HOST" -> "localhost",
      "LOG_MASK_SENSITIVE" -> "false")), Left(ConfigError.InvalidHost))
  }

  test("LOG-04 configuration remains redacted when local metadata and payload capture are enabled") {
    val result = AppConfig.fromEnvironment(Map("APP_ENV" -> "local", "LOG_MASK_SENSITIVE" -> "false",
      "LOG_REQUEST_PAYLOADS" -> "true", "MONGODB_URI" -> "mongodb://user:synthetic-secret@127.0.0.1:1"))
    assert(result.isRight)
    assert(!result.toString.contains("synthetic-secret"))
    assert(!result.toString.contains("mongodb://"))
  }
}
