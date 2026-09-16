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
}
