package com.example.graphQL.cats.config

import munit.FunSuite

class AppConfigSpec extends FunSuite {
  private val defaultConfig =
    """HTTP_HOST=127.0.0.1
      |HTTP_PORT=8080
      |MONGODB_URI=mongodb://127.0.0.1:27017
      |MONGODB_DATABASE=hiring
      |LOG_LEVEL=INFO
      |LOG_MASK_SENSITIVE=true
      |LOG_REQUEST_PAYLOADS=false
      |""".stripMargin

  test("P1-AC01 loads application settings from config text and resolves secret env placeholders") {
    val config =
      """HTTP_HOST=::1
        |HTTP_PORT=65535
        |MONGODB_URI={$MONGODB_URI}
        |MONGODB_DATABASE=hiring_test-2
        |LOG_LEVEL=WARN
        |LOG_MASK_SENSITIVE=true
        |LOG_REQUEST_PAYLOADS=false
        |""".stripMargin
    assertEquals(AppConfig.fromConfig(config, Map(
      "MONGODB_URI" -> "mongodb://test-user:synthetic-secret@localhost:27018/?authSource=admin"
    )), Right(AppConfig("::1", 65535,
      "mongodb://test-user:synthetic-secret@localhost:27018/?authSource=admin",
      "hiring_test-2", "WARN", maskSensitive = true, requestPayloads = false)))
  }

  test("P1-AC01 local config overlays application config values") {
    val local =
      """HTTP_PORT=9090
        |LOG_LEVEL=ERROR
        |LOG_MASK_SENSITIVE=false
        |""".stripMargin
    val loaded = AppConfig.fromConfig(defaultConfig, Map.empty)
      .flatMap(base => AppConfig.fromConfig(local, Map.empty, Some(base)))
    assertEquals(loaded.map(config => (config.port, config.logLevel, config.maskSensitive)),
      Right((9090, "ERROR", false)))
  }

  test("P1-AC01 config text rejects malformed lines and missing secret env placeholders safely") {
    assertEquals(AppConfig.fromConfig("HTTP_HOST 127.0.0.1\n", Map.empty), Left(ConfigError.InvalidConfigFile))
    assertEquals(AppConfig.fromConfig("MONGODB_URI={$MONGODB_URI}\n", Map.empty), Left(ConfigError.InvalidMongoUri))
  }

  test("P1-AC01 rejects missing required settings rather than using code defaults") {
    assertEquals(AppConfig.fromConfig(defaultConfig.linesIterator.filterNot(_.startsWith("HTTP_HOST")).mkString("\n"), Map.empty),
      Left(ConfigError.InvalidHost))
    assertEquals(AppConfig.fromConfig("", Map.empty), Left(ConfigError.InvalidHost))
  }

  test("P1-AC01 rejects invalid values without returning their contents") {
    val invalid = List(
      ("HTTP_HOST", List("localhost", "999.1.1.1"), ConfigError.InvalidHost),
      ("HTTP_PORT", List("0", "65536", "-1", "+80", "2147483648"), ConfigError.InvalidPort),
      ("MONGODB_URI", List("https://synthetic-secret", "mongodb://", "mongodb://host:wrong"), ConfigError.InvalidMongoUri),
      ("MONGODB_DATABASE", List("a/b", "a.b", "a b", "a$b", "a\u0000b", "a" * 64), ConfigError.InvalidMongoDatabase),
      ("LOG_LEVEL", List("DEBUG", "info", "synthetic-secret"), ConfigError.InvalidLogLevel)
    )
    invalid.foreach { case (key, values, error) =>
      values.foreach { value =>
        val result = AppConfig.fromConfig(defaultConfig + s"$key=$value\n", Map.empty)
        assertEquals(result, Left(error), clues(key))
        assert(!result.toString.contains("synthetic-secret"))
      }
    }
  }

  test("P1-AC01 accepts boundary ports and all supported severities") {
    List("1", "65535").foreach { port =>
      List("INFO", "WARN", "ERROR").foreach { level =>
        assert(AppConfig.fromConfig(defaultConfig + s"HTTP_PORT=$port\nLOG_LEVEL=$level\n", Map.empty).isRight)
      }
    }
  }

  test("P1-AC01 config rendering never exposes a secret-bearing URI") {
    val result = AppConfig.fromConfig(defaultConfig + "MONGODB_URI=mongodb://user:synthetic-secret@localhost:27017\n", Map.empty)
    assert(result.isRight)
    assert(!result.toString.contains("synthetic-secret"))
    assert(!result.toString.contains("mongodb://"))
  }

  test("LOG-03 secure defaults come from application config") {
    val result = AppConfig.fromConfig(defaultConfig, Map.empty)
    assertEquals(result.map(value => (value.maskSensitive, value.requestPayloads)), Right((true, false)))
  }

  test("LOG-03 explicit unmasking and payload opt-in accept parsed loopback addresses") {
    List("127.0.0.1", "127.0.0.0", "127.25.67.89", "127.255.255.255",
      "::1", "0:0:0:0:0:0:0:1", "0000:0000:0000:0000:0000:0000:0000:0001",
      "::ffff:127.0.0.1").foreach { host =>
      List("false", "true").foreach { payloads =>
        val result = AppConfig.fromConfig(defaultConfig +
          s"HTTP_HOST=$host\nLOG_MASK_SENSITIVE=false\nLOG_REQUEST_PAYLOADS=$payloads\n", Map.empty)
        assertEquals(result.map(value => (value.host, value.maskSensitive, value.requestPayloads)),
          Right((host, false, payloads == "true")), clues(host))
      }
    }
  }

  test("LOG-03 unsafe logging flags require loopback and disabled masking") {
    List("0.0.0.0", "::", "192.0.2.1", "126.255.255.255", "128.0.0.1",
      "::2", "2001:db8::1", "::ffff:192.0.2.1").foreach { host =>
      assertEquals(AppConfig.fromConfig(defaultConfig + s"HTTP_HOST=$host\nLOG_MASK_SENSITIVE=false\n", Map.empty),
        Left(ConfigError.UnsafeMaskSensitive), clues(host))
      assert(AppConfig.fromConfig(defaultConfig + s"HTTP_HOST=$host\n", Map.empty).isRight, clues(host))
    }
    assertEquals(AppConfig.fromConfig(defaultConfig + "LOG_MASK_SENSITIVE=false\n", Map.empty),
      Right(AppConfig("127.0.0.1", 8080, "mongodb://127.0.0.1:27017", "hiring", "INFO",
        maskSensitive = false, requestPayloads = false)))
    assertEquals(AppConfig.fromConfig(defaultConfig + "LOG_REQUEST_PAYLOADS=true\n", Map.empty),
      Left(ConfigError.UnsafeRequestPayloads))
  }

  test("LOG-03 logging booleans are strict with safe configuration keys") {
    List(
      ("LOG_MASK_SENSITIVE", List("TRUE", "0", "synthetic-secret"), ConfigError.InvalidMaskSensitive),
      ("LOG_REQUEST_PAYLOADS", List("FALSE", "1", "synthetic-secret"), ConfigError.InvalidRequestPayloads)
    ).foreach { case (key, values, expected) =>
      values.foreach { value =>
        val result = AppConfig.fromConfig(defaultConfig + s"$key=$value\n", Map.empty)
        assertEquals(result, Left(expected), clues(key))
        assertEquals(expected.key, key)
        assert(!result.toString.contains("synthetic-secret"))
      }
    }
    assertEquals(AppConfig.fromConfig(defaultConfig + "HTTP_HOST=localhost\nLOG_MASK_SENSITIVE=false\n", Map.empty),
      Left(ConfigError.InvalidHost))
  }

  test("LOG-04 configuration remains redacted when local metadata and payload capture are enabled") {
    val result = AppConfig.fromConfig(defaultConfig +
      "LOG_MASK_SENSITIVE=false\nLOG_REQUEST_PAYLOADS=true\nMONGODB_URI=mongodb://user:synthetic-secret@127.0.0.1:1\n",
      Map.empty)
    assert(result.isRight)
    assert(!result.toString.contains("synthetic-secret"))
    assert(!result.toString.contains("mongodb://"))
  }
}
