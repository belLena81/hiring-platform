package com.example.graphQL.cats.config

import munit.FunSuite

class AppConfigSpec extends FunSuite {
  private val defaultConfig =
    """http {
      |  host = "127.0.0.1"
      |  port = 8080
      |  admission-permits = 16
      |}
      |mongo {
      |  uri = "mongodb://127.0.0.1:27017"
      |  database = "hiring"
      |}
      |logging {
      |  level = "INFO"
      |  mask-sensitive = true
      |}
      |auth.jwt {
      |  hs256-secret = "disabled"
      |  issuer = "hiring-platform-local"
      |  audience = "hiring-graphql-api"
      |}
      |vector-search {
      |  enabled = false
      |  voyage {
      |    api-key = "disabled"
      |    endpoint = "https://api.voyageai.com/v1/embeddings"
      |    model = "voyage-4-lite"
      |    dimension = 1024
      |  }
      |  embedding {
      |    version = 1
      |    queue-size = 128
      |    parallelism = 4
      |    timeout-ms = 5000
      |  }
      |  indexes {
      |    jobs = "jobs_embedding_vector"
      |    candidates = "candidates_embedding_vector"
      |  }
      |  num-candidates = 100
      |}
      |""".stripMargin

  private val defaultVectorSearch =
    VectorSearchConfig(
      enabled = false,
      voyageApiKey = None,
      voyageEndpoint = "https://api.voyageai.com/v1/embeddings",
      voyageModel = "voyage-4-lite",
      voyageDimension = 1024,
      embeddingVersion = 1,
      queueSize = 128,
      parallelism = 4,
      timeoutMillis = 5000,
      jobVectorIndex = "jobs_embedding_vector",
      candidateVectorIndex = "candidates_embedding_vector",
      numCandidates = 100
    )
  private val defaultJwtAuth =
    JwtAuthConfig(None, "hiring-platform-local", "hiring-graphql-api")

  test("P1-AC01 loads grouped HOCON settings and resolves env placeholders") {
    val config =
      """http {
        |  host = "::1"
        |  port = 65535
        |  admission-permits = 64
        |}
        |mongo {
        |  uri = ${MONGODB_URI}
        |  database = "hiring_test-2"
        |}
        |logging {
        |  level = "WARN"
        |  mask-sensitive = true
        |}
        |auth.jwt {
        |  hs256-secret = ${AUTH_JWT_HS256_SECRET}
        |  issuer = "hiring-platform-local"
        |  audience = "hiring-graphql-api"
        |}
        |vector-search {
        |  enabled = false
        |  voyage {
        |    api-key = "disabled"
        |    endpoint = "https://api.voyageai.com/v1/embeddings"
        |    model = "voyage-4-lite"
        |    dimension = 1024
        |  }
        |  embedding {
        |    version = 1
        |    queue-size = 128
        |    parallelism = 4
        |    timeout-ms = 5000
        |  }
        |  indexes {
        |    jobs = "jobs_embedding_vector"
        |    candidates = "candidates_embedding_vector"
        |  }
        |  num-candidates = 100
        |}
        |""".stripMargin
    assertEquals(AppConfig.fromConfig(config, Map(
      "MONGODB_URI" -> "mongodb://test-user:synthetic-secret@localhost:27018/?authSource=admin",
      "AUTH_JWT_HS256_SECRET" -> "01234567890123456789012345678901"
    )), Right(AppConfig("::1", 65535, 64,
      "mongodb://test-user:synthetic-secret@localhost:27018/?authSource=admin",
      "hiring_test-2", maskSensitive = true,
      JwtAuthConfig(Some("01234567890123456789012345678901"), "hiring-platform-local", "hiring-graphql-api"),
      defaultVectorSearch)))
  }

  test("VHS-AC07 rejects a vector candidate budget below the maximum page size") {
    val config = defaultConfig.replace("num-candidates = 100", "num-candidates = 99")

    assertEquals(AppConfig.fromConfig(config, Map.empty), Left(ConfigError.InvalidVectorNumCandidates))
  }

  test("VHS-AC08 packaged application config is grouped, sanitized and fails safely without required local values") {
    val raw = resource("application.conf")
    assertEquals(AppConfig.fromConfig(raw, Map.empty), Left(ConfigError.InvalidHost))
    assert(raw.contains("http {"))
    assert(raw.contains("uri = ${?MONGODB_URI}"))
    assert(raw.contains("hs256-secret = ${?AUTH_JWT_HS256_SECRET}"))
    assert(!raw.contains("mongodb+srv://"))
    assert(!raw.contains("synthetic-secret"))
    assert(!raw.contains(" //"))
  }

  test("VHS-AC08 packaged application config loads with ignored local run values") {
    val raw = resource("application.conf")
    val local =
      """http {
        |  host = "127.0.0.1"
        |  port = 8080
        |  admission-permits = 16
        |}
        |mongo {
        |  uri = "mongodb://127.0.0.1:27017"
        |}
        |""".stripMargin
    val result = AppConfig.fromRawConfig(raw, local, _ => None)
    assertEquals(result.map(config => (config.host, config.port, config.admissionPermits, config.mongoUri)),
      Right(("127.0.0.1", 8080, 16, "mongodb://127.0.0.1:27017")))
  }

  test("VHS-AC08 packaged application config resolves cloud environment values at startup") {
    val raw = resource("application.conf")
    val result = AppConfig.fromConfig(raw, Map(
      "HTTP_HOST" -> "::1",
      "HTTP_PORT" -> "9090",
      "HTTP_ADMISSION_PERMITS" -> "96",
      "MONGODB_URI" -> "mongodb://127.0.0.1:27018",
      "VOYAGE_MODEL" -> "voyage-4-lite"
    ))
    assertEquals(result.map(config => (config.host, config.port, config.admissionPermits, config.mongoUri)),
      Right(("::1", 9090, 96, "mongodb://127.0.0.1:27018")))
  }

  test("CFG-AC02 injected env resolution ignores process system properties") {
    val previous = Option(System.getProperty("HTTP_HOST"))
    System.setProperty("HTTP_HOST", "::1")
    try {
      val config =
        defaultConfig.replace("host = \"127.0.0.1\"", "host = ${?HTTP_HOST}")
      assertEquals(AppConfig.fromConfig(config, Map.empty), Left(ConfigError.InvalidHost))
      assert(AppConfig.fromConfig(config, Map("HTTP_HOST" -> "::1")).isRight)
    } finally {
      previous.fold {
        val _ = System.clearProperty("HTTP_HOST")
        ()
      } { value =>
        val _ = System.setProperty("HTTP_HOST", value)
        ()
      }
    }
  }

  test("P1-AC01 local config overlays application config values") {
    val local =
      """http.port = 9090
        |logging.mask-sensitive = false
        |""".stripMargin
    val loaded = AppConfig.fromConfig(defaultConfig, Map.empty)
      .flatMap(base => AppConfig.fromConfig(local, Map.empty, Some(base)))
    assertEquals(loaded.map(config => (config.port, config.maskSensitive)), Right((9090, false)))
  }

  test("P1-AC01 local config overrides deploy defaults before env resolution") {
    val defaults =
      """http {
        |  host = "127.0.0.1"
        |  host = ${?HTTP_HOST}
        |  port = 8080
        |  port = ${?HTTP_PORT}
        |  admission-permits = 16
        |  admission-permits = ${?HTTP_ADMISSION_PERMITS}
        |}
        |mongo {
        |  uri = "mongodb://127.0.0.1:27017"
        |  uri = ${?MONGODB_URI}
        |  database = "hiring"
        |}
        |logging {
        |  level = "INFO"
        |  mask-sensitive = true
        |}
        |auth.jwt {
        |  hs256-secret = "disabled"
        |  hs256-secret = ${?AUTH_JWT_HS256_SECRET}
        |  issuer = "hiring-platform-local"
        |  audience = "hiring-graphql-api"
        |}
        |vector-search {
        |  enabled = false
        |  voyage {
        |    api-key = "disabled"
        |    api-key = ${?VOYAGE_API_KEY}
        |    endpoint = "https://api.voyageai.com/v1/embeddings"
        |    model = "voyage-4-lite"
        |    model = ${?VOYAGE_MODEL}
        |    dimension = 1024
        |  }
        |  embedding {
        |    version = 1
        |    queue-size = 128
        |    parallelism = 4
        |    timeout-ms = 5000
        |  }
        |  indexes {
        |    jobs = "jobs_embedding_vector"
        |    candidates = "candidates_embedding_vector"
        |  }
        |  num-candidates = 100
        |}
        |""".stripMargin
    val local =
      """http.host = "127.42.10.8"
        |http.port = 9091
        |mongo.uri = "mongodb://127.0.0.1:27018"
        |auth.jwt.hs256-secret = "disabled"
        |vector-search.voyage.api-key = "disabled"
        |vector-search.voyage.model = "voyage-4-lite"
        |""".stripMargin

    assertEquals(AppConfig.fromRawConfig(defaults, local, _ => None).map(config =>
      (config.host, config.port, config.admissionPermits, config.mongoUri)),
      Right(("127.42.10.8", 9091, 16, "mongodb://127.0.0.1:27018")))
  }

  test("P1-AC01 config text rejects malformed HOCON and missing required paths safely") {
    assertEquals(AppConfig.fromConfig("http { host = 127.0.0.1\n", Map.empty), Left(ConfigError.InvalidConfigFile))
    assertEquals(AppConfig.fromConfig("mongo.uri=${MONGODB_URI}\n", Map.empty), Left(ConfigError.InvalidConfigFile))
    assertEquals(AppConfig.fromConfig(defaultConfig.replace("  host = \"127.0.0.1\"\n", ""), Map.empty),
      Left(ConfigError.InvalidHost))
    assertEquals(AppConfig.fromConfig(defaultConfig.replace("  uri = \"mongodb://127.0.0.1:27017\"\n", ""), Map.empty),
      Left(ConfigError.InvalidMongoUri))
    assertEquals(AppConfig.fromConfig(defaultConfig + "mongo.database = \"a\u0000b\"\n", Map.empty),
      Left(ConfigError.InvalidConfigFile))
    assertEquals(AppConfig.fromConfig("", Map.empty), Left(ConfigError.InvalidConfigFile))
  }

  test("P1-AC01 rejects invalid values without returning their contents") {
    val invalid = List(
      ("http.host", List("localhost", "999.1.1.1"), ConfigError.InvalidHost),
      ("http.port", List("0", "65536", "-1", "+80", "2147483648"), ConfigError.InvalidPort),
      ("http.admission-permits", List("0", "1025", "-1", "+16", "synthetic-secret"), ConfigError.InvalidAdmissionPermits),
      ("mongo.uri", List("https://synthetic-secret", "mongodb://", "mongodb://host:wrong"), ConfigError.InvalidMongoUri),
      ("mongo.database", List("a/b", "a.b", "a b", "a$b", "a" * 64), ConfigError.InvalidMongoDatabase)
    )
    invalid.foreach { case (key, values, error) =>
      values.foreach { value =>
        val result = AppConfig.fromConfig(defaultConfig + s"""$key = "$value"\n""", Map.empty)
        assertEquals(result, Left(error), clues(key))
        assert(!result.toString.contains("synthetic-secret"))
      }
    }
  }

  test("P1-AC01 accepts boundary ports without application logging levels") {
    List("1", "65535").foreach { port =>
      assert(AppConfig.fromConfig(defaultConfig + s"http.port = $port\n", Map.empty).isRight)
    }
    List("1", "1024").foreach { permits =>
      assert(AppConfig.fromConfig(defaultConfig + s"http.admission-permits = $permits\n", Map.empty).isRight)
    }
  }

  test("P1-AC01 config rendering never exposes a secret-bearing URI") {
    val result = AppConfig.fromConfig(defaultConfig +
      """mongo.uri = "mongodb://user:synthetic-secret@localhost:27017"
        |""".stripMargin, Map.empty)
    assert(result.isRight)
    assert(!result.toString.contains("synthetic-secret"))
    assert(!result.toString.contains("mongodb://"))
  }

  test("LOG-03 secure defaults come from application config") {
    val result = AppConfig.fromConfig(defaultConfig, Map.empty)
    assertEquals(result.map(_.maskSensitive), Right(true))
  }

  test("LOG-03 explicit unmasking accepts parsed loopback addresses") {
    List("127.0.0.1", "127.0.0.0", "127.25.67.89", "127.255.255.255",
      "::1", "0:0:0:0:0:0:0:1", "0000:0000:0000:0000:0000:0000:0000:0001",
      "::ffff:127.0.0.1").foreach { host =>
      val result = AppConfig.fromConfig(defaultConfig +
        s"""http.host = "$host"\nlogging.mask-sensitive = false\n""", Map.empty)
      assertEquals(result.map(value => (value.host, value.maskSensitive)), Right((host, false)), clues(host))
    }
  }

  test("LOG-03 masking policy is independent of the bind address") {
    List("0.0.0.0", "::", "192.0.2.1", "126.255.255.255", "128.0.0.1",
      "::2", "2001:db8::1", "::ffff:192.0.2.1").foreach { host =>
      assertEquals(AppConfig.fromConfig(defaultConfig + s"""http.host = "$host"\nlogging.mask-sensitive = false\n""", Map.empty)
        .map(_.maskSensitive), Right(false), clues(host))
      assert(AppConfig.fromConfig(defaultConfig + s"""http.host = "$host"\n""", Map.empty).isRight, clues(host))
    }
    assertEquals(AppConfig.fromConfig(defaultConfig + "logging.mask-sensitive = false\n", Map.empty),
      Right(AppConfig("127.0.0.1", 8080, 16, "mongodb://127.0.0.1:27017", "hiring",
        maskSensitive = false, defaultJwtAuth, defaultVectorSearch.copy(enabled = false))))
  }

  test("VHS-AC08 vector search requires an explicit Voyage API key when enabled") {
    assertEquals(AppConfig.fromConfig(defaultConfig + "vector-search.enabled = true\n", Map.empty),
      Left(ConfigError.InvalidVoyageApiKey))
    val result = AppConfig.fromConfig(defaultConfig +
      """vector-search.enabled = true
        |vector-search.voyage.api-key = ${VOYAGE_API_KEY}
        |""".stripMargin, Map("VOYAGE_API_KEY" -> "synthetic-voyage-key"))
    assertEquals(result.map(_.vectorSearch.voyageApiKey), Right(Some("synthetic-voyage-key")))
  }

  test("HGQL-AC02 JWT auth config is disabled by default and requires a strong external secret when enabled") {
    assertEquals(AppConfig.fromConfig(defaultConfig, Map.empty).map(_.jwtAuth), Right(defaultJwtAuth))
    assertEquals(AppConfig.fromConfig(defaultConfig + "auth.jwt.hs256-secret = \"short\"\n", Map.empty),
      Left(ConfigError.InvalidJwtSecret))
    val loaded = AppConfig.fromConfig(defaultConfig + "auth.jwt.hs256-secret = ${AUTH_JWT_HS256_SECRET}\n",
      Map("AUTH_JWT_HS256_SECRET" -> "abcdefghijklmnopqrstuvwxyz123456"))
    assertEquals(loaded.map(_.jwtAuth.hmacSecret), Right(Some("abcdefghijklmnopqrstuvwxyz123456")))
  }

  test("VHS-AC07 vector search dimension is fixed to the configured Atlas index contract") {
    List("256", "512", "2048").foreach { dimension =>
      assertEquals(AppConfig.fromConfig(defaultConfig + s"vector-search.voyage.dimension = $dimension\n", Map.empty),
        Left(ConfigError.InvalidVoyageDimension), clues(dimension))
    }
    assert(AppConfig.fromConfig(defaultConfig + "vector-search.voyage.dimension = 1024\n", Map.empty).isRight)
  }

  test("LOG-03 logging booleans are strict with safe configuration keys") {
    List("TRUE", "0", "synthetic-secret").foreach { value =>
      val result = AppConfig.fromConfig(defaultConfig + s"""logging.mask-sensitive = "$value"\n""", Map.empty)
      assertEquals(result, Left(ConfigError.InvalidMaskSensitive))
      assert(!result.toString.contains("synthetic-secret"))
    }
    assertEquals(AppConfig.fromConfig(defaultConfig + "http.host = \"localhost\"\nlogging.mask-sensitive = false\n", Map.empty),
      Left(ConfigError.InvalidHost))
  }

  test("LOG-04 configuration remains redacted when local metadata is enabled") {
    val result = AppConfig.fromConfig(defaultConfig +
      """logging.mask-sensitive = false
        |mongo.uri = "mongodb://user:synthetic-secret@127.0.0.1:1"
        |""".stripMargin,
      Map.empty)
    assert(result.isRight)
    assert(!result.toString.contains("synthetic-secret"))
    assert(!result.toString.contains("mongodb://"))
  }

  private def resource(name: String): String = {
    val stream = Option(getClass.getClassLoader.getResourceAsStream(name))
      .getOrElse(throw new IllegalArgumentException(s"Missing resource $name"))
    try new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
    finally stream.close()
  }
}
