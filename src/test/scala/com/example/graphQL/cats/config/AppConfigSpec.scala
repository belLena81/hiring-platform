package com.example.graphQL.cats.config

import com.example.graphQL.cats.domain.pagination.PageSize
import com.comcast.ip4s.{Host, Port as Ip4sPort}
import munit.FunSuite
import cats.data.NonEmptyList
import scala.concurrent.duration.*

class AppConfigSpec extends FunSuite {
  private def assertContainsError(result: Either[NonEmptyList[ConfigError], AppConfig], expected: ConfigError): Unit =
    result match {
      case Left(issues) => assert(issues.toList.contains(expected), clues(expected))
      case other        => fail(s"Expected aggregated configuration error containing $expected, got $other")
    }

  private def assertInvalidConfig(result: Either[NonEmptyList[ConfigError], AppConfig]): Unit =
    result match {
      case Left(issues) => assert(issues.toList.exists(_.key == "CONFIG_FILE"), clues(issues))
      case other        => fail(s"Expected invalid configuration result, got $other")
    }

  test("admin seed defaults disabled and requires credentials only when enabled") {
    assertEquals(AppConfigFixtures.fromConfig(defaultConfig, Map.empty).map(_.adminSeed), Right(AdminSeedConfig()))
    assertContainsError(
      AppConfigFixtures.fromConfig(defaultConfig + "\nauth.admin-seed.enabled=true", Map.empty),
      ConfigError.InvalidAdminSeed
    )
    val enabled = defaultConfig + "\nauth.admin-seed { enabled=true, name=Admin, password=synthetic-password }"
    assertEquals(AppConfigFixtures.fromConfig(enabled, Map.empty).map(_.adminSeed.enabled), Right(true))
    assert(!AdminSeedConfig(true, Some("private-name"), Some("private-password")).toString.contains("private"))
  }

  private val defaultConfig =
    """http {
      |  host = "127.0.0.1"
      |  port = 8080
      |  admission-permits = 16
      |  request-timeout-ms = 5000
      |  trusted-proxy-cidrs = []
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
      |  hs256-secret = "01234567890123456789012345678901"
      |  issuer = "hiring-platform-local"
      |  audience = "hiring-graphql-api"
      |  cursor-ttl-seconds = 900
      |}
      |auth.rate-limit {
      |  window-seconds = 60
      |  attempts = 20
      |  max-buckets = 10000
      |}
      |auth.interview-action-rate-limit {
      |  window-seconds = 60
      |  attempts = 30
      |  max-buckets = 10000
      |}
      |kafka {
      |  enabled = false
      |  bootstrap-servers = "127.0.0.1:9092"
      |  topic = "hiring.operational-events.v1"
      |  consumer-group = "hiring-phase5-consumer"
      |  publisher {
      |    worker-id = "local-publisher"
      |    batch-size = 25
      |    lease-seconds = 30
      |    retry-delay-seconds = 5
      |    max-attempts = 10
      |    poll-interval-ms = 500
      |    sasl-username = ${?KAFKA_PUBLISHER_USERNAME}
      |    sasl-password = ${?KAFKA_PUBLISHER_V2_PASSWORD}
      |  }
      |  consumer {
      |    enabled = false
      |    receipt-ttl-days = 8
      |    quarantine-ttl-days = 7
      |    sasl-username = ${?KAFKA_READER_USERNAME}
      |    sasl-password = ${?KAFKA_READER_PASSWORD}
      |  }
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
      |    queue-size = 128
      |    parallelism = 4
      |    timeout-ms = 5000
      |    retry-attempts = 3
      |    retry-delay-ms = 250
      |  }
      |  indexes {
      |    jobs = "jobs_embedding_vector"
      |    candidates = "candidates_embedding_vector_match_v1"
      |    lexical = "jobs_text_search"
      |    candidate-lexical = "candidates_text_search"
      |    ready-timeout-ms = 120000
      |    poll-interval-ms = 1000
      |  }
      |  num-candidates = 100
      |  fusion-strategy = "applicationRrf"
      |  rerank {
      |    enabled = false
      |    model = "rerank-2.5-lite"
      |  }
      |}
      |""".stripMargin

  private val defaultVectorSearch: VectorSearchConfig = VectorSearchConfig.Disabled

  /** The default configuration with vector search enabled; `extra` overrides are appended. */
  private def enabledVectorSearch(extra: String = ""): VectorSearchConfig.Enabled =
    AppConfigFixtures
      .fromConfig(
        defaultConfig + "vector-search.enabled = true\nvector-search.voyage.api-key = \"synthetic-voyage-key\"\n" + extra,
        Map.empty
      )
      .map(_.vectorSearch) match {
      case Right(enabled: VectorSearchConfig.Enabled) => enabled
      case other                                      => fail(s"expected enabled vector search, got $other")
    }

  private val defaultJwtAuth =
    JwtAuthConfig("01234567890123456789012345678901", "hiring-platform-local", "hiring-graphql-api")
  private val defaultPasswordHash =
    PasswordHashConfig(iterations = 2, memoryKilobytes = 19456, parallelism = 1)
  private val defaultAuthRateLimit =
    AuthRateLimitConfig(windowSeconds = 60, attempts = 20, maxBuckets = 10000)
  private val defaultKafka =
    KafkaConfig(
      enabled = false,
      bootstrapServers = "127.0.0.1:9092",
      topic = "hiring.operational-events.v1",
      consumerGroup = "hiring-phase5-consumer",
      publisher = KafkaPublisherConfig(
        workerId = "local-publisher",
        batchSize = 25,
        leaseSeconds = 30,
        retryDelaySeconds = 5,
        maxAttempts = 10,
        pollIntervalMillis = 500
      ),
      consumer = KafkaConsumerConfig(enabled = false, receiptTtlDays = 8, quarantineTtlDays = 7)
    )

  test("an absent interview configuration uses disabled local defaults") {
    val parsed = AppConfigFixtures.fromConfig(defaultConfig, Map.empty)
    assertEquals(parsed.map(_.kafka.interview), Right(InterviewRuntimeConfig()))
  }

  test("Kafka publisher and reader credentials are resolved independently") {
    val enabled = defaultConfig.replace("kafka {\n  enabled = false", "kafka {\n  enabled = true")
    val parsed = AppConfigFixtures.fromConfig(
      enabled,
      Map(
        "KAFKA_PUBLISHER_USERNAME" -> "hiring_publisher_v2",
        "KAFKA_PUBLISHER_V2_PASSWORD" -> "publisher-v2-secret",
        "KAFKA_READER_USERNAME" -> "analytics_reader",
        "KAFKA_READER_PASSWORD" -> "reader-secret"
      )
    )
    assertEquals(parsed.map(_.kafka.publisher.saslUsername), Right(Some("hiring_publisher_v2")))
    assertEquals(parsed.map(_.kafka.publisher.saslPassword), Right(Some("publisher-v2-secret")))
    assertEquals(parsed.map(_.kafka.consumer.saslUsername), Right(Some("analytics_reader")))
    assertEquals(parsed.map(_.kafka.consumer.saslPassword), Right(Some("reader-secret")))
  }

  test("Kafka SASL transport defaults to TLS and supports explicit local plaintext") {
    val tls = AppConfigFixtures.fromConfig(defaultConfig, Map.empty)
    val plaintext = AppConfigFixtures.fromConfig(
      defaultConfig + "kafka.sasl-security-protocol = ${?KAFKA_SASL_SECURITY_PROTOCOL}\n",
      Map("KAFKA_SASL_SECURITY_PROTOCOL" -> "SASL_PLAINTEXT")
    )

    assertEquals(tls.map(_.kafka.saslSecurityProtocol), Right(KafkaSaslSecurityProtocol.Tls))
    assertEquals(plaintext.map(_.kafka.saslSecurityProtocol), Right(KafkaSaslSecurityProtocol.Plaintext))
  }

  test("Kafka SASL transport rejects unsupported protocols") {
    val config = defaultConfig + "kafka.sasl-security-protocol = \"PLAINTEXT\"\n"
    assertContainsError(AppConfigFixtures.fromConfig(config, Map.empty), ConfigError.InvalidKafkaSaslSecurityProtocol)
  }

  test("Kafka SASL plaintext is rejected for non-loopback bootstrap servers") {
    val config = defaultConfig +
      "kafka.bootstrap-servers = \"broker.example:9092\"\n" +
      "kafka.sasl-security-protocol = \"SASL_PLAINTEXT\"\n"

    assertContainsError(AppConfigFixtures.fromConfig(config, Map.empty), ConfigError.InvalidKafkaSaslSecurityProtocol)
  }

  test("enabled Kafka rejects missing authentication credentials") {
    val enabled = defaultConfig.replace("kafka {\n  enabled = false", "kafka {\n  enabled = true")
    assertContainsError(AppConfigFixtures.fromConfig(enabled, Map.empty), ConfigError.InvalidKafkaCredentials)
  }

  test("P1-AC01 loads grouped HOCON settings and resolves env placeholders") {
    val config =
      """http {
        |  host = "::1"
        |  port = 65535
        |  admission-permits = 64
        |  request-timeout-ms = 5000
        |  trusted-proxy-cidrs = []
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
        |  cursor-ttl-seconds = 900
        |}
        |auth.rate-limit {
        |  window-seconds = 30
        |  attempts = 10
        |  max-buckets = 500
        |}
        |auth.interview-action-rate-limit {
        |  window-seconds = 60
        |  attempts = 30
        |  max-buckets = 10000
        |}
        |kafka {
        |  enabled = false
        |  bootstrap-servers = "127.0.0.1:9092"
        |  topic = "hiring.operational-events.v1"
        |  consumer-group = "hiring-phase5-consumer"
        |  publisher {
        |    worker-id = "local-publisher"
        |    batch-size = 25
        |    lease-seconds = 30
        |    retry-delay-seconds = 5
      |    max-attempts = 10
      |    poll-interval-ms = 500
      |    sasl-username = ${?KAFKA_PUBLISHER_USERNAME}
      |    sasl-password = ${?KAFKA_PUBLISHER_V2_PASSWORD}
      |  }
      |  consumer {
        |    enabled = false
        |    receipt-ttl-days = 8
      |    quarantine-ttl-days = 7
      |    sasl-username = ${?KAFKA_READER_USERNAME}
      |    sasl-password = ${?KAFKA_READER_PASSWORD}
        |  }
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
        |    queue-size = 128
        |    parallelism = 4
        |    timeout-ms = 5000
        |    retry-attempts = 3
        |    retry-delay-ms = 250
        |  }
        |  indexes {
        |    jobs = "jobs_embedding_vector"
        |    candidates = "candidates_embedding_vector_match_v1"
        |    lexical = "jobs_text_search"
        |    candidate-lexical = "candidates_text_search"
        |    ready-timeout-ms = 120000
        |    poll-interval-ms = 1000
        |  }
        |  num-candidates = 100
        |  fusion-strategy = "applicationRrf"
        |  rerank {
        |    enabled = false
        |    model = "rerank-2.5-lite"
        |  }
        |}
        |""".stripMargin
    assertEquals(
      AppConfigFixtures.fromConfig(
        config,
        Map(
          "MONGODB_URI" -> "mongodb://test-user:synthetic-secret@localhost:27018/?authSource=admin",
          "AUTH_JWT_HS256_SECRET" -> "01234567890123456789012345678901"
        )
      ),
      Right(
        AppConfig(
          Host.fromString("::1").get,
          Ip4sPort.fromInt(65535).get,
          64,
          5.seconds,
          TrustedProxyConfig(Nil),
          "mongodb://test-user:synthetic-secret@localhost:27018/?authSource=admin",
          "hiring_test-2",
          true,
          JwtAuthConfig("01234567890123456789012345678901", "hiring-platform-local", "hiring-graphql-api"),
          defaultPasswordHash,
          AuthRateLimitConfig(30, 10, 500),
          defaultVectorSearch,
          defaultKafka,
          interviewActionRateLimit = InterviewActionRateLimitConfig(60, 30, 10000)
        )
      )
    )
  }

  test("VHS-AC07 rejects a vector candidate budget below the maximum page size") {
    val config = defaultConfig.replace("num-candidates = 100", "num-candidates = 99")

    assertContainsError(AppConfigFixtures.fromConfig(config, Map.empty), ConfigError.InvalidVectorNumCandidates)
  }

  test("Mongo startup reset is disabled by default and opt-in") {
    assertEquals(AppConfigFixtures.fromConfig(defaultConfig, Map.empty).map(_.resetOnStart), Right(false))
    assertEquals(
      AppConfigFixtures.fromConfig(defaultConfig + "mongo.reset-on-start = true\n", Map.empty).map(_.resetOnStart),
      Right(true)
    )
  }

  test("VHS-AC08 packaged application config is grouped, sanitized and fails safely without required local values") {
    val raw = resource("application.conf")
    assertInvalidConfig(AppConfigFixtures.fromConfig(raw, Map.empty))
    assert(raw.contains("http {"))
    assert(raw.contains("uri = ${?MONGODB_URI}"))
    assert(raw.contains("hs256-secret = ${?AUTH_JWT_HS256_SECRET}"))
    assert(!raw.contains("mongodb+srv://"))
    assert(!raw.contains("synthetic-secret"))
    assert(!raw.contains(" //"))
  }

  test("VHS-AC08 HOCON values from a local source override packaged values") {
    val raw = resource("application.conf")
    val local =
      """http {
        |  host = "127.0.0.1"
        |  port = 8080
        |  admission-permits = 16
        |  request-timeout-ms = 5000
        |  trusted-proxy-cidrs = []
        |}
        |mongo {
        |  uri = "mongodb://127.0.0.1:27017"
        |}
        |auth.jwt.hs256-secret = "01234567890123456789012345678901"
        |""".stripMargin
    val result = AppConfigFixtures.fromConfig(raw + local, Map.empty)
    assertEquals(
      result.map(config => (config.host.toString, config.port.value, config.admissionPermits, config.mongoUri)),
      Right(("127.0.0.1", 8080, 16, "mongodb://127.0.0.1:27017"))
    )
  }

  test("Kafka scalar string limits accept their boundary and classify invalid values") {
    val atLimits = defaultConfig
      .replace("127.0.0.1:9092", "b" * 512)
      .replace("hiring.operational-events.v1", "t" * 249)
      .replace("hiring-phase5-consumer", "g" * 249)
    assert(AppConfigFixtures.fromConfig(atLimits, Map.empty).isRight)

    val invalidValues = List(
      ("127.0.0.1:9092", "b" * 513, ConfigError.InvalidKafkaBootstrapServers),
      ("hiring.operational-events.v1", "t" * 250, ConfigError.InvalidKafkaTopic),
      ("hiring-phase5-consumer", " ", ConfigError.InvalidKafkaConsumerGroup)
    )
    invalidValues.foreach { case (existing, invalid, expected) =>
      assertContainsError(AppConfigFixtures.fromConfig(defaultConfig.replace(existing, invalid), Map.empty), expected)
    }
  }

  test("VHS-AC08 packaged application config resolves cloud environment values at startup") {
    val raw = resource("application.conf")
    val result = AppConfigFixtures.fromConfig(
      raw,
      Map(
        "HTTP_HOST" -> "::1",
        "HTTP_PORT" -> "9090",
        "HTTP_ADMISSION_PERMITS" -> "96",
        "HTTP_REQUEST_TIMEOUT_MS" -> "30000",
        "MONGODB_URI" -> "mongodb://127.0.0.1:27018",
        "AUTH_JWT_HS256_SECRET" -> "01234567890123456789012345678901",
        "VOYAGE_MODEL" -> "voyage-4-lite"
      )
    )
    assertEquals(
      result.map(config => (config.host.toString, config.port.value, config.admissionPermits, config.mongoUri)),
      Right(("::1", 9090, 96, "mongodb://127.0.0.1:27018"))
    )
    assertEquals(result.map(_.requestTimeout), Right(30.seconds))
  }

  test("packaged application config names the environment variable it reads for the timeout and restart delay") {
    val raw = resource("application.conf")
    val base = Map("AUTH_JWT_HS256_SECRET" -> "01234567890123456789012345678901", "VOYAGE_MODEL" -> "voyage-4-lite")
    def keys(extra: (String, String)) =
      AppConfigFixtures.fromConfig(raw, base + extra).swap.toOption.toList.flatMap(_.toList.map(_.key))
    assert(keys("HTTP_REQUEST_TIMEOUT_MS" -> "99999999").contains("HTTP_REQUEST_TIMEOUT_MS"))
    assertEquals(ConfigError.InvalidKafkaRestartMaxDelay.key, "HIRING_KAFKA_RESTART_MAX_DELAY_SECONDS")
    assert(raw.contains("${?HIRING_KAFKA_RESTART_MAX_DELAY_SECONDS}"))
  }

  test("CFG-AC02 injected env resolution ignores process system properties") {
    val previous = Option(System.getProperty("HTTP_HOST"))
    System.setProperty("HTTP_HOST", "::1")
    try {
      val config =
        defaultConfig.replace("host = \"127.0.0.1\"", "host = ${?HTTP_HOST}")
      assertInvalidConfig(AppConfigFixtures.fromConfig(config, Map.empty))
      assert(AppConfigFixtures.fromConfig(config, Map("HTTP_HOST" -> "::1")).isRight)
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

  test("P1-AC01 later HOCON values override earlier values") {
    val local =
      """http.port = 9090
        |logging.mask-sensitive = false
        |""".stripMargin
    val loaded = AppConfigFixtures.fromConfig(defaultConfig + local, Map.empty)
    assertEquals(loaded.map(config => (config.port.value, config.maskSensitive)), Right((9090, false)))
  }

  test("P1-AC01 HOCON overrides apply before environment resolution") {
    val defaults =
      """http {
        |  host = "127.0.0.1"
        |  host = ${?HTTP_HOST}
        |  port = 8080
        |  port = ${?HTTP_PORT}
        |  admission-permits = 16
        |  admission-permits = ${?HTTP_ADMISSION_PERMITS}
        |  request-timeout-ms = 5000
        |  trusted-proxy-cidrs = []
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
        |  cursor-ttl-seconds = 900
        |}
        |auth.rate-limit {
        |  window-seconds = 60
        |  attempts = 20
        |  max-buckets = 10000
        |}
        |auth.interview-action-rate-limit {
        |  window-seconds = 60
        |  attempts = 30
        |  max-buckets = 10000
        |}
        |kafka {
        |  enabled = false
        |  bootstrap-servers = "127.0.0.1:9092"
        |  topic = "hiring.operational-events.v1"
        |  consumer-group = "hiring-phase5-consumer"
        |  publisher {
        |    worker-id = "local-publisher"
        |    batch-size = 25
        |    lease-seconds = 30
        |    retry-delay-seconds = 5
        |    max-attempts = 10
        |    poll-interval-ms = 500
        |  }
        |  consumer {
        |    enabled = false
        |    receipt-ttl-days = 8
        |    quarantine-ttl-days = 7
        |  }
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
        |    queue-size = 128
        |    parallelism = 4
        |    timeout-ms = 5000
        |    retry-attempts = 3
        |    retry-delay-ms = 250
        |  }
        |  indexes {
        |    jobs = "jobs_embedding_vector"
        |    candidates = "candidates_embedding_vector_match_v1"
        |    lexical = "jobs_text_search"
        |    candidate-lexical = "candidates_text_search"
        |    ready-timeout-ms = 120000
        |    poll-interval-ms = 1000
        |  }
        |  num-candidates = 100
        |  fusion-strategy = "applicationRrf"
        |  rerank {
        |    enabled = false
        |    model = "rerank-2.5-lite"
        |  }
        |}
        |""".stripMargin
    val local =
      """http.host = "127.42.10.8"
        |http.port = 9091
        |mongo.uri = "mongodb://127.0.0.1:27018"
        |auth.jwt.hs256-secret = "01234567890123456789012345678901"
        |vector-search.voyage.api-key = "disabled"
        |vector-search.voyage.model = "voyage-4-lite"
        |""".stripMargin

    assertEquals(
      AppConfigFixtures
        .fromConfig(defaults + local, Map.empty)
        .map(config => (config.host.toString, config.port.value, config.admissionPermits, config.mongoUri)),
      Right(("127.42.10.8", 9091, 16, "mongodb://127.0.0.1:27018"))
    )
  }

  test("P1-AC01 config text rejects malformed HOCON and missing required paths safely") {
    assertInvalidConfig(AppConfigFixtures.fromConfig("http { host = 127.0.0.1\n", Map.empty))
    assertInvalidConfig(AppConfigFixtures.fromConfig("mongo.uri=${MONGODB_URI}\n", Map.empty))
    assertInvalidConfig(AppConfigFixtures.fromConfig(defaultConfig.replace("  host = \"127.0.0.1\"\n", ""), Map.empty))
    assertInvalidConfig(
      AppConfigFixtures.fromConfig(defaultConfig.replace("  uri = \"mongodb://127.0.0.1:27017\"\n", ""), Map.empty)
    )
    assertInvalidConfig(AppConfigFixtures.fromConfig(defaultConfig + "mongo.database = \"a\u0000b\"\n", Map.empty))
    assertInvalidConfig(AppConfigFixtures.fromConfig("", Map.empty))
  }

  test("P1-AC01 rejects invalid values without returning their contents") {
    val invalid = List(
      ("http.host", List("not-an-ip"), ConfigError.InvalidHost),
      ("http.port", List("0", "65536", "-1", "2147483648"), ConfigError.InvalidPort),
      ("http.admission-permits", List("0", "1025", "-1", "synthetic-secret"), ConfigError.InvalidAdmissionPermits),
      (
        "mongo.uri",
        List("https://synthetic-secret", "mongodb://", "mongodb://host:wrong"),
        ConfigError.InvalidMongoUri
      ),
      ("mongo.database", List("a/b", "a.b", "a b", "a$b", "a" * 64), ConfigError.InvalidMongoDatabase)
    )
    invalid.foreach { case (key, values, error) =>
      values.foreach { value =>
        val rendered = if (key == "http.port" || key == "http.admission-permits") then s"$key = $value"
        else key + " = \"" + value + "\""
        val result = AppConfigFixtures.fromConfig(defaultConfig + rendered + "\n", Map.empty)
        assertContainsError(result, error)
        assert(!result.toString.contains("synthetic-secret"))
      }
    }
  }

  test("P1-AC01 accepts boundary ports without application logging levels") {
    List("1", "65535").foreach { port =>
      assert(AppConfigFixtures.fromConfig(defaultConfig + s"http.port = $port\n", Map.empty).isRight)
    }
    List("1", "1024").foreach { permits =>
      assert(AppConfigFixtures.fromConfig(defaultConfig + s"http.admission-permits = $permits\n", Map.empty).isRight)
    }
  }

  test("P1-AC01 config rendering never exposes a secret-bearing URI") {
    val result = AppConfigFixtures.fromConfig(
      defaultConfig +
        """mongo.uri = "mongodb://user:synthetic-secret@localhost:27017"
        |""".stripMargin,
      Map.empty
    )
    assert(result.isRight)
    assert(!result.toString.contains("synthetic-secret"))
    assert(!result.toString.contains("mongodb://"))
  }

  test("LOG-03 secure defaults come from application config") {
    val result = AppConfigFixtures.fromConfig(defaultConfig, Map.empty)
    assertEquals(result.map(_.maskSensitive), Right(true))
  }

  test("LOG-03 explicit unmasking accepts parsed loopback addresses") {
    List(
      "127.0.0.1",
      "127.0.0.0",
      "127.25.67.89",
      "127.255.255.255",
      "::1",
      "0:0:0:0:0:0:0:1",
      "0000:0000:0000:0000:0000:0000:0000:0001",
      "::ffff:127.0.0.1"
    ).foreach { host =>
      val result = AppConfigFixtures.fromConfig(
        defaultConfig +
          s"""http.host = "$host"\nlogging.mask-sensitive = false\n""",
        Map.empty
      )
      assertEquals(
        result.map(value => (value.host.toString, value.maskSensitive)),
        Right((Host.fromString(host).get.toString, false)),
        clues(host)
      )
    }
  }

  test("LOG-03 masking policy is independent of the bind address") {
    List("0.0.0.0", "::", "192.0.2.1", "126.255.255.255", "128.0.0.1", "::2", "2001:db8::1", "::ffff:192.0.2.1")
      .foreach { host =>
        assertEquals(
          AppConfigFixtures
            .fromConfig(defaultConfig + s"""http.host = "$host"\nlogging.mask-sensitive = false\n""", Map.empty)
            .map(_.maskSensitive),
          Right(false),
          clues(host)
        )
        assert(
          AppConfigFixtures.fromConfig(defaultConfig + s"""http.host = "$host"\n""", Map.empty).isRight,
          clues(host)
        )
      }
    assertEquals(
      AppConfigFixtures.fromConfig(defaultConfig + "logging.mask-sensitive = false\n", Map.empty),
      Right(
        AppConfig(
          Host.fromString("127.0.0.1").get,
          Ip4sPort.fromInt(8080).get,
          16,
          5.seconds,
          TrustedProxyConfig(Nil),
          "mongodb://127.0.0.1:27017",
          "hiring",
          false,
          defaultJwtAuth,
          defaultPasswordHash,
          defaultAuthRateLimit,
          defaultVectorSearch,
          defaultKafka,
          interviewActionRateLimit = InterviewActionRateLimitConfig(60, 30, 10000)
        )
      )
    )
  }

  test("VHS-AC08 vector search requires an explicit Voyage API key when enabled") {
    assertContainsError(
      AppConfigFixtures.fromConfig(defaultConfig + "vector-search.enabled = true\n", Map.empty),
      ConfigError.InvalidVoyageApiKey
    )
    assertEquals(enabledVectorSearch().voyage.apiKey, "synthetic-voyage-key")
    assert(!enabledVectorSearch().voyage.toString.contains("synthetic-voyage-key"))
  }

  test("HGQL-AC02 JWT auth config requires a strong secret") {
    assertEquals(AppConfigFixtures.fromConfig(defaultConfig, Map.empty).map(_.jwtAuth), Right(defaultJwtAuth))
    assertContainsError(
      AppConfigFixtures.fromConfig(defaultConfig + "auth.jwt.hs256-secret = disabled\n", Map.empty),
      ConfigError.InvalidJwtSecret
    )
    assertContainsError(
      AppConfigFixtures
        .fromConfig(defaultConfig.replace("hs256-secret = \"01234567890123456789012345678901\"", ""), Map.empty),
      ConfigError.InvalidJwtSecret
    )
    assertContainsError(
      AppConfigFixtures.fromConfig(defaultConfig + "auth.jwt.hs256-secret = \"short\"\n", Map.empty),
      ConfigError.InvalidJwtSecret
    )
    val loaded = AppConfigFixtures.fromConfig(
      defaultConfig + "auth.jwt.hs256-secret = ${AUTH_JWT_HS256_SECRET}\n",
      Map("AUTH_JWT_HS256_SECRET" -> "abcdefghijklmnopqrstuvwxyz123456")
    )
    assertEquals(loaded.map(_.jwtAuth.hmacSecret), Right("abcdefghijklmnopqrstuvwxyz123456"))
  }

  test("authentication receipt secret is optional, strong when set, and falls back to the JWT secret") {
    val strong = "abcdefghijklmnopqrstuvwxyz123456"
    assertEquals(
      AppConfigFixtures.fromConfig(defaultConfig, Map.empty).map(_.jwtAuth.receiptSecret),
      Right(defaultJwtAuth.hmacSecret)
    )
    assertEquals(
      AppConfigFixtures
        .fromConfig(defaultConfig + "auth.jwt.receipt-fingerprint-secret = \"" + strong + "\"\n", Map.empty)
        .map(_.jwtAuth.receiptSecret),
      Right(strong)
    )
    assertEquals(
      AppConfigFixtures
        .fromConfig(defaultConfig + "auth.jwt.receipt-fingerprint-secret = \"  \"\n", Map.empty)
        .map(_.jwtAuth.receiptSecret),
      Right(defaultJwtAuth.hmacSecret)
    )
    assertContainsError(
      AppConfigFixtures.fromConfig(defaultConfig + "auth.jwt.receipt-fingerprint-secret = \"short\"\n", Map.empty),
      ConfigError.InvalidReceiptFingerprintSecret
    )
    assert(!defaultJwtAuth.copy(receiptFingerprintSecret = Some(strong)).toString.contains(strong))
  }

  test("HGQL-AC02 cursor JWT TTL is bounded and defaults to fifteen minutes") {
    assertEquals(AppConfigFixtures.fromConfig(defaultConfig, Map.empty).map(_.jwtAuth.cursorTtlSeconds), Right(900L))
    assertContainsError(
      AppConfigFixtures
        .fromConfig(defaultConfig.replace("cursor-ttl-seconds = 900", "cursor-ttl-seconds = 59"), Map.empty),
      ConfigError.InvalidCursorTtl
    )
    assertContainsError(
      AppConfigFixtures
        .fromConfig(defaultConfig.replace("cursor-ttl-seconds = 900", "cursor-ttl-seconds = 86401"), Map.empty),
      ConfigError.InvalidCursorTtl
    )
  }

  test("HGQL-AC02 auth limiter config is bounded and explicit") {
    assertEquals(
      AppConfigFixtures.fromConfig(defaultConfig, Map.empty).map(_.authRateLimit),
      Right(defaultAuthRateLimit)
    )
    assertContainsError(
      AppConfigFixtures.fromConfig(defaultConfig + "auth.rate-limit.window-seconds = 0\n", Map.empty),
      ConfigError.InvalidAuthRateLimitWindow
    )
    assertContainsError(
      AppConfigFixtures.fromConfig(defaultConfig + "auth.rate-limit.attempts = 0\n", Map.empty),
      ConfigError.InvalidAuthRateLimitAttempts
    )
    assertContainsError(
      AppConfigFixtures.fromConfig(defaultConfig + "auth.rate-limit.max-buckets = 0\n", Map.empty),
      ConfigError.InvalidAuthRateLimitBuckets
    )
  }

  test("DHW-34 the per-actor interview action limit is required, bounded and accumulates invalid settings") {
    def block(window: Int = 30, attempts: Int = 5, buckets: Int = 200): String =
      s"""auth.interview-action-rate-limit {
         |  window-seconds = $window
         |  attempts = $attempts
         |  max-buckets = $buckets
         |}
         |""".stripMargin
    assertEquals(
      AppConfigFixtures.fromConfig(defaultConfig, Map.empty).map(_.interviewActionRateLimit),
      Right(InterviewActionRateLimitConfig(60, 30, 10000))
    )
    // The section is required: removing it fails closed instead of falling back to a built-in value.
    val without = defaultConfig.replace(block(60, 30, 10000), "")
    assertNotEquals(without, defaultConfig)
    assertContainsError(AppConfigFixtures.fromConfig(without, Map.empty), ConfigError.InvalidConfigFile)
    assertEquals(
      AppConfigFixtures.fromConfig(defaultConfig + block(), Map.empty).map(_.interviewActionRateLimit),
      Right(InterviewActionRateLimitConfig(30, 5, 200))
    )
    assertContainsError(
      AppConfigFixtures.fromConfig(defaultConfig + block(window = 0), Map.empty),
      ConfigError.InvalidInterviewActionRateLimitWindow
    )
    assertContainsError(
      AppConfigFixtures.fromConfig(defaultConfig + block(window = 3601), Map.empty),
      ConfigError.InvalidInterviewActionRateLimitWindow
    )
    assertContainsError(
      AppConfigFixtures.fromConfig(defaultConfig + block(attempts = 0), Map.empty),
      ConfigError.InvalidInterviewActionRateLimitAttempts
    )
    assertContainsError(
      AppConfigFixtures.fromConfig(defaultConfig + block(buckets = 0), Map.empty),
      ConfigError.InvalidInterviewActionRateLimitBuckets
    )
    // Independent invalid settings accumulate rather than hiding each other.
    val accumulated =
      AppConfigFixtures.fromConfig(defaultConfig + block(window = 0, attempts = 0, buckets = 0), Map.empty)
    List(
      ConfigError.InvalidInterviewActionRateLimitWindow,
      ConfigError.InvalidInterviewActionRateLimitAttempts,
      ConfigError.InvalidInterviewActionRateLimitBuckets
    ).foreach(error => assertContainsError(accumulated, error))
  }

  test("password hash cost is explicit and bounded") {
    val passwordHash =
      """auth.password-hash {
        |  iterations = 3
        |  memory-kib = 32768
        |  parallelism = 2
        |}
        |""".stripMargin
    val configured = AppConfigFixtures.fromConfig(defaultConfig + passwordHash, Map.empty)
    assertEquals(configured.map(_.passwordHash), Right(PasswordHashConfig(3, 32768, 2)))
    assertContainsError(
      AppConfigFixtures.fromConfig(defaultConfig + passwordHash.replace("iterations = 3", "iterations = 0"), Map.empty),
      ConfigError.InvalidPasswordHashIterations
    )
    assertContainsError(
      AppConfigFixtures
        .fromConfig(defaultConfig + passwordHash.replace("memory-kib = 32768", "memory-kib = 1"), Map.empty),
      ConfigError.InvalidPasswordHashMemory
    )
    assertContainsError(
      AppConfigFixtures
        .fromConfig(defaultConfig + passwordHash.replace("parallelism = 2", "parallelism = 0"), Map.empty),
      ConfigError.InvalidPasswordHashParallelism
    )
  }

  test("shared bounded validation keeps inclusive numeric boundaries") {
    val bounded = List(
      ("auth.rate-limit.window-seconds", 1, 3600, ConfigError.InvalidAuthRateLimitWindow),
      ("kafka.publisher.batch-size", 1, 500, ConfigError.InvalidKafkaBatchSize),
      ("vector-search.num-candidates", PageSize.Max, 10000, ConfigError.InvalidVectorNumCandidates),
      ("vector-search.indexes.ready-timeout-ms", 1000, 600000, ConfigError.InvalidSearchIndexReadyTimeout),
      ("vector-search.embedding.retry-delay-ms", 100, 60000, ConfigError.InvalidEmbeddingRetryDelay)
    )

    bounded.foreach { case (path, minimum, maximum, error) =>
      assert(
        AppConfigFixtures.fromConfig(defaultConfig + s"$path = $minimum\n", Map.empty).isRight,
        clues(path, minimum)
      )
      assert(
        AppConfigFixtures.fromConfig(defaultConfig + s"$path = $maximum\n", Map.empty).isRight,
        clues(path, maximum)
      )
      assertContainsError(AppConfigFixtures.fromConfig(defaultConfig + s"$path = ${minimum - 1}\n", Map.empty), error)
      assertContainsError(AppConfigFixtures.fromConfig(defaultConfig + s"$path = ${maximum + 1}\n", Map.empty), error)
    }
  }

  test("branch result limit defaults to num-candidates and stays above maximum requested page size") {
    assertEquals(
      enabledVectorSearch().branchResultLimit,
      100
    )
    assert(AppConfigFixtures.fromConfig(defaultConfig + "vector-search.branch-result-limit = 100\n", Map.empty).isRight)
    assertContainsError(
      AppConfigFixtures.fromConfig(defaultConfig + "vector-search.branch-result-limit = 99\n", Map.empty),
      ConfigError.InvalidVectorBranchResultLimit
    )
    assertContainsError(
      AppConfigFixtures.fromConfig(
        defaultConfig + "vector-search.branch-result-limit = 101\nvector-search.num-candidates = 100\n",
        Map.empty
      ),
      ConfigError.InvalidVectorBranchResultLimit
    )
  }

  test("experimental Mongo fusion and reranking require explicit compatible configuration") {
    val rankFusion = defaultConfig + "vector-search.fusion-strategy = mongoRankFusion\n"
    val scoreFusion = defaultConfig + "vector-search.fusion-strategy = mongoScoreFusion\n"
    val invalidFusion = defaultConfig + "vector-search.fusion-strategy = unsupported\n"
    val rerankWithoutNativeFusion = defaultConfig + "vector-search.rerank.enabled = true\n"
    val invalidReranker = defaultConfig + "vector-search.rerank.model = unknown-model\n"

    assert(AppConfigFixtures.fromConfig(rankFusion, Map.empty).isRight)
    assert(AppConfigFixtures.fromConfig(scoreFusion, Map.empty).isRight)
    assertContainsError(AppConfigFixtures.fromConfig(invalidFusion, Map.empty), ConfigError.InvalidVectorFusionStrategy)
    assertContainsError(
      AppConfigFixtures.fromConfig(rerankWithoutNativeFusion, Map.empty),
      ConfigError.InvalidVectorFusionStrategy
    )
    assertContainsError(AppConfigFixtures.fromConfig(invalidReranker, Map.empty), ConfigError.InvalidRerankModel)
  }

  test("discovery query budgets name the violated field and the HTTP deadline separately") {
    assertEquals(AppConfigFixtures.fromConfig(defaultConfig, Map.empty).map(_.discovery), Right(DiscoveryConfig()))
    List(
      ("mongo.discovery.permits = 0", ConfigError.InvalidDiscoveryPermits),
      ("mongo.discovery.permits = 65", ConfigError.InvalidDiscoveryPermits),
      ("mongo.discovery.max-roots = 0", ConfigError.InvalidDiscoveryMaxRoots),
      ("mongo.discovery.max-roots = 65", ConfigError.InvalidDiscoveryMaxRoots),
      ("mongo.discovery.max-time-millis = 99", ConfigError.InvalidDiscoveryMaxTime),
      ("mongo.discovery.max-time-millis = \"fast\"", ConfigError.InvalidDiscoveryMaxTime),
      ("mongo.discovery.max-time-millis = 5000", ConfigError.InvalidDiscoveryDeadline)
    ).foreach { case (setting, expected) =>
      assertContainsError(AppConfigFixtures.fromConfig(defaultConfig + setting + "\n", Map.empty), expected)
    }
    assert(AppConfigFixtures.fromConfig(defaultConfig + "mongo.discovery.max-time-millis = 4999\n", Map.empty).isRight)
    assertEquals(
      AppConfigFixtures
        .fromConfig(defaultConfig + "mongo.discovery { permits = 0, max-roots = 65 }\n", Map.empty)
        .swap
        .map(_.toList.toSet),
      Right(Set[ConfigError](ConfigError.InvalidDiscoveryPermits, ConfigError.InvalidDiscoveryMaxRoots))
    )
  }

  test("durable embedding settings name each violated field and the retry window") {
    assertEquals(
      enabledVectorSearch().embedding.durableRetryAttempts,
      8
    )
    List(
      ("durable-retry-attempts = 0", ConfigError.InvalidEmbeddingDurableRetryAttempts),
      ("durable-retry-attempts = 101", ConfigError.InvalidEmbeddingDurableRetryAttempts),
      ("durable-retry-base-millis = 99", ConfigError.InvalidEmbeddingDurableRetryBase),
      ("durable-retry-cap-millis = 3600001", ConfigError.InvalidEmbeddingDurableRetryCap),
      ("durable-retry-cap-millis = 999", ConfigError.InvalidEmbeddingDurableRetryWindow),
      ("worker-restart-delay-millis = 99", ConfigError.InvalidEmbeddingWorkerRestartDelay),
      ("worker-restart-delay-millis = 60001", ConfigError.InvalidEmbeddingWorkerRestartDelay)
    ).foreach { case (setting, expected) =>
      assertContainsError(
        AppConfigFixtures.fromConfig(defaultConfig + s"vector-search.embedding.$setting\n", Map.empty),
        expected
      )
    }
    List("durable-retry-attempts = 100", "durable-retry-cap-millis = 1000", "worker-restart-delay-millis = 100")
      .foreach { setting =>
        assert(
          AppConfigFixtures.fromConfig(defaultConfig + s"vector-search.embedding.$setting\n", Map.empty).isRight,
          clues(setting)
        )
      }
    assertEquals(
      AppConfigFixtures
        .fromConfig(
          defaultConfig + "vector-search.embedding { durable-retry-attempts = 0, worker-restart-delay-millis = 99 }\n",
          Map.empty
        )
        .swap
        .map(_.toList.toSet),
      Right(
        Set[ConfigError](
          ConfigError.InvalidEmbeddingDurableRetryAttempts,
          ConfigError.InvalidEmbeddingWorkerRestartDelay
        )
      )
    )
  }

  test("Kafka partition concurrency names its own field") {
    List("0", "65", "\"many\"").foreach { value =>
      assertContainsError(
        AppConfigFixtures.fromConfig(defaultConfig + s"kafka.consumer.partition-concurrency = $value\n", Map.empty),
        ConfigError.InvalidKafkaPartitionConcurrency
      )
    }
    assertEquals(
      AppConfigFixtures
        .fromConfig(defaultConfig + "kafka.consumer.partition-concurrency = 64\n", Map.empty)
        .map(_.kafka.consumer.partitionConcurrency),
      Right(64)
    )
  }

  test("settings without a dedicated error before now name their own field") {
    List(
      ("mongo.reset-on-start = \"sometimes\"", ConfigError.InvalidMongoResetOnStart),
      ("kafka.publisher.worker-id = \" \"", ConfigError.InvalidKafkaWorkerId),
      ("kafka.consumer.enabled = \"maybe\"", ConfigError.InvalidKafkaConsumerEnabled),
      ("vector-search.indexes.candidate-lexical = \"\"", ConfigError.InvalidCandidateLexicalIndex),
      ("vector-search.rerank.enabled = \"later\"", ConfigError.InvalidRerankEnabled),
      ("vector-search.fusion-strategy = \"application-rrf\"", ConfigError.InvalidVectorFusionStrategy)
    ).foreach { case (setting, expected) =>
      assertContainsError(AppConfigFixtures.fromConfig(defaultConfig + setting + "\n", Map.empty), expected)
    }
  }

  test("a decode failure in one section does not hide validation failures in other sections") {
    assertEquals(
      AppConfigFixtures
        .fromConfig(
          defaultConfig + "kafka.consumer.partition-concurrency = 0\nhttp.host = \"not-an-ip\"\n",
          Map.empty
        )
        .swap
        .map(_.toList.toSet),
      Right(Set[ConfigError](ConfigError.InvalidKafkaPartitionConcurrency, ConfigError.InvalidHost))
    )
    assertEquals(
      AppConfigFixtures
        .fromConfig(
          defaultConfig + "mongo.discovery.permits = 0\nauth.jwt.hs256-secret = \"short\"\nvector-search.enabled = true\n",
          Map.empty
        )
        .swap
        .map(_.toList.toSet),
      Right(
        Set[ConfigError](
          ConfigError.InvalidDiscoveryPermits,
          ConfigError.InvalidJwtSecret,
          ConfigError.InvalidVoyageApiKey
        )
      )
    )
  }

  test("every typed error has a loggable key and every decoded path maps to one error") {
    assertEquals(ConfigError.publicKeys.size, ConfigError.values.length)
    ConfigError.values.foreach(error => assert(error.key.matches("[A-Z0-9_]+"), clues(error)))
    val owners = ConfigError.values.toList.flatMap(error => error.paths.toList.map(_ -> error)).groupMap(_._1)(_._2)
    assert(owners.values.forall(_.size == 1), clues(owners.filter(_._2.size > 1)))
  }

  test("request timeout is bounded") {
    assertEquals(AppConfigFixtures.fromConfig(defaultConfig, Map.empty).map(_.requestTimeout), Right(5.seconds))
    assertContainsError(
      AppConfigFixtures.fromConfig(defaultConfig + "http.request-timeout-ms = 99\n", Map.empty),
      ConfigError.InvalidRequestTimeout
    )
  }

  test("trusted proxy CIDRs are explicit, typed, and never global") {
    assertEquals(AppConfigFixtures.fromConfig(defaultConfig, Map.empty).map(_.trustedProxy.cidrs), Right(Nil))
    val configured = AppConfigFixtures.fromConfig(
      defaultConfig +
        "http.trusted-proxy-cidrs = [\"10.0.0.5/32\", \"2001:db8:10::5/128\"]\n",
      Map.empty
    )
    assertEquals(configured.map(_.trustedProxy.cidrs.map(_.toString)), Right(List("10.0.0.5/32", "2001:db8:10::5/128")))
    List("not-a-cidr", "0.0.0.0/0", "::/0").foreach { cidr =>
      assertContainsError(
        AppConfigFixtures.fromConfig(defaultConfig + s"http.trusted-proxy-cidrs = [\"$cidr\"]\n", Map.empty),
        ConfigError.InvalidTrustedProxyCidrs
      )
    }
  }

  test("VHS-AC07 vector search dimension is fixed to the configured Atlas index contract") {
    List("256", "512", "2048").foreach { dimension =>
      assertContainsError(
        AppConfigFixtures.fromConfig(defaultConfig + s"vector-search.voyage.dimension = $dimension\n", Map.empty),
        ConfigError.InvalidVoyageDimension
      )
    }
    assert(AppConfigFixtures.fromConfig(defaultConfig + "vector-search.voyage.dimension = 1024\n", Map.empty).isRight)
  }

  test("LOG-03 logging booleans are strict with safe configuration keys") {
    List("TRUE", "0", "synthetic-secret").foreach { value =>
      val result = AppConfigFixtures.fromConfig(defaultConfig + s"""logging.mask-sensitive = "$value"\n""", Map.empty)
      assertContainsError(result, ConfigError.InvalidMaskSensitive)
      assert(!result.toString.contains("synthetic-secret"))
    }
    assertContainsError(
      AppConfigFixtures
        .fromConfig(defaultConfig + "http.host = \"not-an-ip\"\nlogging.mask-sensitive = false\n", Map.empty),
      ConfigError.InvalidHost
    )
  }

  test("P1-AC01 aggregates independent configuration failures") {
    val invalid = defaultConfig
      .replace("host = \"127.0.0.1\"", "host = \"not-an-ip\"")
      .replace("database = \"hiring\"", "database = \"bad/name\"")
      .replace("enabled = false", "enabled = true")
    AppConfigFixtures.fromConfig(invalid, Map.empty) match {
      case Left(issues) =>
        assertEquals(
          issues.toList.toSet,
          Set(
            ConfigError.InvalidHost,
            ConfigError.InvalidMongoDatabase,
            ConfigError.InvalidKafkaCredentials,
            ConfigError.InvalidVoyageApiKey,
            ConfigError.InvalidVectorFusionStrategy
          )
        )
      case other => fail(s"Expected aggregated configuration failures, got $other")
    }
  }

  test("LOG-04 configuration remains redacted when local metadata is enabled") {
    val result = AppConfigFixtures.fromConfig(
      defaultConfig +
        """logging.mask-sensitive = false
        |mongo.uri = "mongodb://user:synthetic-secret@127.0.0.1:1"
        |""".stripMargin,
      Map.empty
    )
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
