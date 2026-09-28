package com.example.hiring.analytics
import com.example.hiring.analytics.service.keyretirement.*
import com.example.hiring.analytics.cli.AnalyticsKeyRetirementAuditMain
import com.example.hiring.analytics.service.batch.*
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*
import com.example.hiring.analytics.adapter.kafka.*
import com.example.hiring.analytics.adapter.local.*
import com.example.hiring.analytics.service.erasure.*

import com.example.hiring.analytics.adapter.spark.*
import com.example.hiring.analytics.adapter.mongo.*

import java.util.Base64
import java.nio.charset.StandardCharsets

class AnalyticsRuntimeConfigSpec extends munit.FunSuite {
  private val key = Base64.getEncoder.encodeToString(Array.fill[Byte](32)(7))
  private val settings = Map(
    "MONGODB_URI" -> "mongodb://localhost:27017/?replicaSet=rs0",
    "ANALYTICS_BOOTSTRAP_SERVERS" -> "localhost:9092",
    "ANALYTICS_KAFKA_USERNAME" -> "analytics_reader",
    "ANALYTICS_KAFKA_PASSWORD" -> "reader-secret",
    "ANALYTICS_KAFKA_FENCER_USERNAME" -> "analytics_fencer",
    "ANALYTICS_KAFKA_FENCER_PASSWORD" -> "fencer-secret",
    "ANALYTICS_LAKEHOUSE_ROOT" -> "file:///tmp/hiring-analytics",
    "HIRING_ANALYTICS_HMAC_SECRET_BASE64" -> key,
    "ANALYTICS_RUN_ID" -> "run-local-1",
    "ANALYTICS_PARTITION" -> "2",
    "ANALYTICS_START_OFFSET" -> "10",
    "ANALYTICS_END_OFFSET_EXCLUSIVE" -> "20"
  )

  private val hocon = """
    |analytics {
    |  mongo { uri = ${?MONGODB_URI}, database = "hiring" }
    |  spark { master = "local[*]" }
    |  kafka {
    |    bootstrap-servers = ${?ANALYTICS_BOOTSTRAP_SERVERS}
    |    username = ${?ANALYTICS_KAFKA_USERNAME}
    |    password = ${?ANALYTICS_KAFKA_PASSWORD}
    |    topic = "hiring.operational-events"
    |    security-protocol = ${?ANALYTICS_KAFKA_SECURITY_PROTOCOL}
    |    allow-plaintext = ${?ANALYTICS_KAFKA_ALLOW_PLAINTEXT}
    |    fencer {
    |      username = ${?ANALYTICS_KAFKA_FENCER_USERNAME}
    |      password = ${?ANALYTICS_KAFKA_FENCER_PASSWORD}
    |    }
    |  }
    |  lakehouse { root = ${?ANALYTICS_LAKEHOUSE_ROOT} }
    |  hmac {
    |    secret-base64 = ${?HIRING_ANALYTICS_HMAC_SECRET_BASE64}
    |    key-id = "hmac-v1"
    |    previous-key-id = ${?HIRING_ANALYTICS_HMAC_PREVIOUS_KEY_ID}
    |    previous-secret-base64 = ${?HIRING_ANALYTICS_HMAC_PREVIOUS_SECRET_BASE64}
    |  }
    |  batch {
    |    run-id = ${?ANALYTICS_RUN_ID}
    |    partition = ${?ANALYTICS_PARTITION}
    |    start-offset = ${?ANALYTICS_START_OFFSET}
    |    end-offset-exclusive = ${?ANALYTICS_END_OFFSET_EXCLUSIVE}
    |  }
    |}
    |""".stripMargin

  test("batch settings load service and run inputs through HOCON substitutions") {
    val loaded =
      AnalyticsRuntimeConfig.batchFromHocon(hocon, settings).toOption.getOrElse(fail("expected valid batch config"))
    assertEquals(loaded.manifest.runId.value, "run-local-1")
    assertEquals(
      loaded.manifest.offsetRanges,
      Vector(PartitionOffsetRange.unsafe("hiring.operational-events", 2, 10L, 20L))
    )
    assertEquals(loaded.common.kafka.bootstrapServers, "localhost:9092")
    assertEquals(loaded.common.kafka.securityProtocol, "SASL_SSL")
    assertEquals(loaded.common.kafka.allowPlaintext, false)
    assertEquals(loaded.common.mongoDatabase, "hiring")
    assert(!loaded.toString.contains(key))
  }

  test("Kafka configuration allows plaintext only with an explicit opt-in") {
    val local = settings ++ Map(
      "ANALYTICS_KAFKA_SECURITY_PROTOCOL" -> "SASL_PLAINTEXT",
      "ANALYTICS_KAFKA_ALLOW_PLAINTEXT" -> "true"
    )
    val loaded = AnalyticsRuntimeConfig.batchFromHocon(hocon, local).toOption.getOrElse(fail("expected local config"))
    assertEquals(loaded.common.kafka.securityProtocol, "SASL_PLAINTEXT")
    assertEquals(loaded.common.kafka.allowPlaintext, true)

    val missingOptIn = AnalyticsRuntimeConfig
      .batchFromHocon(
        hocon,
        settings + ("ANALYTICS_KAFKA_SECURITY_PROTOCOL" -> "SASL_PLAINTEXT")
      )
      .swap
      .toOption
      .getOrElse(fail("expected disallowed plaintext protocol"))
    assert(missingOptIn.getMessage.contains("allow-plaintext=true"))

    val composeEndpoint = KafkaConnection.validate(
      KafkaConnection("kafka:9092", Some("reader"), Some("password"), "SASL_PLAINTEXT", allowPlaintext = true)
    )
    assert(composeEndpoint.isValid)
    val externalEndpoint = KafkaConnection(
      "broker.example.com:9092",
      Some("reader"),
      Some("password"),
      "SASL_PLAINTEXT",
      allowPlaintext = true
    )
    assert(KafkaConnection.validate(externalEndpoint).isInvalid)
    assertEquals(KafkaConnection.clientProperties(externalEndpoint), Map.empty[String, String])
  }

  test("Kafka configuration rejects unsupported protocols and malformed opt-in values") {
    val unsupported = AnalyticsRuntimeConfig.batchFromHocon(
      hocon,
      settings ++ Map(
        "ANALYTICS_KAFKA_SECURITY_PROTOCOL" -> "PLAINTEXT",
        "ANALYTICS_KAFKA_ALLOW_PLAINTEXT" -> "true"
      )
    )
    assert(unsupported.swap.toOption.exists(_.getMessage.contains("security protocol")))

    val malformedFlag = AnalyticsRuntimeConfig.batchFromHocon(
      hocon,
      settings ++ Map(
        "ANALYTICS_KAFKA_SECURITY_PROTOCOL" -> "SASL_PLAINTEXT",
        "ANALYTICS_KAFKA_ALLOW_PLAINTEXT" -> "yes"
      )
    )
    assert(malformedFlag.isLeft)
  }

  test("packaged application.conf supports the same substitutions and defaults") {
    val input = Option(getClass.getResourceAsStream("/application.conf")).getOrElse(fail("application.conf is missing"))
    val packaged = try new String(input.readAllBytes(), StandardCharsets.UTF_8)
    finally input.close()
    val loaded =
      AnalyticsRuntimeConfig.batchFromHocon(packaged, settings).toOption.getOrElse(fail("expected packaged config"))
    assertEquals(loaded.common.sparkMaster, "local[*]")
    assertEquals(loaded.manifest.offsetRanges.head.topic, "hiring.operational-events")
    assertEquals(AnalyticsKeyRetirementAuditMain.validateAuditHocon(packaged), Right(()))
  }

  test("worker settings validate a separate fencer connection") {
    val loaded =
      AnalyticsRuntimeConfig.workerFromHocon(hocon, settings).toOption.getOrElse(fail("expected valid worker config"))
    assertEquals(loaded.topic, "hiring.operational-events")
    assertEquals(loaded.fencerKafka.saslUsername, Some("analytics_fencer"))
    assert(!loaded.toString.contains("fencer-secret"))
  }

  test("empty optional previous-key substitutions mean no retiring key") {
    val withEmptyRotationSettings = settings ++ Map(
      "HIRING_ANALYTICS_HMAC_PREVIOUS_KEY_ID" -> "",
      "HIRING_ANALYTICS_HMAC_PREVIOUS_SECRET_BASE64" -> ""
    )
    assert(AnalyticsRuntimeConfig.batchFromHocon(hocon, withEmptyRotationSettings).isRight)

    val incomplete = SubjectPseudonymizer.validateFromBase64(Some(key), "hmac-v1", Some("old-key"), Some(""))
    assert(
      incomplete.toEither.swap.toOption.exists(
        _.toChain.toList.contains(
          "previous HMAC key ID and secret must be configured together"
        )
      )
    )
  }

  test("batch configuration accumulates missing and malformed independent fields without exposing values") {
    val invalid = settings -- Set(
      "MONGODB_URI",
      "ANALYTICS_BOOTSTRAP_SERVERS",
      "ANALYTICS_KAFKA_USERNAME"
    ) ++ Map(
      "HIRING_ANALYTICS_HMAC_SECRET_BASE64" -> "this-value-must-not-appear-in-errors",
      "ANALYTICS_PARTITION" -> "not-a-number",
      "ANALYTICS_START_OFFSET" -> "10",
      "ANALYTICS_END_OFFSET_EXCLUSIVE" -> "4"
    )
    val error =
      AnalyticsRuntimeConfig.batchFromHocon(hocon, invalid).swap.toOption.getOrElse(fail("expected invalid config"))
    val message = error.getMessage
    assert(message.contains("analytics.mongo.uri is required"))
    assert(message.contains("analytics.kafka.bootstrap-servers is required"))
    assert(message.contains("analytics.kafka.username is required"))
    assert(message.contains("analytics.batch.partition must be an integer"))
    assert(message.contains("end offset must not precede start offset"))
    assert(!message.contains("this-value-must-not-appear-in-errors"))
  }

  test("HMAC validation accumulates short, malformed, and unpaired key settings") {
    val shortKey = Base64.getEncoder.encodeToString("short".getBytes(StandardCharsets.UTF_8))
    val validation = SubjectPseudonymizer.validateFromBase64(
      Some(shortKey),
      " ",
      Some("old-key"),
      None
    )
    val errors =
      validation.toEither.swap.toOption.getOrElse(fail("expected invalid key ring")).toChain.toList.mkString(" ")
    assert(errors.contains("HIRING_ANALYTICS_HMAC_SECRET_BASE64 must decode to at least 32 bytes"))
    assert(errors.contains("HIRING_ANALYTICS_HMAC_KEY_ID must be non-empty"))
    assert(errors.contains("previous HMAC key ID and secret must be configured together"))
  }
}
