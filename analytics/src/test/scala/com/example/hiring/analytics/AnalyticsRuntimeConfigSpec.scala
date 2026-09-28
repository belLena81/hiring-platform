package com.example.hiring.analytics
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.service.keyretirement.*
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
|  operational {
    |    retention {
    |      bronze-days = 7
    |      bronze-days = ${?ANALYTICS_RETENTION_BRONZE_DAYS}
    |      quarantine-days = 7
    |      quarantine-days = ${?ANALYTICS_RETENTION_QUARANTINE_DAYS}
    |      silver-days = 30
    |      silver-days = ${?ANALYTICS_RETENTION_SILVER_DAYS}
    |      published-snapshot-days = 30
    |      published-snapshot-days = ${?ANALYTICS_RETENTION_PUBLISHED_SNAPSHOT_DAYS}
    |      deletion-marker-days = 31
    |      deletion-marker-days = ${?ANALYTICS_RETENTION_DELETION_MARKER_DAYS}
    |      delta-vacuum-safety-days = 7
    |      delta-vacuum-safety-days = ${?ANALYTICS_RETENTION_DELTA_VACUUM_SAFETY_DAYS}
    |      delta-log-retention-days = 30
    |      delta-log-retention-days = ${?ANALYTICS_RETENTION_DELTA_LOG_RETENTION_DAYS}
    |    }
    |    report-reservation-ttl-days = 90
    |    report-reservation-ttl-days = ${?ANALYTICS_REPORT_RESERVATION_TTL_DAYS}
    |    mongo-transaction-window-seconds = 120
    |    mongo-transaction-window-seconds = ${?ANALYTICS_MONGO_TRANSACTION_WINDOW_SECONDS}
    |    maximum-erasure-evidence-files = 100000
    |    maximum-erasure-evidence-files = ${?ANALYTICS_MAXIMUM_ERASURE_EVIDENCE_FILES}
    |    mongo-publisher-buffer-size = 256
    |    mongo-publisher-buffer-size = ${?ANALYTICS_MONGO_PUBLISHER_BUFFER_SIZE}
|  }
|  key-retirement-audit {
|    retiring-key-id = ${?ANALYTICS_RETIRING_KEY_ID}
|    kafka {
|      barrier-offset = ${?ANALYTICS_RETIRING_KAFKA_BARRIER_OFFSET}
|      earliest-available-offset = ${?ANALYTICS_RETIRING_KAFKA_EARLIEST_OFFSET}
|      evidence-reference = "kafka-barrier-evidence"
|    }
|    delta-data { retained-until = "2026-09-27T00:00:00Z", evidence-reference = "delta-data-evidence" }
|    delta-logs { retained-until = "2026-09-27T00:00:00Z", evidence-reference = "delta-logs-evidence" }
|    reports { retained-until = "2026-09-27T00:00:00Z", evidence-reference = "report-evidence" }
|    writers {
|      observed-at = "2026-09-27T00:00:00Z"
|      coverage-reference = "writer-coverage"
|      managed = [{ identity = "batch", disposition = "stopped", evidence-reference = "stop-record" }]
|      unmanaged = []
|    }
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
    assertEquals(loaded.common.lakehouseRoot, "file:///tmp/hiring-analytics")
    assertEquals(loaded.common.mongoDatabase, "hiring")
    assertEquals(loaded.common.operational.reportReservationTtlDays, 90)
    assertEquals(loaded.common.operational.mongoTransactionWindowSeconds, 120)
    assertEquals(loaded.common.operational.maximumErasureEvidenceFiles, 100000)
    assertEquals(loaded.common.operational.mongoPublisherBufferSize, 256)
    assert(!loaded.toString.contains(key))
  }

  test("runtime composition resolves validated lakehouse roots into service paths") {
    val paths = AppModule
      .resolveLakehousePaths("file:///tmp/hiring-analytics")
      .toOption
      .getOrElse(fail("expected valid lakehouse paths"))
    assertEquals(paths.bronze, "file:///tmp/hiring-analytics/bronze/operational_events")

    val error = AppModule
      .resolveLakehousePaths(" ")
      .swap
      .toOption
      .getOrElse(fail("expected invalid lakehouse root"))
    assert(error.isInstanceOf[AnalyticsError.InvalidConfiguration])
    assertEquals(error.getMessage, "lakehouse root must be non-empty")
  }

  test("operational retention and runtime bounds load environment overrides and reject non-positive values") {
    val configured = AnalyticsRuntimeConfig
      .operationalFromHocon(
        hocon,
        Map(
          "ANALYTICS_RETENTION_BRONZE_DAYS" -> "14",
          "ANALYTICS_RETENTION_QUARANTINE_DAYS" -> "12",
          "ANALYTICS_RETENTION_SILVER_DAYS" -> "60",
          "ANALYTICS_RETENTION_PUBLISHED_SNAPSHOT_DAYS" -> "15",
          "ANALYTICS_RETENTION_DELETION_MARKER_DAYS" -> "20",
          "ANALYTICS_RETENTION_DELTA_VACUUM_SAFETY_DAYS" -> "3",
          "ANALYTICS_RETENTION_DELTA_LOG_RETENTION_DAYS" -> "10",
          "ANALYTICS_REPORT_RESERVATION_TTL_DAYS" -> "45",
          "ANALYTICS_MONGO_TRANSACTION_WINDOW_SECONDS" -> "30",
          "ANALYTICS_MAXIMUM_ERASURE_EVIDENCE_FILES" -> "8000",
          "ANALYTICS_MONGO_PUBLISHER_BUFFER_SIZE" -> "64"
        )
      )
      .toOption
      .getOrElse(fail("expected valid operational config"))
    assertEquals(configured.retention.bronzeDays, 14)
    assertEquals(configured.retention.quarantineDays, 12)
    assertEquals(configured.retention.silverDays, 60)
    assertEquals(configured.retention.publishedSnapshotDays, 15)
    assertEquals(configured.retention.deletionMarkerDays, 20)
    assertEquals(configured.retention.deltaVacuumSafetyDays, 3)
    assertEquals(configured.retention.deltaVacuumSafetyCheckEnabled, false)
    assertEquals(configured.retention.deltaLogRetentionDays, 10)
    assertEquals(configured.reportReservationTtlDays, 45)
    assertEquals(configured.mongoTransactionWindowSeconds, 30)
    assertEquals(configured.maximumErasureEvidenceFiles, 8000)
    assertEquals(configured.mongoPublisherBufferSize, 64)

    val invalid = AnalyticsRuntimeConfig.operationalFromHocon(
      hocon,
      Map("ANALYTICS_RETENTION_BRONZE_DAYS" -> "0", "ANALYTICS_MONGO_PUBLISHER_BUFFER_SIZE" -> "-1")
    )
    val message = invalid.swap.toOption.getOrElse(fail("expected rejected operational settings")).getMessage
    assert(message.contains("retention.bronze-days must be greater than zero"))
    assert(message.contains("mongo-publisher-buffer-size must be between one and"))

    val evidenceOverflow = AnalyticsRuntimeConfig.operationalFromHocon(
      hocon,
      Map("ANALYTICS_MAXIMUM_ERASURE_EVIDENCE_FILES" -> Int.MaxValue.toString)
    )
    assert(evidenceOverflow.isLeft)

    val safeVacuum = AnalyticsRuntimeConfig
      .operationalFromHocon(
        hocon,
        Map("ANALYTICS_RETENTION_DELTA_VACUUM_SAFETY_DAYS" -> "7")
      )
      .toOption
      .getOrElse(fail("expected valid Delta vacuum config"))
    assertEquals(safeVacuum.retention.deltaVacuumSafetyCheckEnabled, true)
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
    intercept[IllegalArgumentException](KafkaClientProperties.clientProperties(externalEndpoint))
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
        "ANALYTICS_KAFKA_ALLOW_PLAINTEXT" -> "perhaps"
      )
    )
    assert(malformedFlag.isLeft)
  }

  test("typed batch readers reject malformed numeric substitutions without echoing their values") {
    val malformed = AnalyticsRuntimeConfig.batchFromHocon(
      hocon,
      settings + ("ANALYTICS_PARTITION" -> "not-a-number")
    )
    assert(malformed.swap.toOption.exists(error => !error.getMessage.contains("not-a-number")))
  }

  test("packaged application.conf supports the same substitutions and defaults") {
    val input = Option(getClass.getResourceAsStream("/application.conf")).getOrElse(fail("application.conf is missing"))
    val packaged = try new String(input.readAllBytes(), StandardCharsets.UTF_8)
    finally input.close()
    val loaded =
      AnalyticsRuntimeConfig.batchFromHocon(packaged, settings).toOption.getOrElse(fail("expected packaged config"))
    assertEquals(loaded.common.sparkMaster, "local[*]")
    assertEquals(loaded.manifest.offsetRanges.head.topic, "hiring.operational-events")
    val audit = AnalyticsRuntimeConfig
      .keyRetirementAuditFromHocon(packaged, settings + ("ANALYTICS_RETIRING_KEY_ID" -> "hmac-v1"))
      .toOption
      .getOrElse(fail("expected valid audit config"))
    assertEquals(audit.mongoDatabase, "hiring")
    assertEquals(audit.sparkMaster, "local[*]")
    assertEquals(audit.lakehouseRoot, "file:///tmp/hiring-analytics")
    assertEquals(audit.kafkaBarrierOffset, None)
    assertEquals(audit.writers.managed, Vector.empty)
    assertEquals(audit.operational.mongoPublisherBufferSize, 256)

    val credentialedAudit = AnalyticsRuntimeConfig
      .keyRetirementAuditFromHocon(
        packaged,
        settings ++ Map(
          "ANALYTICS_RETIRING_KEY_ID" -> "hmac-v1",
          "MONGODB_URI" -> "mongodb://audit-user:audit-secret@localhost:27017/?replicaSet=rs0"
        )
      )
      .toOption
      .getOrElse(fail("expected valid credentialed audit config"))
    assert(!credentialedAudit.toString.contains("audit-user"))
    assert(!credentialedAudit.toString.contains("audit-secret"))
  }

  test("key-retirement audit config decodes typed evidence and rejects malformed typed fields") {
    val audit = AnalyticsRuntimeConfig
      .keyRetirementAuditFromHocon(
        hocon,
        settings ++ Map(
          "ANALYTICS_RETIRING_KEY_ID" -> "hmac-v1",
          "ANALYTICS_RETIRING_KAFKA_BARRIER_OFFSET" -> "12",
          "ANALYTICS_RETIRING_KAFKA_EARLIEST_OFFSET" -> "12"
        )
      )
      .toOption
      .getOrElse(fail("expected valid audit config"))
    assertEquals(audit.kafkaBarrierOffset, Some(12L))
    assertEquals(audit.kafkaEarliestAvailableOffset, Some(12L))
    assertEquals(audit.deltaData.retainedUntil, Some(java.time.Instant.parse("2026-09-27T00:00:00Z")))
    assertEquals(audit.writers.observedAt, Some(java.time.Instant.parse("2026-09-27T00:00:00Z")))
    assertEquals(audit.writers.managed.head.disposition, Some(AnalyticsAuditWriterDisposition.Stopped))

    val malformedTimestamp = hocon.replace("2026-09-27T00:00:00Z", "invalid-timestamp")
    assert(AnalyticsRuntimeConfig.keyRetirementAuditFromHocon(malformedTimestamp, settings).isLeft)
    val malformedDisposition = hocon.replace("disposition = \"stopped\"", "disposition = \"invalid\"")
    assert(AnalyticsRuntimeConfig.keyRetirementAuditFromHocon(malformedDisposition, settings).isLeft)
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
      "ANALYTICS_PARTITION" -> "2",
      "ANALYTICS_START_OFFSET" -> "10",
      "ANALYTICS_END_OFFSET_EXCLUSIVE" -> "4"
    )
    val error =
      AnalyticsRuntimeConfig.batchFromHocon(hocon, invalid).swap.toOption.getOrElse(fail("expected invalid config"))
    val message = error.getMessage
    assert(message.contains("analytics.mongo.uri is required"))
    assert(message.contains("analytics.kafka.bootstrap-servers is required"))
    assert(message.contains("analytics.kafka.username is required"))
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
