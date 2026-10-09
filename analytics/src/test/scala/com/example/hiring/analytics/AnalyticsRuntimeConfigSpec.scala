package com.example.hiring.analytics
import com.example.hiring.analytics.app.AppModule
import com.example.hiring.analytics.errors.*
import com.example.hiring.analytics.domain.*
import com.example.hiring.analytics.config.*
import com.example.hiring.analytics.adapter.kafka.*
import scala.concurrent.duration.*

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
    |  spark { master = "local[*]", local-directory = "/var/lib/hiring-analytics/spark-temp/runtime-test" }
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
    |      delta-vacuum-safety = 7 days
    |      delta-vacuum-safety = ${?ANALYTICS_RETENTION_DELTA_VACUUM_SAFETY}
    |      delta-log-retention = 30 days
    |      delta-log-retention = ${?ANALYTICS_RETENTION_DELTA_LOG_RETENTION}
    |    }
    |    report-reservation-ttl = 90 days
    |    report-reservation-ttl = ${?ANALYTICS_REPORT_RESERVATION_TTL}
    |    mongo-transaction-window = 120 seconds
    |    mongo-transaction-window = ${?ANALYTICS_MONGO_TRANSACTION_WINDOW}
    |    maximum-erasure-evidence-files = 100000
    |    maximum-erasure-evidence-files = ${?ANALYTICS_MAXIMUM_ERASURE_EVIDENCE_FILES}
|    mongo-publisher-buffer-size = 256
|    mongo-publisher-buffer-size = ${?ANALYTICS_MONGO_PUBLISHER_BUFFER_SIZE}
|    erasure-worker {
|      lease-duration = 90 seconds
|      lease-duration = ${?ANALYTICS_ERASURE_LEASE_DURATION}
|      delivery-timeout = 30 seconds
|      delivery-timeout = ${?ANALYTICS_ERASURE_DELIVERY_TIMEOUT}
|      poll-interval = 5 seconds
|      poll-interval = ${?ANALYTICS_ERASURE_POLL_INTERVAL}
|    }
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

  /** Operational settings as the batch loader resolves them, with `overrides` layered over the base environment. */
  private def operational(overrides: Map[String, String]): Either[AnalyticsError, AnalyticsOperationalSettings] =
    AnalyticsConfigFixtures.batch(hocon, settings ++ overrides).map(_.common.operational)

  test("batch settings load service and run inputs through HOCON substitutions") {
    val loaded =
      AnalyticsConfigFixtures.batch(hocon, settings).toOption.getOrElse(fail("expected valid batch config"))
    assertEquals(loaded.manifest.runId.value, "run-local-1")
    assertEquals(
      loaded.manifest.offsetRanges,
      Vector(TestPartitionOffsetRange.unsafe("hiring.operational-events", 2, 10L, 20L))
    )
    assertEquals(loaded.common.kafka.bootstrapServers, "localhost:9092")
    assertEquals(loaded.common.kafka.securityProtocol, KafkaSecurityProtocol.SaslSsl)
    assertEquals(loaded.common.kafka.allowPlaintext, false)
    assertEquals(loaded.common.lakehouseRoot, "file:///tmp/hiring-analytics")
    assertEquals(loaded.common.mongoDatabase, "hiring")
    assertEquals(loaded.common.operational.reportReservationTtl, 90.days)
    assertEquals(loaded.common.operational.mongoTransactionWindow, 120.seconds)
    assertEquals(loaded.common.operational.maximumErasureEvidenceFiles, 100000)
    assertEquals(loaded.common.operational.mongoPublisherBufferSize, 256)
    assert(!loaded.toString.contains(key))
  }

  test("late-fact replay settings bind an explicit request to the configured Kafka topic") {
    val replayHocon = hocon.replace(
      "  batch {",
      """|  replay {
        |    request-id = "late-replay-2026-09-30"
        |    maximum-records = 2
        |    coordinates = [
        |      { topic = "hiring.operational-events", partition = 0, offset = 7 }
        |      { topic = "hiring.operational-events", partition = 1, offset = 19 }
        |    ]
        |  }
        |  batch {""".stripMargin
    )
    val loaded = AnalyticsConfigFixtures
      .lateFactReplay(replayHocon, settings)
      .toOption
      .getOrElse(fail("expected valid late-fact replay config"))

    assertEquals(loaded.request.requestId.value, "late-replay-2026-09-30")
    assertEquals(loaded.request.coordinates.size, 2)
    assertEquals(loaded.request.selectionDigest.value.length, 64)
    assert(!loaded.toString.contains("reader-secret"))
  }

  test("late-fact replay configuration rejects topic drift and invalid request limits") {
    val replayHocon = hocon.replace(
      "  batch {",
      """|  replay {
        |    request-id = "late-replay-2026-09-30"
        |    maximum-records = 1
        |    coordinates = [
        |      { topic = "other.topic", partition = 0, offset = 7 }
        |      { topic = "hiring.operational-events", partition = 1, offset = 19 }
        |    ]
        |  }
        |  batch {""".stripMargin
    )
    val error = AnalyticsConfigFixtures
      .lateFactReplay(replayHocon, settings)
      .swap
      .toOption
      .getOrElse(fail("expected topic mismatch and over-limit errors"))
    assert(error.getMessage.contains("replay request exceeds its configured limit"))
    assert(error.getMessage.contains("coordinates must use analytics.kafka.topic"))
  }

  test("late-fact replay configuration requires identity and coordinates and rejects duplicate selections") {
    val missingBlock = AnalyticsConfigFixtures.lateFactReplay(hocon, settings)
    assert(missingBlock.swap.toOption.exists(_.getMessage.contains("Key not found: 'replay'")))

    val incomplete = AnalyticsConfigFixtures.lateFactReplay(
      hocon.replace("  batch {", "  replay { maximum-records = 1000 }\n  batch {"),
      settings
    )
    val incompleteMessage = incomplete.swap.toOption.getOrElse(fail("expected required replay fields to fail"))
    assert(incompleteMessage.getMessage.contains("Key not found: 'request-id'"))
    assert(incompleteMessage.getMessage.contains("Key not found: 'coordinates'"))

    val duplicate = AnalyticsConfigFixtures.lateFactReplay(
      hocon.replace(
        "  batch {",
        """  replay {
          request-id = "duplicate-selection"
          coordinates = [
            { topic = "hiring.operational-events", partition = 0, offset = 7 }
            { topic = "hiring.operational-events", partition = 0, offset = 7 }
          ]
        }
        batch {"""
      ),
      settings
    )
    assert(
      duplicate.swap.toOption
        .exists(_.getMessage.contains("replay Kafka coordinates must be unique"))
    )
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

  test("operational retention, worker timings, and runtime bounds validate defaults and overrides") {
    val configured = operational(
      Map(
        "ANALYTICS_RETENTION_BRONZE_DAYS" -> "14",
        "ANALYTICS_RETENTION_QUARANTINE_DAYS" -> "12",
        "ANALYTICS_RETENTION_SILVER_DAYS" -> "60",
        "ANALYTICS_RETENTION_PUBLISHED_SNAPSHOT_DAYS" -> "15",
        "ANALYTICS_RETENTION_DELETION_MARKER_DAYS" -> "20",
        "ANALYTICS_RETENTION_DELTA_VACUUM_SAFETY" -> "3 days",
        "ANALYTICS_RETENTION_DELTA_LOG_RETENTION" -> "10 days",
        "ANALYTICS_REPORT_RESERVATION_TTL" -> "45 days",
        "ANALYTICS_MONGO_TRANSACTION_WINDOW" -> "30 seconds",
        "ANALYTICS_MAXIMUM_ERASURE_EVIDENCE_FILES" -> "8000",
        "ANALYTICS_MONGO_PUBLISHER_BUFFER_SIZE" -> "64",
        "ANALYTICS_ERASURE_LEASE_DURATION" -> "120 seconds",
        "ANALYTICS_ERASURE_DELIVERY_TIMEOUT" -> "45 seconds",
        "ANALYTICS_ERASURE_POLL_INTERVAL" -> "8 seconds"
      )
    ).toOption
      .getOrElse(fail("expected valid operational config"))
    val observedAt = java.time.Instant.parse("2026-10-01T00:00:00Z")
    assertEquals(configured.retention.publishedSnapshotExpiry(observedAt), observedAt.plusSeconds(15L * 86400L))
    assertEquals(configured.retention.deletionMarkerCutoff(observedAt), observedAt.minusSeconds(20L * 86400L))
    assertEquals(configured.retention.bronzeDays, 14)
    assertEquals(configured.retention.quarantineDays, 12)
    assertEquals(configured.retention.silverDays, 60)
    assertEquals(configured.retention.publishedSnapshotDays, 15)
    assertEquals(configured.retention.deletionMarkerDays, 20)
    assertEquals(configured.retention.deltaVacuumSafety, 3.days)
    assertEquals(configured.retention.deltaVacuumSafetyCheckEnabled, false)
    assertEquals(configured.retention.deltaLogRetention, 10.days)
    assertEquals(configured.reportReservationTtl, 45.days)
    assertEquals(configured.mongoTransactionWindow, 30.seconds)
    assertEquals(configured.erasureWorkerTimings.leaseDuration, 120.seconds)
    assertEquals(configured.erasureWorkerTimings.deliveryTimeout, 45.seconds)
    assertEquals(configured.erasureWorkerTimings.pollInterval, 8.seconds)
    assertEquals(configured.maximumErasureEvidenceFiles, 8000)
    assertEquals(configured.mongoPublisherBufferSize, 64)

    val invalid = operational(
      Map("ANALYTICS_RETENTION_BRONZE_DAYS" -> "0", "ANALYTICS_MONGO_PUBLISHER_BUFFER_SIZE" -> "-1")
    )
    val message = invalid.swap.toOption.getOrElse(fail("expected rejected operational settings")).getMessage
    assert(message.contains("analytics.operational.retention.bronze-days"), message)
    assert(message.contains("Should be strictly positive"))
    assert(message.contains("analytics.operational.mongo-publisher-buffer-size"))
    assert(message.contains("Should be included in [1, 65536]"))

    val invalidWorkerTimings = operational(
      Map(
        "ANALYTICS_ERASURE_LEASE_DURATION" -> "0 seconds",
        "ANALYTICS_ERASURE_DELIVERY_TIMEOUT" -> "-1 seconds",
        "ANALYTICS_ERASURE_POLL_INTERVAL" -> "0 seconds"
      )
    )
    val workerTimingMessage = invalidWorkerTimings.swap.toOption
      .getOrElse(fail("expected rejected erasure worker timings"))
      .getMessage
    assert(
      workerTimingMessage.contains("erasure-worker.lease-duration: Should be at least one millisecond"),
      workerTimingMessage
    )
    assert(workerTimingMessage.contains("erasure-worker.delivery-timeout: Should be at least one millisecond"))
    assert(workerTimingMessage.contains("erasure-worker.poll-interval: Should be at least one millisecond"))

    val subMillisecondWorkerTimings = operational(
      Map(
        "ANALYTICS_ERASURE_LEASE_DURATION" -> "1 nanosecond",
        "ANALYTICS_ERASURE_DELIVERY_TIMEOUT" -> "999 microseconds",
        "ANALYTICS_ERASURE_POLL_INTERVAL" -> "999 microseconds"
      )
    )
    val subMillisecondMessage = subMillisecondWorkerTimings.swap.toOption
      .getOrElse(fail("expected rejected sub-millisecond erasure worker timings"))
      .getMessage
    assert(subMillisecondMessage.contains("erasure-worker.lease-duration: Should be at least one millisecond"))
    assert(subMillisecondMessage.contains("erasure-worker.delivery-timeout: Should be at least one millisecond"))
    assert(subMillisecondMessage.contains("erasure-worker.poll-interval: Should be at least one millisecond"))

    val minimumWorkerTimings = operational(
      Map(
        "ANALYTICS_ERASURE_LEASE_DURATION" -> "1 millisecond",
        "ANALYTICS_ERASURE_DELIVERY_TIMEOUT" -> "1 millisecond",
        "ANALYTICS_ERASURE_POLL_INTERVAL" -> "1 millisecond"
      )
    ).toOption
      .getOrElse(fail("expected one-millisecond erasure worker timings to be accepted"))
    assertEquals(minimumWorkerTimings.erasureWorkerTimings.leaseDuration, 1.millisecond)
    assertEquals(minimumWorkerTimings.erasureWorkerTimings.deliveryTimeout, 1.millisecond)
    assertEquals(minimumWorkerTimings.erasureWorkerTimings.pollInterval, 1.millisecond)

    val invalidDeltaDurations = operational(
      Map(
        "ANALYTICS_RETENTION_DELTA_VACUUM_SAFETY" -> "0 seconds",
        "ANALYTICS_RETENTION_DELTA_LOG_RETENTION" -> "-1 day"
      )
    )
    val deltaDurationMessage = invalidDeltaDurations.swap.toOption
      .getOrElse(fail("expected rejected Delta retention durations"))
      .getMessage
    assert(deltaDurationMessage.contains("retention.delta-vacuum-safety: Should be strictly positive"))
    assert(deltaDurationMessage.contains("retention.delta-log-retention: Should be strictly positive"))

    val invalidTimeWindows = operational(
      Map("ANALYTICS_REPORT_RESERVATION_TTL" -> "0 seconds", "ANALYTICS_MONGO_TRANSACTION_WINDOW" -> "0 seconds")
    )
    val timeWindowMessage = invalidTimeWindows.swap.toOption
      .getOrElse(fail("expected rejected time windows"))
      .getMessage
    assert(timeWindowMessage.contains("report-reservation-ttl: Should be strictly positive"))
    assert(timeWindowMessage.contains("mongo-transaction-window: Should be strictly positive"))

    val evidenceOverflow = operational(
      Map("ANALYTICS_MAXIMUM_ERASURE_EVIDENCE_FILES" -> Int.MaxValue.toString)
    )
    assert(evidenceOverflow.isLeft)

    val safeVacuum = operational(
      Map("ANALYTICS_RETENTION_DELTA_VACUUM_SAFETY" -> "7 days")
    ).toOption
      .getOrElse(fail("expected valid Delta vacuum config"))
    assertEquals(safeVacuum.retention.deltaVacuumSafetyCheckEnabled, true)
    assertEquals(safeVacuum.retention.deltaVacuumSafety, 7.days)
    assertEquals(safeVacuum.retention.deltaLogRetention, 30.days)
    assertEquals(safeVacuum.erasureWorkerTimings.leaseDuration, 90.seconds)
    assertEquals(safeVacuum.erasureWorkerTimings.deliveryTimeout, 30.seconds)
    assertEquals(safeVacuum.erasureWorkerTimings.pollInterval, 5.seconds)
  }

  test("bounded operational refinements accept their limits and reject values outside them") {
    val minimums = operational(
      Map(
        "ANALYTICS_MAXIMUM_ERASURE_EVIDENCE_FILES" -> "1",
        "ANALYTICS_MONGO_PUBLISHER_BUFFER_SIZE" -> "1"
      )
    ).toOption
      .getOrElse(fail("expected minimum bounded settings to load"))
    assertEquals(minimums.maximumErasureEvidenceFiles, 1)
    assertEquals(minimums.mongoPublisherBufferSize, 1)

    val maximums = operational(
      Map(
        "ANALYTICS_MAXIMUM_ERASURE_EVIDENCE_FILES" -> "2147483646",
        "ANALYTICS_MONGO_PUBLISHER_BUFFER_SIZE" -> "65536"
      )
    ).toOption
      .getOrElse(fail("expected maximum bounded settings to load"))
    assertEquals(maximums.maximumErasureEvidenceFiles, Int.MaxValue - 1)
    assertEquals(maximums.mongoPublisherBufferSize, 65536)

    val invalid = operational(
      Map(
        "ANALYTICS_MAXIMUM_ERASURE_EVIDENCE_FILES" -> "2147483647",
        "ANALYTICS_MONGO_PUBLISHER_BUFFER_SIZE" -> "65537"
      )
    )
    val message = invalid.swap.toOption.getOrElse(fail("expected out-of-range settings to be rejected")).getMessage
    assert(message.contains("maximum-erasure-evidence-files"))
    assert(message.contains("mongo-publisher-buffer-size"))

    val nonPositive = operational(
      Map(
        "ANALYTICS_MAXIMUM_ERASURE_EVIDENCE_FILES" -> "0",
        "ANALYTICS_MONGO_PUBLISHER_BUFFER_SIZE" -> "0"
      )
    )
    val nonPositiveMessage =
      nonPositive.swap.toOption.getOrElse(fail("expected non-positive settings to be rejected")).getMessage
    assert(nonPositiveMessage.contains("maximum-erasure-evidence-files"))
    assert(nonPositiveMessage.contains("mongo-publisher-buffer-size"))
  }

  test("Kafka configuration allows plaintext only with an explicit opt-in") {
    val local = settings ++ Map(
      "ANALYTICS_KAFKA_SECURITY_PROTOCOL" -> "SASL_PLAINTEXT",
      "ANALYTICS_KAFKA_ALLOW_PLAINTEXT" -> "true"
    )
    val loaded = AnalyticsConfigFixtures.batch(hocon, local).toOption.getOrElse(fail("expected local config"))
    assertEquals(loaded.common.kafka.securityProtocol, KafkaSecurityProtocol.SaslPlaintext)
    assertEquals(loaded.common.kafka.allowPlaintext, true)

    val missingOptIn = AnalyticsConfigFixtures
      .batch(
        hocon,
        settings + ("ANALYTICS_KAFKA_SECURITY_PROTOCOL" -> "SASL_PLAINTEXT")
      )
      .swap
      .toOption
      .getOrElse(fail("expected disallowed plaintext protocol"))
    assert(missingOptIn.getMessage.contains("allow-plaintext=true"))

    val composeEndpoint = KafkaConnection.validate(
      KafkaConnection(
        "kafka:9092",
        Some("reader"),
        Some("password"),
        KafkaSecurityProtocol.SaslPlaintext,
        allowPlaintext = true
      )
    )
    assert(composeEndpoint.isValid)
    val externalEndpoint = KafkaConnection(
      "broker.example.com:9092",
      Some("reader"),
      Some("password"),
      KafkaSecurityProtocol.SaslPlaintext,
      allowPlaintext = true
    )
    assert(KafkaConnection.validate(externalEndpoint).isInvalid)
    assert(
      KafkaClientProperties.clientProperties(externalEndpoint).left.toOption.exists {
        case _: AnalyticsError.InvalidConfiguration => true
        case _                                      => false
      }
    )
  }

  test("plaintext bootstrap parser accepts only valid local broker endpoints") {
    def plaintext(endpoint: String) =
      KafkaConnection(endpoint, Some("reader"), Some("password"), KafkaSecurityProtocol.SaslPlaintext, true)
    Vector("localhost:9092", "kafka:9092", "127.0.0.1:9092", "[::1]:9092").foreach { endpoint =>
      assert(KafkaConnection.validate(plaintext(endpoint)).isValid, s"expected local endpoint: $endpoint")
    }
    Vector("kafka:9093", "broker.example:9092", "localhost", "localhost:0", "[::1]9092", "localhost:70000")
      .foreach { endpoint =>
        assert(KafkaConnection.validate(plaintext(endpoint)).isInvalid, s"expected rejected endpoint: $endpoint")
      }
    assert(KafkaConnection.validate(plaintext("localhost:9092,broker.example:9092")).isInvalid)
  }

  test("Kafka configuration rejects unsupported protocols and malformed opt-in values") {
    val unsupported = AnalyticsConfigFixtures.batch(
      hocon,
      settings ++ Map(
        "ANALYTICS_KAFKA_SECURITY_PROTOCOL" -> "PLAINTEXT",
        "ANALYTICS_KAFKA_ALLOW_PLAINTEXT" -> "true"
      )
    )
    assert(unsupported.swap.toOption.exists(_.getMessage.contains("security-protocol")))

    val malformedFlag = AnalyticsConfigFixtures.batch(
      hocon,
      settings ++ Map(
        "ANALYTICS_KAFKA_SECURITY_PROTOCOL" -> "SASL_PLAINTEXT",
        "ANALYTICS_KAFKA_ALLOW_PLAINTEXT" -> "perhaps"
      )
    )
    assert(malformedFlag.isLeft)
  }

  test("typed batch readers reject malformed numeric substitutions without echoing their values") {
    val malformed = AnalyticsConfigFixtures.batch(
      hocon,
      settings + ("ANALYTICS_PARTITION" -> "not-a-number")
    )
    assert(malformed.swap.toOption.exists(error => !error.getMessage.contains("not-a-number")))

    val blankTopic = AnalyticsConfigFixtures.batch(
      hocon.replace("topic = \"hiring.operational-events\"", "topic = \" \""),
      settings
    )
    assert(blankTopic.swap.toOption.exists(_.getMessage.contains("analytics.kafka.topic")))
  }

  test("PureConfig failures identify missing and malformed keys") {
    val missing = AnalyticsConfigFixtures.batch(
      hocon.replace("database = \"hiring\"", "databse = \"hiring\""),
      settings
    )
    val missingMessage = missing.swap.toOption.getOrElse(fail("expected missing database config")).getMessage
    assert(missingMessage.contains("database"))

    val malformed = AnalyticsConfigFixtures.batch(
      hocon.replace("username = ${?ANALYTICS_KAFKA_USERNAME}", "username = [42]"),
      settings
    )
    val malformedMessage = malformed.swap.toOption.getOrElse(fail("expected malformed username config")).getMessage
    assert(malformedMessage.contains("username"))

    val secretSentinel = "SENSITIVE_DIAGNOSTIC_SENTINEL"
    val malformedHocon = hocon.replace(
      "secret-base64 = ${?HIRING_ANALYTICS_HMAC_SECRET_BASE64}",
      s"secret-base64 = \"$secretSentinel\"\n    broken = ["
    )
    val parseMessage = AnalyticsConfigFixtures
      .batch(malformedHocon, settings)
      .swap
      .toOption
      .getOrElse(fail("expected malformed HOCON"))
      .getMessage
    assert(!parseMessage.contains(secretSentinel))

    val unresolvedSubstitution = hocon.replace(
      "secret-base64 = ${?HIRING_ANALYTICS_HMAC_SECRET_BASE64}",
      s"secret-base64 = \"$secretSentinel\"\n    unresolved = $${UNRESOLVED_CONFIG_SENTINEL}"
    )
    val substitutionMessage = AnalyticsConfigFixtures
      .batch(unresolvedSubstitution, settings)
      .swap
      .toOption
      .getOrElse(fail("expected unresolved secret substitution"))
      .getMessage
    assert(substitutionMessage.contains("Could not resolve substitution"), substitutionMessage)
    assert(substitutionMessage.contains("UNRESOLVED_CONFIG_SENTINEL"))
    assert(!substitutionMessage.contains(secretSentinel))
  }

  test("key-retirement audit reports its missing config path") {
    val missingAudit = AnalyticsConfigFixtures.keyRetirementAudit(
      hocon.replace("key-retirement-audit {", "unused-audit {"),
      settings
    )
    assert(missingAudit.swap.toOption.exists(_.getMessage.contains("key-retirement-audit")))
  }

  test("packaged application.conf supports the same substitutions and defaults") {
    val input = Option(getClass.getResourceAsStream("/application.conf")).getOrElse(fail("application.conf is missing"))
    val packaged = try new String(input.readAllBytes(), StandardCharsets.UTF_8)
    finally input.close()
    val loaded =
      AnalyticsConfigFixtures.batch(packaged, settings).toOption.getOrElse(fail("expected packaged config"))
    assertEquals(loaded.common.sparkMaster, "local[*]")
    assertEquals(AnalyticsTopic.unwrap(loaded.manifest.offsetRanges.head.topic), "hiring.operational-events")
    val audit = AnalyticsConfigFixtures
      .keyRetirementAudit(packaged, settings + ("ANALYTICS_RETIRING_KEY_ID" -> "hmac-v1"))
      .fold(error => fail(error.getMessage), identity)
    assertEquals(audit.mongoDatabase, "hiring")
    assertEquals(audit.sparkMaster, "local[*]")
    assertEquals(audit.lakehouseRoot, "file:///tmp/hiring-analytics")
    assertEquals(audit.retention.kafka.barrierOffset, None)
    assertEquals(audit.writers.managed, Vector.empty)
    assertEquals(audit.operational.mongoPublisherBufferSize, 256)

    val credentialedAudit = AnalyticsConfigFixtures
      .keyRetirementAudit(
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
    val audit = AnalyticsConfigFixtures
      .keyRetirementAudit(
        hocon,
        settings ++ Map(
          "ANALYTICS_RETIRING_KEY_ID" -> "hmac-v1",
          "ANALYTICS_RETIRING_KAFKA_BARRIER_OFFSET" -> "12",
          "ANALYTICS_RETIRING_KAFKA_EARLIEST_OFFSET" -> "12"
        )
      )
      .toOption
      .getOrElse(fail("expected valid audit config"))
    assertEquals(audit.retention.kafka.barrierOffset, Some(12L))
    assertEquals(audit.retention.kafka.earliestAvailableOffset, Some(12L))
    assertEquals(audit.retention.deltaData.retainedUntil, Some(java.time.Instant.parse("2026-09-27T00:00:00Z")))
    assertEquals(audit.writers.observedAt, Some(java.time.Instant.parse("2026-09-27T00:00:00Z")))
    assertEquals(audit.writers.managed.head.disposition, WriterDisposition.Stopped)

    val malformedTimestamp = hocon.replace("2026-09-27T00:00:00Z", "invalid-timestamp")
    assert(AnalyticsConfigFixtures.keyRetirementAudit(malformedTimestamp, settings).isLeft)
    val malformedDisposition = hocon.replace("disposition = \"stopped\"", "disposition = \"invalid\"")
    assert(AnalyticsConfigFixtures.keyRetirementAudit(malformedDisposition, settings).isLeft)
  }

  test("worker settings validate a separate fencer connection") {
    val loaded =
      AnalyticsConfigFixtures.worker(hocon, settings).toOption.getOrElse(fail("expected valid worker config"))
    assertEquals(AnalyticsTopic.unwrap(loaded.topic), "hiring.operational-events")
    assertEquals(loaded.fencerKafka.saslUsername, Some("analytics_fencer"))
    assert(!loaded.toString.contains("fencer-secret"))
  }

  test("application HOCON loads worker settings without batch or retirement inputs") {
    val applicationHocon = scala.util.Using.resource(scala.io.Source.fromResource("application.conf"))(_.mkString)
    val workerEnvironment = settings -- Set(
      "ANALYTICS_RUN_ID",
      "ANALYTICS_PARTITION",
      "ANALYTICS_START_OFFSET",
      "ANALYTICS_END_OFFSET_EXCLUSIVE"
    ) ++ Map(
      "ANALYTICS_KAFKA_SECURITY_PROTOCOL" -> "SASL_PLAINTEXT",
      "ANALYTICS_KAFKA_ALLOW_PLAINTEXT" -> "true",
      "HIRING_ANALYTICS_HMAC_PREVIOUS_KEY_ID" -> "",
      "HIRING_ANALYTICS_HMAC_PREVIOUS_SECRET_BASE64" -> ""
    )
    assert(AnalyticsConfigFixtures.worker(applicationHocon, workerEnvironment).isRight)
    assert(AnalyticsConfigFixtures.batch(applicationHocon, workerEnvironment).isLeft)
    assert(AnalyticsConfigFixtures.keyRetirementAudit(applicationHocon, workerEnvironment).isLeft)
  }

  test("empty optional previous-key substitutions mean no retiring key") {
    val withEmptyRotationSettings = settings ++ Map(
      "HIRING_ANALYTICS_HMAC_PREVIOUS_KEY_ID" -> "",
      "HIRING_ANALYTICS_HMAC_PREVIOUS_SECRET_BASE64" -> ""
    )
    assert(AnalyticsConfigFixtures.batch(hocon, withEmptyRotationSettings).isRight)

    val incomplete = SubjectPseudonymizer.validateFromBase64(key, "hmac-v1", Some("old-key"), Some(""))
    assert(
      incomplete.toEither.swap.toOption.exists(
        _.toChain.toList.contains(
          "previous HMAC key ID and secret must be configured together"
        )
      )
    )
  }

  test("batch configuration reports missing fields and invalid ranges without exposing values") {
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
      AnalyticsConfigFixtures.batch(hocon, invalid).swap.toOption.getOrElse(fail("expected invalid config"))
    val message = error.getMessage
    assert(message.contains("analytics.mongo.uri"), message)
    assert(message.contains("analytics.kafka.bootstrap-servers"))
    assert(message.contains("analytics.kafka.username"))
    assert(message.contains("Key not found"))
    assert(!message.contains("this-value-must-not-appear-in-errors"))

    val invalidRange =
      AnalyticsConfigFixtures.batch(hocon, settings + ("ANALYTICS_END_OFFSET_EXCLUSIVE" -> "4"))
    assert(invalidRange.swap.toOption.exists(_.getMessage.contains("end offset must not precede start offset")))
  }

  test("HMAC validation accumulates short, malformed, and unpaired key settings") {
    val shortKey = Base64.getEncoder.encodeToString("short".getBytes(StandardCharsets.UTF_8))
    val validation = SubjectPseudonymizer.validateFromBase64(shortKey, " ", Some("old-key"), None)
    val errors =
      validation.toEither.swap.toOption.getOrElse(fail("expected invalid key ring")).toChain.toList.mkString(" ")
    assert(errors.contains("HMAC secret must decode to at least 32 bytes"))
    assert(errors.contains("previous HMAC key ID and secret must be configured together"))
  }

  test("batch validation reports independent cross-field violations together") {
    val invalid = settings ++ Map(
      "MONGODB_URI" -> "not a mongo uri",
      "ANALYTICS_KAFKA_SECURITY_PROTOCOL" -> "SASL_PLAINTEXT",
      "ANALYTICS_END_OFFSET_EXCLUSIVE" -> "4"
    )
    assertEquals(
      failureSet(AnalyticsConfigFixtures.batch(hocon, invalid)),
      List(
        "Kafka SASL_PLAINTEXT requires analytics.kafka.allow-plaintext=true",
        "analytics.mongo.uri is invalid",
        "end offset must not precede start offset"
      )
    )
  }

  test("worker validation reports a missing fencer credential and an invalid Mongo URI together") {
    val invalid = settings - "ANALYTICS_KAFKA_FENCER_PASSWORD" + ("MONGODB_URI" -> "not a mongo uri")
    assertEquals(
      failureSet(AnalyticsConfigFixtures.worker(hocon, invalid)),
      List("analytics.kafka.fencer.password: Key not found: 'password'.", "analytics.mongo.uri is invalid")
    )
  }

  test("key-retirement audit reports a missing retiring key and an invalid Mongo URI together") {
    assertEquals(
      failureSet(AnalyticsConfigFixtures.keyRetirementAudit(hocon, settings + ("MONGODB_URI" -> "not a mongo uri"))),
      List(
        "analytics.key-retirement-audit.retiring-key-id: Key not found: 'retiring-key-id'.",
        "analytics.mongo.uri is invalid"
      )
    )
  }

  test("streaming runtime settings accumulate common and streaming violations and report decode paths") {
    val streamingHocon = hocon.replace(
      "  batch {",
      """|  streaming {
        |    stream-id = "hiring-events"
        |    activation-grant-id = "grant-2026-10"
        |    checkpoint-location = "file:///var/lib/hiring-analytics/checkpoints/hiring-events"
        |    trigger-interval = 10 seconds
        |    max-offsets-per-trigger = 1000
        |    maximum-replay-records = 1000
        |    maintenance-interval = 60 seconds
        |    progress-retention = 7 days
        |    initial-offsets = [{ partition = 0, offset = 0 }]
        |  }
        |  batch {""".stripMargin
    )
    val loaded = AnalyticsConfigFixtures.streamingRuntime(streamingHocon, settings)
    assertEquals(loaded.map(runtime => AnalyticsTopic.unwrap(runtime.topic)), Right("hiring.operational-events"))
    assertEquals(loaded.map(_.streaming.triggerInterval), Right(10.seconds))

    val emptyOffsets =
      streamingHocon.replace("initial-offsets = [{ partition = 0, offset = 0 }]", "initial-offsets = []")
    assertEquals(
      failureSet(AnalyticsConfigFixtures.streamingRuntime(emptyOffsets, settings + ("MONGODB_URI" -> "not a uri"))),
      List(
        "analytics.mongo.uri is invalid",
        "analytics.streaming.initial-offsets must explicitly name every source partition"
      )
    )

    val outOfBounds = streamingHocon
      .replace("max-offsets-per-trigger = 1000", "max-offsets-per-trigger = 100001")
      .replace("trigger-interval = 10 seconds", "trigger-interval = 11 seconds")
    val decodeMessage = invalidMessage(AnalyticsConfigFixtures.streamingRuntime(outOfBounds, settings))
    assert(decodeMessage.contains("analytics.streaming.max-offsets-per-trigger: Should be included in [1, 100000]"))
    assert(
      decodeMessage.contains("analytics.streaming.trigger-interval: Should be strictly positive and at most 10 seconds")
    )
  }

  test("every loader names the failing paths of bounded operational fields") {
    val outOfBounds = settings ++ Map(
      "ANALYTICS_RETENTION_BRONZE_DAYS" -> "0",
      "ANALYTICS_ERASURE_POLL_INTERVAL" -> "0 seconds"
    )
    val loaders: Vector[(String, Map[String, String]) => Either[AnalyticsError, Any]] = Vector(
      AnalyticsConfigFixtures.batch,
      AnalyticsConfigFixtures.worker,
      AnalyticsConfigFixtures.lateFactReplay,
      AnalyticsConfigFixtures.keyRetirementAudit
    )
    loaders.foreach { read =>
      val message = invalidMessage(read(hocon, outOfBounds))
      assert(message.contains("analytics.operational.retention.bronze-days: Should be strictly positive"), message)
      assert(
        message.contains("analytics.operational.erasure-worker.poll-interval: Should be at least one millisecond"),
        message
      )
    }
  }

  test("decode failures name the failing paths without echoing malformed values or exception text") {
    val secret = "sk-live-SECRET-TEXT-1234"
    val message = invalidMessage(
      AnalyticsConfigFixtures.batch(
        hocon,
        settings ++ Map(
          "ANALYTICS_MONGO_PUBLISHER_BUFFER_SIZE" -> secret,
          "ANALYTICS_REPORT_RESERVATION_TTL" -> secret
        )
      )
    )
    assert(message.contains("analytics.operational.mongo-publisher-buffer-size"), message)
    assert(message.contains("analytics.operational.report-reservation-ttl"), message)
    assert(!message.contains(secret), message)
    assert(!message.contains("SECRET-TEXT"), message)
    assert(!message.contains("sk-live"), message)
  }

  test("an unresolved substitution reports only its name and no neighbouring values") {
    val secret = "sk-live-SECRET-TEXT-1234"
    val unresolved = hocon.replace(
      "key-id = \"hmac-v1\"",
      s"key-id = \"$secret\"\n    unresolved = $${?SECRET_SUBSTITUTION}\n    required = $${SECRET_SUBSTITUTION}"
    )
    val message = invalidMessage(AnalyticsConfigFixtures.batch(unresolved, settings))
    assert(message.contains("SECRET_SUBSTITUTION"), message)
    assert(!message.contains(secret), message)
    assert(!message.contains("sk-live"), message)
  }

  private def invalidMessage(result: Either[AnalyticsError, Any]): String =
    result.swap.toOption.getOrElse(fail("expected an invalid configuration")).getMessage

  private def failureSet(result: Either[AnalyticsError, Any]): List[String] =
    invalidMessage(result).split("; ").toList.sorted
}
