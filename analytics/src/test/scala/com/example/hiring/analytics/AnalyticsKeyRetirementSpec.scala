package com.example.hiring.analytics

import munit.FunSuite

import java.time.Instant

class AnalyticsKeyRetirementSpec extends FunSuite {
  import AnalyticsKeyRetirement.*

  private val now = Instant.parse("2026-10-26T00:00:00Z")

  private val passedRetention = RetentionEvidence(
    kafka = KafkaRetentionEvidence(Some(10L), Some(10L), "kafka-barrier-evidence"),
    deltaData = RetentionHorizon(Some(now.minusSeconds(1)), "delta-data-retention-evidence"),
    deltaLogs = RetentionHorizon(Some(now.minusSeconds(1)), "delta-log-retention-evidence"),
    reports = RetentionHorizon(Some(now.minusSeconds(1)), "report-retention-evidence")
  )

  private val completeWriterInventory = WriterInventory(
    observedAt = now.minusSeconds(1),
    coverageReference = "operator-inventory-2026-10-25",
    managed = Vector(WriterRecord("analytics-batch", WriterDisposition.Stopped, "job-stop-record")),
    unmanaged = Vector(WriterRecord("external-delta-jobs", WriterDisposition.AccessRevoked, "acl-audit-record"))
  )

  test("retention gate requires passed Kafka barrier and every elapsed horizon") {
    assertEquals(validateRetention(passedRetention, now), Vector.empty)

    val blocked = passedRetention.copy(
      kafka = KafkaRetentionEvidence(Some(10L), Some(9L), "kafka-barrier-evidence"),
      deltaData = RetentionHorizon(None, "delta-data-retention-evidence"),
      reports = RetentionHorizon(Some(now.plusSeconds(1)), "report-retention-evidence")
    )
    val reasons = validateRetention(blocked, now)
    assert(reasons.exists(_.contains("Kafka retention barrier")))
    assert(reasons.exists(_.contains("Delta data-file retention horizon")))
    assert(reasons.exists(_.contains("report retention horizon")))
  }

  test("writer gate requires coverage and evidence for every managed and unmanaged writer") {
    assertEquals(validateWriters(completeWriterInventory, now), Vector.empty)

    val incomplete = completeWriterInventory.copy(
      unmanaged = Vector(WriterRecord("unmanaged-spark-job", WriterDisposition.Unknown, ""))
    )
    val reasons = validateWriters(incomplete, now)
    assert(reasons.exists(_.contains("not accounted for as stopped or access-revoked")))
    assert(reasons.exists(_.contains("evidence reference is missing")))
  }

  test("an empty or stale writer inventory cannot authorize retirement") {
    val empty = completeWriterInventory.copy(
      observedAt = now.plusSeconds(1),
      coverageReference = "",
      managed = Vector.empty,
      unmanaged = Vector.empty
    )
    val reasons = validateWriters(empty, now)
    assert(reasons.exists(_.contains("lacks fresh operator-attested coverage evidence")))
    assert(reasons.exists(_.contains("no individually accounted writer identities")))
  }

  test("writer evidence older than one hour blocks the audit") {
    val stale = completeWriterInventory.copy(observedAt = now.minusSeconds(3601))
    assert(validateWriters(stale, now).exists(_.contains("fresh operator-attested coverage evidence")))
  }
}
