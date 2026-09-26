package com.example.hiring.analytics

import munit.FunSuite

import java.time.Instant
import java.util.Date
import org.bson.Document

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

  test("erasure request states fail closed unless completion retention has elapsed") {
    assertEquals(erasureRequestActivity(new Document("state", "Pending"), now), Right(true))
    assertEquals(erasureRequestActivity(new Document("state", "Processing"), now), Right(true))
    assertEquals(
      erasureRequestActivity(
        new Document("state", "Complete").append("expiresAt", Date.from(now.minusSeconds(1))),
        now
      ),
      Right(false)
    )
    assertEquals(
      erasureRequestActivity(new Document("state", "Complete").append("expiresAt", Date.from(now.plusSeconds(1))), now),
      Right(true)
    )
    assert(erasureRequestActivity(new Document("state", "Complete"), now).isLeft)
    assert(erasureRequestActivity(new Document("state", "Unknown"), now).isLeft)
    assert(erasureRequestActivity(new Document(), now).isLeft)
  }

  test("pure Delta verdict distinguishes checkpoint metadata from ordinary Parquet files") {
    val checkpoint = deltaParquetVerdict("/table/_delta_log/000.checkpoint.parquet", containsReference = true)
    assertEquals(checkpoint.count, 1L)
    assert(checkpoint.blockers.toList.exists(_.contains("checkpoint")))

    val data = deltaParquetVerdict("/table/part-000.parquet", containsReference = true)
    assertEquals(data.count, 1L)
    assert(data.blockers.toList.exists(_.contains("data file")))
    assertEquals(deltaParquetVerdict("/table/part-000.parquet", containsReference = false), ScanResult(1L))
  }

  test("pure Mongo key-reference scan descends nested BSON values and leaves unrelated values clear") {
    val nested = new Document("payload", new Document("tokens", java.util.Arrays.asList("key-1_subject")))
    assert(containsKeyReferenceInValue(nested, "key-1"))
    assert(!containsKeyReferenceInValue(new Document("payload", "key-2_subject"), "key-1"))
  }

  test("pure Mongo observation reducer carries active erasure subjects into outbox checks") {
    val start = MongoScanState(0L, Set.empty, cats.data.Chain.empty)
    val subject = "subject-123"
    val (withActive, count) = reduceMongoObservation(
      start,
      com.example.hiring.analytics.mongo.AnalyticsCollections.ErasureRequests,
      new Document("_id", subject).append("state", "Pending"),
      "key-1",
      now,
      0
    )
    assertEquals(count, 1)
    assert(withActive.activeSubjects.contains(subject))
    assert(withActive.blockers.toList.exists(_.contains("active or unexpired erasure")))

    val (withOutbox, _) = reduceMongoObservation(
      withActive,
      com.example.hiring.analytics.mongo.AnalyticsCollections.EventOutbox,
      new Document("subjectIds", java.util.Arrays.asList(subject)).append("subjectRefsVersion", 1),
      "key-1",
      now,
      0
    )
    assert(withOutbox.blockers.toList.exists(_.contains("outbox contains replay work")))
  }
}
