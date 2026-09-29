package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.service.port.{AnalyticsRangeFingerprint, AnalyticsRunId}
import munit.FunSuite
import org.bson.Document

import java.util.Date
import scala.jdk.CollectionConverters.*

class AnalyticsReportReservationSpec extends FunSuite {
  test("report identifiers reject empty values at the port boundary") {
    assertEquals(AnalyticsRunId.from(null), None)
    assertEquals(AnalyticsRunId.from("  "), None)
    assertEquals(AnalyticsRangeFingerprint.from(null), None)
    assertEquals(AnalyticsRangeFingerprint.from("\t"), None)
    assertEquals(AnalyticsRunId.from("run-1").map(_.value), Some("run-1"))
    assertEquals(AnalyticsRangeFingerprint.from("range-1").map(_.value), Some("range-1"))
  }

  test("report reservation codec keeps BSON names and numeric widths") {
    val createdAt = new Date(1_700_000_000_000L)
    val expiresAt = new Date(1_700_000_100_000L)
    val stored = MongoHiringPersistenceCodecs.StoredAnalyticsReportRun(
      "run-1",
      "range-1",
      2L,
      3L,
      Some("Reserved"),
      Some(createdAt),
      Some(expiresAt)
    )
    val document = MongoHiringPersistenceCodecs.analyticsReportRun(stored)

    assertEquals(
      document.keySet().asScala.toSet,
      Set("_id", "rangeFingerprint", "generation", "revision", "state", "createdAt", "expiresAt")
    )
    assertEquals(document.get("generation").getClass, classOf[java.lang.Long])
    assertEquals(document.get("revision").getClass, classOf[java.lang.Long])
    assertEquals(MongoHiringPersistenceCodecs.decodeAnalyticsReportRun(document), Right(stored))

    val legacy = new Document("_id", "run-2")
      .append("rangeFingerprint", "range-2")
      .append("generation", 0L)
      .append("revision", 1L)
    assertEquals(MongoHiringPersistenceCodecs.decodeAnalyticsReportRun(legacy).toOption.map(_._id), Some("run-2"))
  }

  test("published report control remains readable without optional last run ID") {
    val control = new Document("_id", "analytics-report")
      .append("generation", 2L)
      .append("state", "Published")
      .append("nextRevision", 4L)
      .append("lastPublishedRevision", 3L)

    val decoded = MongoHiringPersistenceCodecs.decodeAnalyticsReportControl(control)
    assertEquals(decoded.toOption.map(_.lastRunId), Some(None))
    assertEquals(decoded.toOption.map(_.lastPublishedRevision), Some(3L))
  }
}
