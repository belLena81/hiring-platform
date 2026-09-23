package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.repository.protocol.{
  AnalyticsFunnelDay,
  AnalyticsReportSnapshot,
  AnalyticsSkillPostingDay,
  AnalyticsTimeToHire
}
import munit.FunSuite

import java.time.Instant

class AnalyticsReportSnapshotDocumentSpec extends FunSuite {
  private val asOf = Instant.parse("2026-09-22T12:00:00Z")
  private val expiresAt = Instant.parse("2026-10-22T12:00:00Z")
  private val snapshot = AnalyticsReportSnapshot(
    asOf,
    List(AnalyticsFunnelDay(asOf, 12L, 5L, 1L, 3L, 2L, 1L)),
    Some(AnalyticsTimeToHire(12.5, 20.0, 36.0, 48.0, 12L, 2L)),
    List(AnalyticsSkillPostingDay(asOf, "scala", 14L))
  )

  test("published snapshot documents preserve aggregate values and expiry") {
    val document = AnalyticsReportSnapshotDocument.write(snapshot, expiresAt)

    assertEquals(document.getString("_id"), AnalyticsReportSnapshotDocument.CurrentId)
    assertEquals(document.getString("state"), "Published")
    assertEquals(document.getDate("expiresAt").toInstant, expiresAt)
    assertEquals(AnalyticsReportSnapshotDocument.read(document), Some(snapshot))
  }

  test("a malformed snapshot document is not exposed to the API") {
    val document = AnalyticsReportSnapshotDocument.write(snapshot, expiresAt)
    document.remove("skillPostingActivity")

    assertEquals(AnalyticsReportSnapshotDocument.read(document), None)
  }
}
