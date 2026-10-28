package com.example.graphQL.cats.repository.mongo

import com.example.graphQL.cats.service.{
  AnalyticsFunnelDay,
  AnalyticsHiringStageDuration,
  AnalyticsHiringStageMetric,
  AnalyticsReportSnapshot,
  AnalyticsSkillPostingDay,
  AnalyticsTimeToHire
}
import munit.FunSuite
import org.bson.Document

import java.time.Instant

class AnalyticsReportSnapshotDocumentSpec extends FunSuite {
  private val asOf = Instant.parse("2026-09-22T12:00:00Z")
  private val expiresAt = Instant.parse("2026-10-22T12:00:00Z")
  private val snapshot = AnalyticsReportSnapshot(
    asOf,
    List(AnalyticsFunnelDay(asOf, 12L, 5L, 1L, 3L, 2L, 1L)),
    Some(AnalyticsTimeToHire(12.5, 20.0, 36.0, 48.0, 12L, 2L)),
    Vector(
      AnalyticsHiringStageDuration(AnalyticsHiringStageMetric.TimeToAccept, 1.0, 2.0, 3.0, 4.0, 15L),
      AnalyticsHiringStageDuration(AnalyticsHiringStageMetric.AcceptedToInterview, 5.0, 5.0, 6.0, 7.5, 11L)
    ),
    List(AnalyticsSkillPostingDay(asOf, "scala", 14L))
  )

  test("published snapshot documents preserve aggregate values and expiry") {
    val document = AnalyticsReportSnapshotDocument.write(snapshot, expiresAt)

    assertEquals(document.getString("_id"), AnalyticsReportSnapshotDocument.CurrentId)
    assertEquals(document.getString("state"), "Published")
    assertEquals(document.getDate("expiresAt").toInstant, expiresAt)
    assertEquals(AnalyticsReportSnapshotDocument.read(document), Right(snapshot))
  }

  test("a malformed snapshot document is not exposed to the API") {
    val document = AnalyticsReportSnapshotDocument.write(snapshot, expiresAt)
    document.remove("skillPostingActivity")

    assert(AnalyticsReportSnapshotDocument.read(document).isLeft)
  }

  private def stored: Document = AnalyticsReportSnapshotDocument.write(snapshot, expiresAt)

  private def durations(document: Document): java.util.List[Document] =
    document.getList("stageDurations", classOf[Document])

  private def rejects(label: String)(mutate: Document => Any): Unit =
    test(s"AC-03 a snapshot document is unreadable when $label") {
      val document = stored
      val _ = mutate(document)
      assert(AnalyticsReportSnapshotDocument.read(document).isLeft)
    }

  test("AC-03 an empty stageDurations array round-trips as no durations") {
    val empty = snapshot.copy(stageDurations = Vector.empty)
    val document = AnalyticsReportSnapshotDocument.write(empty, expiresAt)

    assertEquals(AnalyticsReportSnapshotDocument.read(document), Right(empty))
  }

  rejects("stageDurations is missing")(_.remove("stageDurations"))
  rejects("a metric is unknown")(durations(_).get(0).put("metric", "TIME_TO_LUNCH"))
  rejects("a metric is duplicated")(durations(_).get(1).put("metric", "TIME_TO_ACCEPT"))
  rejects("percentiles are unordered")(durations(_).get(0).put("p75Hours", 0.5))
  rejects("a percentile is negative")(durations(_).get(0).put("p50Hours", -1.0))
  rejects("a percentile is not finite")(durations(_).get(0).put("p95Hours", Double.PositiveInfinity))
  rejects("a percentile is not a number")(durations(_).get(0).put("p95Hours", Double.NaN))
  rejects("eligibleCount is negative")(durations(_).get(0).put("eligibleCount", -1L))
  rejects("eligibleCount is missing")(durations(_).get(0).remove("eligibleCount"))
}
